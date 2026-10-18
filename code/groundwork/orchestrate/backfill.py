"""
Backfills: Groundwork's night, for every logical date in a range.

A backfill is not the nightly DAG run once a night for four years. The
night does two kinds of work. Some of it is an observation that can
only be made as of that night: what Tradeline's log held, what Relay
exported, what the desk answered, what the snapshot saw. That is done
again for every night, oldest first, because each night starts from
the bookmarks the night before left. The rest is derived: the marts
are a function of raw, so they are built once, at the end, from every
night at once, and reconciled with the sources as they stood when the
last night's run was due.

    Replay(folder, parallel=1)    the sources as they stood when a
                                  night's run was due: Tradeline at
                                  that moment, Relay's export from
                                  IT's archive, the landing zone, the
                                  desk answering as of then
    backfill_dag()                one night's observations
    finish_dag(days)              the build, the reconciliation and
                                  publish, once, for the whole range
    run(first, last, env=..., replay=..., parallel=1, pause=0)
    python -m groundwork.orchestrate backfill --from D1 --to D2

Order: nights run oldest first, one at a time, and the backfill stops
at the first that fails, because the next would start from a gap. A
night is a run of the "backfill" DAG in audit.dag_runs, so a backfill
that is stopped and started again keeps every night that succeeded and
reruns the one that did not, from the task that did not finish.

Parallelism: the warehouse takes one writer, so loads are one night at
a time. Replaying the sources is not, and `parallel` nights' sources
are prepared ahead of the night being loaded, each in a slot of its
own. The source's patience: `pause` seconds between nights, and the
backfill gives way to the nightly run, waiting for its lock.

The marts that are partitioned by day (PARTITIONED) can be rebuilt a
day at a time: compiled() is a model's SQL as a full build, and
replace_days() swaps chosen days for what that SQL says now.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import date, datetime, time as dtime, timedelta, timezone
from functools import partial
from pathlib import Path

import duckdb

from groundwork.config import GROUNDWORK, LANDING, NOW
from groundwork.orchestrate import runner
from groundwork.orchestrate.dag import (DAG, PROJECT, PUBLISHED, Fatal,
                                        Task, comments, contracts_hold,
                                        dbt_build, ingest, judged_at)

RUN_AT = dtime(6)                       # the run for D: 06:00 on D+1
STAGED = ("suppliers", "catalogue", "ledger", "relay/migration_map.csv")
PARTITIONED = {"fct_order_lines": "order_date",
               "fct_orders": "order_date"}


def due(day: date) -> datetime:
    """When the run for logical date `day` is due: 06:00 UTC on D+1."""
    return datetime.combine(day + timedelta(days=1), RUN_AT)


def nights(first, last) -> list[date]:
    """Every logical date from first to last, oldest first."""
    a, b = date.fromisoformat(str(first)), date.fromisoformat(str(last))
    if b < a:
        raise ValueError(f"{first} is after {last}")
    return [a + timedelta(days=i) for i in range((b - a).days + 1)]


# ------------------------------------------------ the sources, replayed
@dataclass(frozen=True)
class Prepared:
    """One night's sources, as they stood when its run was due."""
    day: date
    at: datetime
    tradeline_db: Path
    landing: Path
    exported: bool                     # did IT's archive have Relay's?


class Replay:
    """
    The sources as they stood at a run's moment, in a folder of their
    own. Each slot holds a copy of Tradeline and a landing zone, and
    only moves forward, so preparing a night costs the changes since
    that slot's last one. One desk serves every night, its clock set
    when the night starts.
    """

    def __init__(self, folder: Path, parallel: int = 1):
        from meridian.sources import Clock
        self.folder = Path(folder).resolve()
        self.slots = max(0, parallel) + 1
        self.clock = Clock(NOW)
        self._desk = None
        self.timings = {}              # day: (start, end), perf_counter

    def prepare(self, day: date, slot: int = 0) -> Prepared:
        from meridian.sources import landing, relay, tradeline
        t0, at = time.perf_counter(), due(day)
        here = self.folder / f"slot-{slot}"
        land = here / "landing"
        (land / "relay").mkdir(parents=True, exist_ok=True)
        db = Path(tradeline.at(at, here / "tradeline.db"))
        made = relay.export(day, land / "relay")
        if made:                       # tonight's export, none older
            for old in (land / "relay").glob("20*"):
                if old.name != day.isoformat():
                    shutil.rmtree(old)
        relay.write_audit(land / "relay", until=at)
        for under in STAGED:
            src = LANDING / under
            if src.is_dir():
                landing.stage(at, land, under)
            elif (src.exists() and not (land / under).exists()
                  and landing.arrival(src) <= at.replace(
                      tzinfo=timezone.utc).timestamp()):
                shutil.copy2(src, land / under)     # one file, if in
        self.timings[day] = (t0, time.perf_counter())
        return Prepared(day, at, db, land, bool(made))

    def desk(self, at: datetime) -> str:
        """The desk's address, answering as of `at`."""
        from meridian.sources import frontline
        if self._desk is None:
            self._desk = (frontline.serve(self.clock),
                          frontline.serve.last[1])
        self.clock.set(at)
        return self._desk[0]

    @property
    def requests(self) -> int:
        """Requests the desk has answered for this replay so far."""
        return 0 if self._desk is None else len(self._desk[1].log)

    def env(self, base: runner.Env, p: Prepared) -> runner.Env:
        """Where a run of night p reads its sources; base says where
        Groundwork is written."""
        return replace(base, tradeline_db=p.tradeline_db,
                       landing=p.landing,
                       frontline_base=self.desk(p.at))


# ------------------------------------------------------ one night's DAG
MIGRATION = date(2024, 3, 1)           # Relay's first night


def old_crm(contract):
    """Chapter 10's contract for Relay's accounts, held to what the old
    CRM kept: its second records of 212 companies, which Chapter 12
    needs to resolve, were never given an email."""
    col = contract.columns["contact_email"]
    lax = replace(col, when={"source_format": {"legacy": {
        "nullable": True}}})
    return replace(contract, columns={**contract.columns,
                                      "contact_email": lax})


def load_relay(env: runner.Env, day: date, run_at: datetime) -> list:
    """Chapter 10's Relay ingest for a night, step for step, except
    that a night of the old CRM's is held to old_crm()."""
    from groundwork import contracts, loadlog
    from groundwork.ingest import files, relay
    from groundwork.ingest.__main__ import Line, land
    from groundwork.storage import raw_views
    folder = f"relay/{day.isoformat()}"
    found = {a.name.rsplit("/", 1)[1]: a for a in
             files.discover(env.landing, folder, by=run_at)}
    extras = (("field_history", "relay/audit",
               relay.read_csv(relay._month)),
              ("migration_map", "relay/migration_map.csv",
               relay.read_csv(lambda a: "migration_map")))
    Path(env.warehouse).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(env.warehouse))
    out = []
    try:
        for table in ("accounts", "contracts"):
            a = found.get(f"{table}.csv")
            if a is None:
                lid = loadlog.start(con, "relay", table, day,
                                    started_at=run_at)
                why = (f"MissingFile: no {folder}/{table}.csv by "
                       f"{run_at:%Y-%m-%d %H:%M} UTC")
                loadlog.fail(con, lid, finished_at=run_at, error=why)
                out.append(Line("relay", table, "missing", 0, 0, 0, 0,
                                lid, why))
                continue
            old = table == "accounts" and day < MIGRATION
            held = old_crm(contracts.load("relay.accounts")) if old \
                else None
            out.append(land(con, "relay", table, [a], day, run_at,
                            relay.read_export(table), folder, env.root,
                            env.archive, isolate=False, contract=held))
        for table, under, read in extras:
            arrivals = files.discover(env.landing, under, by=run_at)
            if arrivals:
                out.append(land(con, "relay", table, arrivals, day,
                                run_at, read, under, env.root,
                                env.archive))
        raw_views(con, env.root)
    finally:
        con.close()
    return out


def relay_night(ctx) -> str:
    """The night's Relay export, from the archive. A night the archive
    holds no export for is a gap: recorded in the load log, as Chapter
    9 records it, and not a failure. Half an export is a failure."""
    from groundwork.ingest.__main__ import BAD
    lines = load_relay(ctx.env, ctx.day, ctx.now())
    missing = [x for x in lines if x.status == "missing"]
    bad = [x for x in lines if x.status in BAD and x not in missing]
    if len(missing) == 2 and not bad:
        return "no export in the archive: a gap"
    for x in bad + missing:
        why = f"relay.{x.table} {x.status}: {x.detail}"
        raise (Fatal if x.status == "broken" else RuntimeError)(why)
    parked = sum(x.parked for x in lines)
    rows = sum(x.loaded for x in lines)
    return (f"{rows:,} rows, {len(lines)} tables"
            + (f", {parked:,} parked" if parked else ""))


def _dbt(env: runner.Env, *args, vars=None, project: Path = PROJECT):
    """dbt, in this process: starting a new one for every night would
    cost more than the night's snapshot does."""
    from dbt.adapters.duckdb.connections import DuckDBConnectionManager
    from dbt.cli.main import dbtRunner
    target = Path(env.dbt_target_path or PROJECT / "target")
    argv = ["--log-level", "none", *args, "--profiles-dir", ".",
            "--target", env.dbt_target, "--target-path", str(target),
            "--log-path", str(target / "logs")]
    if vars:
        argv += ["--vars", json.dumps(vars)]
    was, cwd = os.environ.get("GROUNDWORK_DBT_PATH"), Path.cwd()
    os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    if env.dbt_target == "dev":
        os.environ["GROUNDWORK_DBT_PATH"] = str(env.warehouse)
    os.chdir(project)
    try:
        res = dbtRunner().invoke(argv)
    finally:
        DuckDBConnectionManager.close_all_connections()
        os.chdir(cwd)
        if was is None:
            os.environ.pop("GROUNDWORK_DBT_PATH", None)
        else:
            os.environ["GROUNDWORK_DBT_PATH"] = was
    if res.exception is not None:
        raise res.exception
    if not res.success:
        raise RuntimeError(f"dbt {args[0]} failed")
    return res


def snapshotted(con) -> date | None:
    """The latest night the snapshot has seen: from the run history,
    or from the versions themselves when it was run by hand."""
    seen = []
    for sql in ("SELECT max(logical_date) FROM audit.task_runs WHERE "
                "task IN ('snapshot', 'dbt_build') AND state = "
                "'succeeded' AND coalesce(detail, '') NOT LIKE 'no %'",
                "SELECT CAST(max(dbt_valid_from) AS DATE) "
                "FROM snapshots.snp_relay__accounts"):
        try:
            seen.append(con.execute(sql).fetchone()[0])
        except duckdb.CatalogException:
            pass
    return max((d for d in seen if d is not None), default=None)


def snapshot(ctx) -> str:
    """dbt's snapshot of Relay's accounts, told the night. Nights must
    reach it in order: one earlier than a night it has seen would write
    history backwards, so it is refused. A gap is not observed."""
    con = duckdb.connect(str(ctx.env.warehouse))
    try:
        latest = snapshotted(con)
        if latest is not None and latest > ctx.day:
            raise Fatal(f"the snapshot has seen {latest}; nights reach "
                        "it oldest first")
        try:
            (rows,) = con.execute(
                "SELECT count(*) FROM raw.relay_accounts "
                "WHERE load_date = ?", [ctx.day]).fetchone()
        except duckdb.CatalogException:            # no export yet
            rows = 0
        (view,) = con.execute(
            "SELECT count(*) FROM information_schema.tables WHERE "
            "table_schema = 'staging' AND table_name = "
            "'stg_relay__accounts'").fetchone()
    finally:
        con.close()
    if not rows:
        return "no export: nothing observed"
    if not view:
        _dbt(ctx.env, "run", "--select", "stg_relay__accounts")
    _dbt(ctx.env, "snapshot", vars={"night": ctx.day.isoformat()})
    con = duckdb.connect(str(ctx.env.warehouse), read_only=True)
    try:
        opened, closed = con.execute(
            "SELECT count(*) FILTER (WHERE dbt_valid_from = ?), "
            "count(*) FILTER (WHERE dbt_valid_to = ?) "
            "FROM snapshots.snp_relay__accounts",
            [ctx.day, ctx.day]).fetchone()
    finally:
        con.close()
    return f"{opened:,} opened, {closed:,} closed"


def loads_judged(ctx) -> str:
    """Chapter 15's checks on the night's loads, recorded as Chapter 15
    judges them. A load that is missing or empty stops the night, as
    it does every night. A volume outside its band does not: the band
    is drawn from the weeks before, to catch tonight's surprise, and a
    backfill replays history's surprises, which have happened. They
    are kept in audit.quality_checks, to be read afterwards."""
    from groundwork import quality
    at = judged_at(ctx)
    con = duckdb.connect(str(ctx.env.warehouse))
    try:
        checks = quality.check(con, at, ctx.env.root)
        quality.record(con, checks, at)
    finally:
        con.close()
    errors = [c for c in checks if c.status == "error"]
    band = [c for c in errors if c.kind == "volume" and
            "band" in c.detail]
    stop = [c for c in errors if c not in band]
    if stop:
        raise Fatal("; ".join(f"{c.kind} {c.name}: {c.detail}"
                              for c in stop[:3]))
    return (f"{len(checks)} checks" + (f", {len(band)} outside the "
                                       "band, noted" if band else ""))


def backfill_dag() -> DAG:
    """One night of a backfill: Chapter 17's ingests and checks, in
    this process, and the snapshot. No sensor: the archive has the
    night's export or never will."""
    def load(name, *sources, upstream=()):
        return Task(name, partial(ingest, sources), tuple(upstream),
                    retries=1, retry_delay=10)

    files = ("suppliers", "catalogue", "ledger", "reference")
    loads = ("ingest_relay", "ingest_tradeline", "ingest_frontline",
             *[f"ingest_{s}" for s in files])
    return DAG("backfill", [
        Task("ingest_relay", relay_night, retries=1, retry_delay=10),
        load("ingest_tradeline", "tradeline"),
        load("ingest_frontline", "frontline"),
        Task("load_comments", comments, ("ingest_frontline",),
             retries=1, retry_delay=10),
        *[load(f"ingest_{s}", s) for s in files],
        Task("check_contracts", contracts_hold, loads),
        Task("check_quality", loads_judged, loads),
        Task("snapshot", snapshot, ("check_contracts",
                                    "check_quality")),
    ])


def desk_total(base: str) -> int:
    """How many tickets the desk holds, by its own count."""
    from groundwork.ingest.frontline import BEGINNING, Client, total
    client = Client(base)
    try:
        return total(client, BEGINNING)
    finally:
        client.close()


def reconciled(ctx) -> str:
    """Chapter 15's reconciliation, as Chapter 17's reconcile task runs
    it, with every source asked as it stood when the run was due: the
    replayed Tradeline and landing zone, and the desk for its own
    total. (Chapter 15 takes the desk's total from the last load,
    which is the whole desk only after a first pull; after a night's
    pull it is that night's tickets.)"""
    from groundwork import reconcile as rc
    con = duckdb.connect(str(ctx.env.warehouse))
    try:
        con.execute("SET enable_progress_bar = false")
        cells = rc.revenue(con)
        b = rc.bridge(con, max(c.period for c in cells))
        counts = rc.counts(con, ctx.env.tradeline_db, ctx.env.landing)
        desk = desk_total(ctx.env.frontline_base)
        counts = [replace(c, source=desk) if c.name ==
                  "Frontline tickets" else c for c in counts]
        rc.record(con, judged_at(ctx), cells, b, counts)
    finally:
        con.close()
    bad = [f"{c.period:%Y-%m} {c.region}" for c in cells
           if not (c.ties and c.closes
                   and (c.documented_ok or not c.manual))]
    bad += [] if b.closes else [f"bridge {b.month:%Y-%m}"]
    bad += [c.name for c in counts if not c.ties]
    if bad:
        raise Fatal("does not tie: " + ", ".join(bad[:5]))
    return f"{len(cells)} cells, {len(counts)} counts: all tie"


def publish_all(days: tuple, ctx) -> str:
    """Every night of the range, published by the run that built and
    reconciled them."""
    con = duckdb.connect(str(ctx.env.warehouse))
    try:
        con.execute(PUBLISHED)
        con.executemany("INSERT OR REPLACE INTO audit.published "
                        "VALUES ('nightly', ?, ?, ?)",
                        [[d, ctx.run_id, ctx.now()] for d in days])
    finally:
        con.close()
    return f"{len(days)} nights published"


def finish_dag(days) -> DAG:
    """Once, after the last night: Chapter 17's build and
    reconciliation, then publish for every night of the range."""
    return DAG("backfill_build", [
        Task("dbt_build", dbt_build, retries=1, retry_delay=60,
             timeout=3600, isolate=True),
        Task("reconcile", reconciled, ("dbt_build",), timeout=1800,
             isolate=True),
        Task("publish", partial(publish_all, tuple(days)),
             ("reconcile",)),
    ])


# ----------------------------------------------------------- the range
@dataclass
class Night:
    day: date
    state: str                 # succeeded, kept, failed or not run
    run_id: str | None = None
    seconds: float = 0.0       # wall time, waiting for its replay too
    requests: int = 0          # what it asked the desk
    detail: str = ""
    replayed: tuple | None = None    # (start, end), perf_counter
    loaded: tuple | None = None


@dataclass
class Backfill:
    first: date
    last: date
    nights: list = field(default_factory=list)
    finish: runner.Run | None = None

    @property
    def state(self) -> str:
        ok = all(n.state in runner.DONE for n in self.nights)
        if not ok:
            return "stopped"
        return "succeeded" if self.finish is None or \
            self.finish.state == "succeeded" else "failed"

    def count(self, state: str) -> int:
        return sum(n.state == state for n in self.nights)

    def report(self) -> str:
        out = [f"backfill {self.first} to {self.last}: "
               f"{len(self.nights)} nights"]
        for s in ("succeeded", "kept", "failed", "not run"):
            if self.count(s):
                out.append(f"  {s:<10}{self.count(s):>6}")
        gaps = [n.day for n in self.nights if "gap" in n.detail]
        if gaps:
            out.append(f"  gaps      {len(gaps):>6}  "
                       + ", ".join(str(d) for d in gaps[:3])
                       + (" ..." if len(gaps) > 3 else ""))
        for n in self.nights:
            if n.state == "failed":
                out.append(f"  {n.day} failed: {n.detail}")
        if self.finish is not None:
            out.append(self.finish.report())
        out.append(f"backfill {self.state}")
        return "\n".join(out)


def _states(env: runner.Env, days) -> dict:
    """Each night's latest backfill run, and how it ended."""
    con = runner._connect(env)
    try:
        rows = con.execute("""
            SELECT logical_date, state FROM audit.dag_runs
            WHERE dag = 'backfill' AND logical_date BETWEEN ? AND ?
            QUALIFY row_number() OVER (PARTITION BY logical_date
                    ORDER BY run_at DESC, run_id DESC) = 1""",
                           [days[0], days[-1]]).fetchall()
    finally:
        con.close()
    return dict(rows)


def _gap(env, run_id) -> str:
    con = duckdb.connect(str(env.warehouse), read_only=True)
    try:
        row = con.execute("SELECT detail FROM audit.task_runs WHERE "
                          "run_id = ? AND task = 'ingest_relay' AND "
                          "state = 'succeeded'", [run_id]).fetchone()
    finally:
        con.close()
    return row[0] if row and "gap" in (row[0] or "") else ""


@contextmanager
def _give_way(env: runner.Env, wait: float = 60.0):
    """Hold the nightly run's lock for one night. If tonight's run has
    it, wait for it: history can wait, today cannot."""
    while True:
        try:
            with runner.lock(env, "nightly"):
                yield
            return
        except runner.RunInProgress:
            time.sleep(wait)


def load_as_it_stands(env: runner.Env, day: date, sources) -> None:
    """Load sources that have no past (one version, which arrived after
    the range began) as they stand now, under the range's first date;
    the file ledger keeps when each really arrived."""
    from groundwork.ingest.__main__ import run
    run(day, run_at=NOW.replace(tzinfo=None), only=list(sources),
        landing=LANDING, root=env.root, warehouse=env.warehouse,
        archive=env.archive)


def run(first, last, *, env: runner.Env | None = None,
        replay: Replay | None = None, parallel: int = 1,
        pause: float = 0.0, build: bool = True, as_it_stands=(),
        dag: DAG | None = None, log: list | None = None) -> Backfill:
    """
    Backfill every logical date from `first` to `last`. Nights that a
    backfill already finished are kept; one that it started and did not
    finish is rerun from where it stopped. Stops at the first night
    that fails. With `build`, once every night is done, the marts are
    built, reconciled as of the last night's run and published. Each
    night run is appended to `log` as it ends, if one is given.
    """
    env = env or runner.Env()
    replay = replay or Replay(Path(env.warehouse).parent / "replay",
                              parallel)
    dag = dag or backfill_dag()
    days = nights(first, last)
    if as_it_stands:
        load_as_it_stands(env, days[0], as_it_stands)
    states = _states(env, days)
    out = Backfill(days[0], days[-1])
    todo = [d for d in days if states.get(d) != "succeeded"]
    out.nights = [Night(d, "kept") for d in days
                  if states.get(d) == "succeeded"]
    ahead = max(0, parallel)
    with ThreadPoolExecutor(max_workers=max(1, ahead)) as pool:
        ready = {}

        def queue(i):
            if i < len(todo) and i not in ready:
                ready[i] = pool.submit(replay.prepare, todo[i],
                                       i % replay.slots)

        for i in range(ahead):
            queue(i)
        for i, day in enumerate(todo):
            t0 = time.perf_counter()
            queue(i)
            p = ready.pop(i).result()
            queue(i + ahead)
            asked, t1 = replay.requests, time.perf_counter()
            with _give_way(env):
                r = runner.run(dag, day, run_at=p.at,
                               env=replay.env(env, p),
                               rerun=day in states)
            t2 = time.perf_counter()
            n = Night(day, r.state, r.run_id, t2 - t0,
                      replay.requests - asked,
                      replayed=getattr(replay, "timings", {}).get(day),
                      loaded=(t1, t2))
            n.detail = (_gap(env, r.run_id) if r.state == "succeeded"
                        else "; ".join(f"{k}: {t.detail}" for k, t in
                                       r.tasks.items()
                                       if t.state == "failed"))
            out.nights.append(n)
            if log is not None:
                log.append(n)
            if r.state != "succeeded":
                for d in todo[i + 1:]:
                    out.nights.append(Night(d, "not run"))
                break
            if pause:
                time.sleep(pause)
        for f in ready.values():
            f.cancel()
    out.nights.sort(key=lambda n: n.day)
    if build and out.state == "succeeded":
        last_night = replay.prepare(days[-1], 0)
        with _give_way(env):
            out.finish = runner.run(finish_dag(days), days[-1],
                                    run_at=last_night.at,
                                    env=replay.env(env, last_night))
    return out


# ------------------------------------- marts rebuilt a day at a time
def compiled(model: str, env: runner.Env,
             project: Path = PROJECT) -> str:
    """A model's SQL as dbt would run it for a full build, against the
    warehouse `env` names."""
    res = _dbt(env, "compile", "--select", model, "--full-refresh",
               project=project)
    (node,) = [r.node for r in res.result.results
               if r.node.name == model]
    return node.compiled_code


def replace_days(con, model: str, days, sql: str) -> tuple[int, int]:
    """Swap a partitioned mart's rows for chosen days with what `sql`
    gives for them now, in one transaction: a reader sees the days as
    they were or as they are, never half of each. Returns rows taken
    out and rows put in."""
    col = PARTITIONED[model]
    days = sorted(set(days))
    con.execute("BEGIN")
    try:
        (gone,) = con.execute(
            f"SELECT count(*) FROM marts.{model} WHERE {col} IN "
            f"(SELECT unnest(?::DATE[]))", [days]).fetchone()
        con.execute(f"DELETE FROM marts.{model} WHERE {col} IN "
                    f"(SELECT unnest(?::DATE[]))", [days])
        con.execute(f"INSERT INTO marts.{model} BY NAME SELECT * "
                    f"FROM ({sql}) WHERE {col} IN "
                    "(SELECT unnest(?::DATE[]))", [days])
        (now,) = con.execute(
            f"SELECT count(*) FROM marts.{model} WHERE {col} IN "
            f"(SELECT unnest(?::DATE[]))", [days]).fetchone()
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return gone, now


# ----------------------------------------------------------------- CLI
def sandbox_env(folder: Path, **kw) -> runner.Env:
    """A Groundwork in a folder of its own: warehouse.duckdb, raw/ and
    landed/, built by dbt's dev target, its target/ kept inside."""
    folder = Path(folder)
    return runner.Env(warehouse=folder / "warehouse.duckdb",
                      root=folder / "raw", archive=folder / "landed",
                      dbt_target="dev", dbt_target_path=folder / "dbt",
                      **kw)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m groundwork.orchestrate backfill",
        description="Backfill Groundwork for a range of logical dates.")
    p.add_argument("--from", dest="first", type=date.fromisoformat,
                   required=True, help="the first logical date")
    p.add_argument("--to", dest="last", type=date.fromisoformat,
                   required=True, help="the last logical date")
    p.add_argument("--parallel", type=int, default=1,
                   help="nights whose sources are replayed ahead")
    p.add_argument("--pause", type=float, default=0.0,
                   help="seconds to wait between nights")
    p.add_argument("--no-build", action="store_true",
                   help="the nights only: no build, reconcile, publish")
    p.add_argument("--as-it-stands", nargs="*", default=[],
                   metavar="SOURCE", help="load these sources as they "
                   "stand now, first (a source with no past)")
    p.add_argument("--sandbox", type=Path, help="backfill the "
                   "Groundwork in this folder, not data/groundwork")
    a = p.parse_args(argv)
    env = sandbox_env(a.sandbox) if a.sandbox else runner.Env()
    replay = Replay((a.sandbox or GROUNDWORK) / "replay", a.parallel)
    b = run(a.first, a.last, env=env, replay=replay,
            parallel=a.parallel, pause=a.pause, build=not a.no_build,
            as_it_stands=a.as_it_stands)
    print(b.report())
    return 0 if b.state == "succeeded" else 1
