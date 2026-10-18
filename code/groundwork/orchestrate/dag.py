"""
Groundwork's nightly run, as a graph of tasks.

A task is a function of the run's context, with the names of the
tasks it waits for. A sensor is a task that asks a question, "has the
file arrived?", and is asked again later when the answer is no. A DAG
is the tasks, checked once for names it does not know and for cycles,
and put in an order where every task comes after what it waits for.

    Task(name, fn, upstream=(), retries=0, retry_delay=300,
         backoff=2.0, max_delay=3600, timeout=None, isolate=False)
    Sensor(name, poke, upstream=(), every=600, timeout=6 * 3600)
    DAG(name, tasks)    .order()  .levels()  .downstream(names)
    nightly(checks=())  the DAG Groundwork runs for every logical day

The nightly DAG, for logical date D, run at 06:00 UTC on D+1:

    wait_relay -> ingest_relay ---\\
    ingest_tradeline -------------+-> check_contracts -\\
    ingest_frontline -------------+-> check_quality ---+-> dbt_build
    ingest_suppliers, _catalogue, |                    |      |
    _ledger, _reference ----------/                    |  reconcile,
    ingest_frontline -> load_comments -----------------/  [checks]
                                                              |
                                                           publish

Every ingest is Chapter 10's `ingest.run` for one source, failing the
task when a table broke, failed or went missing. The comments loader
is Chapter 13's. check_contracts holds the day's batches to their
contracts again (Chapter 10); check_quality is Chapter 15's checks on
the loads, before anything is built from them. dbt_build is Chapter
14's project, with Chapter 15's tests in it, `night` set to the
logical date and `since` to the moment of the last Tradeline read a
successful build consumed. reconcile is Chapter 15's, on the built
warehouse, before anyone is told it is ready. `checks` are extra
tasks between the build and publish: Chapter 21's monitors and
Chapter 24's governance plug in there.

Task functions are module-level, so a task can run in a process of
its own (isolate=True), which is what lets a timeout stop it.
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from dataclasses import dataclass, replace
from datetime import datetime, time, timedelta
from functools import partial
from pathlib import Path
from typing import Callable

from groundwork.config import ROOT


class Fatal(Exception):
    """A failure that trying again cannot fix: a broken contract, a
    test that found bad data. The runner does not retry it."""


@dataclass(frozen=True)
class Task:
    """One step of a run, and what it waits for."""
    name: str
    fn: Callable
    upstream: tuple = ()
    retries: int = 0                  # tries after the first
    retry_delay: float = 300.0        # seconds before the first retry
    backoff: float = 2.0              # each later wait is this x longer
    max_delay: float = 3600.0         # ... up to this
    timeout: float | None = None      # seconds a try may take
    isolate: bool = False             # run in its own process
    sensor: bool = False

    def delay(self, tries: int) -> float:
        """Seconds to wait after the try numbered `tries` failed."""
        return min(self.max_delay,
                   self.retry_delay * self.backoff ** (tries - 1))


def Sensor(name, poke, upstream=(), every=600.0,
           timeout=6 * 3600.0) -> Task:
    """A task that answers yes or no, and is asked again `every`
    seconds until yes, or until `timeout` has passed since the first
    question, when it fails."""
    return Task(name, poke, tuple(upstream), retry_delay=every,
                timeout=timeout, sensor=True)


class CycleError(ValueError):
    pass


class DAG:
    """Tasks and their dependencies, checked when it is made."""

    def __init__(self, name: str, tasks):
        self.name = name
        self.tasks = {}
        for t in tasks:
            if t.name in self.tasks:
                raise ValueError(f"two tasks are called {t.name}")
            self.tasks[t.name] = t
        for t in self.tasks.values():
            for u in t.upstream:
                if u not in self.tasks:
                    raise ValueError(f"{t.name} waits for {u}, "
                                     "which is not a task")
        self.order()                      # refuse a cycle now

    def order(self) -> list[str]:
        """Every task after everything it waits for; ties keep the
        order the tasks were given in."""
        done, out = set(), []
        names = list(self.tasks)
        while len(out) < len(names):
            ready = [n for n in names if n not in done and
                     all(u in done for u in self.tasks[n].upstream)]
            if not ready:
                stuck = [n for n in names if n not in done]
                raise CycleError("a cycle among: " + ", ".join(stuck))
            for n in ready:
                done.add(n)
                out.append(n)
        return out

    def levels(self) -> dict[str, int]:
        """0 for a task that waits for nothing, else one more than
        the highest level it waits for."""
        lvl = {}
        for n in self.order():
            ups = self.tasks[n].upstream
            lvl[n] = 1 + max((lvl[u] for u in ups), default=-1)
        return lvl

    def downstream(self, names) -> set[str]:
        """The tasks that wait, directly or not, for any of `names`."""
        out, todo = set(), list(names)
        while todo:
            n = todo.pop()
            for t in self.tasks.values():
                if n in t.upstream and t.name not in out:
                    out.add(t.name)
                    todo.append(t.name)
        return out

    def edges(self) -> list[tuple[str, str]]:
        return [(u, t.name) for t in self.tasks.values()
                for u in t.upstream]

    def stubbed(self, fns: dict) -> "DAG":
        """The same graph with some tasks' functions replaced: for
        trying the runner without touching anything real."""
        return DAG(self.name, [replace(t, fn=fns.get(t.name, t.fn),
                                       isolate=False)
                               for t in self.tasks.values()])


# ----------------------------------------------------------- the tasks
EXPORT = ("accounts.csv", "contracts.csv")


def relay_export(ctx) -> bool:
    """Sensor: has Relay's export for the logical night landed, and
    been there long enough to be whole?"""
    from groundwork.ingest import files
    folder = f"relay/{ctx.day.isoformat()}"
    seen = {a.name.rsplit("/", 1)[1] for a in
            files.discover(ctx.env.landing, folder, by=ctx.now())}
    return all(n in seen for n in EXPORT)


def ingest(sources: tuple, ctx) -> str:
    """Chapter 10's ingest for some sources and the logical day.
    A broken contract is fatal; anything else may pass on a retry."""
    from groundwork.ingest.__main__ import BAD, run
    e = ctx.env
    lines = run(ctx.day, run_at=ctx.now(), only=list(sources),
                landing=e.landing, reference_dir=e.reference,
                tradeline_db=e.tradeline_db,
                frontline_base=e.frontline_base, root=e.root,
                warehouse=e.warehouse, archive=e.archive)
    bad = [x for x in lines if x.status in BAD]
    for x in bad:
        why = f"{x.source}.{x.table} {x.status}: {x.detail}"
        if x.status == "broken":
            raise Fatal(why)
    if bad:
        raise RuntimeError("; ".join(f"{x.source}.{x.table} {x.status}"
                                     f": {x.detail}" for x in bad))
    rows = sum(x.loaded for x in lines)
    parked = sum(x.parked for x in lines)
    return (f"{rows:,} rows, {len(lines)} table"
            f"{'s' * (len(lines) > 1)}"
            + (f", {parked:,} parked" if parked else ""))


def comments(ctx) -> str:
    """Chapter 13's comments loader, after the night's tickets."""
    from groundwork.ingest import frontline_comments
    base = ctx.env.frontline_base
    if base is None:                  # Meridian's desk, served locally
        from meridian.sources import Clock
        from meridian.sources import frontline as desk
        base = desk.serve(Clock(ctx.now()))
    x = frontline_comments.load(ctx.day, run_at=ctx.now(), base=base,
                                root=ctx.env.root,
                                warehouse=ctx.env.warehouse)
    return f"{x.rows_written:,} comments"


def contracts_hold(ctx) -> str:
    """Every raw table's batches for the logical day, held again to
    its contract: the gate before anything is built from them."""
    import duckdb

    from groundwork import contracts
    from groundwork.contracts import check
    con = duckdb.connect(str(ctx.env.warehouse), read_only=True)
    try:
        views = {r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'raw'").fetchall()}
        batches, rows, broken = 0, 0, []
        for table in contracts.names():
            if table.replace(".", "_") not in views:
                continue
            for ref, name, res in check.audit(
                    con, table, load_date=ctx.day,
                    archive=ctx.env.archive):
                batches += 1
                rows += res.rows
                if res.breaking:
                    broken.append(f"{table} {name}: {res.breaking[0]}")
    finally:
        con.close()
    if broken:
        raise Fatal("; ".join(broken[:3]))
    return f"{batches} batches, {rows:,} rows"


def judged_at(ctx) -> datetime:
    """The moment Chapter 15's checks judge the night as of: when the
    logical day falls due, an hour after its run is due to start.
    Worked out from the logical date, so a rerun or a backfill judges
    the same day the same way."""
    from groundwork import quality
    return (datetime.combine(ctx.day + timedelta(days=1),
                             time(quality.RUN_HOUR)) + quality.DUE)


def loads_hold(ctx) -> str:
    """Chapter 15's checks on the night's loads (freshness, volume,
    partitions), recorded, before the build. An error is fatal."""
    import duckdb

    from groundwork import quality
    at = judged_at(ctx)
    con = duckdb.connect(str(ctx.env.warehouse))
    try:
        checks = quality.check(con, at, ctx.env.root)
        quality.record(con, checks, at)
    finally:
        con.close()
    errors = [c for c in checks if c.status == "error"]
    if errors:
        raise Fatal("; ".join(f"{c.kind} {c.name}: {c.detail}"
                              for c in errors[:3]))
    warned = [c.name for c in checks if c.status == "warn"]
    if warned:
        return f"{len(checks)} checks, warned: {', '.join(warned)}"
    return f"{len(checks)} checks, no errors"


def ties(ctx) -> str:
    """Chapter 15's reconciliation of the built warehouse with the
    ledger and the sources, recorded. Anything that does not tie is
    fatal: publish waits for it."""
    import duckdb

    from groundwork import reconcile as rc
    con = duckdb.connect(str(ctx.env.warehouse))
    try:
        con.execute("SET enable_progress_bar = false")
        cells = rc.revenue(con)
        b = rc.bridge(con, max(c.period for c in cells))
        counts = rc.counts(con, ctx.env.tradeline_db, ctx.env.landing)
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


DBT = Path(sys.executable).parent / "dbt"
PROJECT = ROOT / "dbt" / "groundwork"


def dbt_vars(ctx) -> dict:
    """What the dbt project is told about this run: the night its
    snapshot records, and where the incremental fact starts."""
    out = {"night": ctx.day.isoformat()}
    if ctx.since is not None:
        out["since"] = f"{ctx.since:%Y-%m-%d %H:%M:%S}"
    return out


def dbt_build(ctx) -> str:
    """`dbt build` on Chapter 14's project. A failed test is fatal;
    a model that errored may pass on a retry."""
    e = ctx.env
    target = Path(e.dbt_target_path or PROJECT / "target")
    env = dict(os.environ, DBT_SEND_ANONYMOUS_USAGE_STATS="false")
    if e.dbt_target == "dev":
        env["GROUNDWORK_DBT_PATH"] = str(Path(e.warehouse).resolve())
    cmd = [str(DBT), "build", "--target", e.dbt_target,
           "--profiles-dir", ".", "--vars", json.dumps(dbt_vars(ctx)),
           "--target-path", str(target), "--log-path",
           str(target / "logs"), "--quiet"]
    done = subprocess.run(cmd, cwd=PROJECT, env=env, text=True,
                          capture_output=True)
    results = target / "run_results.json"
    got = {}
    if results.exists():
        for r in json.loads(results.read_text())["results"]:
            got[r["status"]] = got.get(r["status"], 0) + 1
    bad = {s: n for s, n in got.items() if s in ("error", "fail")}
    if done.returncode == 0:
        return "built, all tests passed" if not got.get(
            "warn") else f"built; {got['warn']} test(s) warned"
    last = (done.stdout + done.stderr).strip().splitlines()[-3:]
    if bad.get("fail"):
        raise Fatal(f"dbt build: {bad['fail']} test(s) failed")
    raise RuntimeError(f"dbt build exit {done.returncode}: "
                       + " | ".join(last))


PUBLISHED = """
CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS audit.published (
    dag          VARCHAR NOT NULL,
    logical_date DATE NOT NULL,
    run_id       VARCHAR NOT NULL,
    published_at TIMESTAMP NOT NULL,
    PRIMARY KEY (dag, logical_date)
);
"""


def publish(ctx) -> str:
    """Say that the logical day is built and tested: one row that a
    reader of the warehouse can ask for before trusting a day."""
    import duckdb
    con = duckdb.connect(str(ctx.env.warehouse))
    try:
        con.execute(PUBLISHED)
        con.execute("INSERT OR REPLACE INTO audit.published "
                    "VALUES (?, ?, ?, ?)",
                    [ctx.dag, ctx.day, ctx.run_id, ctx.now()])
    finally:
        con.close()
    return f"{ctx.day} published"


# ---------------------------------------------------- the nightly DAG
def nightly(checks=()) -> DAG:
    """Groundwork's nightly DAG. `checks` are extra tasks to run
    after dbt_build and before publish (each waits for dbt_build)."""
    def ingest_task(name, *sources, upstream=(), retries=2):
        return Task(name, partial(ingest, sources), tuple(upstream),
                    retries=retries, retry_delay=300, timeout=3600,
                    isolate=True)

    files = ("suppliers", "catalogue", "ledger", "reference")
    loads = ("ingest_relay", "ingest_tradeline", "ingest_frontline",
             *[f"ingest_{s}" for s in files])
    tasks = [
        Sensor("wait_relay", relay_export, every=600,
               timeout=6 * 3600),
        ingest_task("ingest_relay", "relay", upstream=["wait_relay"]),
        ingest_task("ingest_tradeline", "tradeline"),
        ingest_task("ingest_frontline", "frontline"),
        Task("load_comments", comments, ("ingest_frontline",),
             retries=2, retry_delay=300, timeout=3600, isolate=True),
        *[ingest_task(f"ingest_{s}", s) for s in files],
        Task("check_contracts", contracts_hold, loads,
             timeout=1800, isolate=True),
        Task("check_quality", loads_hold, loads,
             timeout=900, isolate=True),
        Task("dbt_build", dbt_build,
             ("check_contracts", "check_quality", "load_comments"),
             retries=1, retry_delay=600, timeout=3600, isolate=True),
        Task("reconcile", ties, ("dbt_build",),
             timeout=1800, isolate=True),
    ]
    extra = [replace(c, upstream=("dbt_build",)) for c in checks]
    tasks += extra
    tasks.append(Task("publish", publish,
                      ("reconcile", *[c.name for c in extra]),
                      timeout=300))
    return DAG("nightly", tasks)


DAGS = {"nightly": nightly}
