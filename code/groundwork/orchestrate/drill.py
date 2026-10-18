"""
The rerun drill: pick days at random, rebuild each in isolation, and
compare what comes out with what is there.

Every backfill and every rerun rests on one promise: a night run again
gives the same answer, whenever it is run. The drill tests it the way
a fire drill tests the exits, on days nobody chose. For each day D:

  raw       Groundwork as it stood after night D-1 is put together in
            a scratch folder (the warehouse copied and its records cut
            back to the nights before D; raw's older partitions linked,
            not copied), the sources are replayed to 06:00 on D+1, and
            night D's loads run there. Every partition the night wrote
            must hold the same rows as Groundwork's, the columns a run
            adds (_loaded_at, _load_id, _source_file) aside, and the
            desk's pages must be the same bytes.
  facts     the marts partitioned by day are worked out again for D by
            each model's own SQL, and must equal the rows held for D.
  snapshot  the versions the snapshot opened and closed on night D
            must be the accounts whose segment, owner or status
            differs between D's export and the export before it.

Nothing in Groundwork is written: the rebuild happens in the scratch
folder, which is deleted, and the checks of the marts only read.

    pick(env, n=20, seed=SEED)        days from the nights run
    as_of(env, day, dest)             Groundwork after night `day`
    rebuild(env, day, scratch)        night D again, in isolation
    drill(env, days, parallel=2)      -> [Result], one per day
    python -m groundwork.orchestrate drill [--days 20] [--sandbox DIR]
"""
from __future__ import annotations

import argparse
import multiprocessing as mp
import os
import random
import shutil
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

import duckdb

from groundwork.config import GROUNDWORK, SEED
from groundwork.orchestrate import backfill, runner

TRIM = {                          # what each record holds per night
    "load_log": "load_date > ?",
    "file_ledger": "load_date > ?",
    "quarantine": "load_date > ?",
    "contract_runs": "CAST(split_part(load_id, '.', 3) AS DATE) > ?",
    "dag_runs": "logical_date > ?",
    "task_runs": "logical_date > ?",
    "published": "logical_date > ?",
}


@dataclass(frozen=True)
class Check:
    """One comparison: what, how many rows, how many differ."""
    what: str
    rows: int
    differ: int
    note: str = ""


@dataclass
class Result:
    day: date
    checks: list = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.differ == 0 for c in self.checks)

    def failures(self) -> list[Check]:
        return [c for c in self.checks if c.differ]


# ------------------------------------------- Groundwork as of a night
def _part_day(rel: Path) -> date | None:
    for p in rel.parts:
        if p.startswith("load_date="):
            return date.fromisoformat(p.removeprefix("load_date="))
    return None


def as_of(env: runner.Env, day: date, dest: Path) -> runner.Env:
    """A scratch Groundwork as it stood after night `day`: raw's
    partitions up to that night, hard-linked (a rebuild writes new
    files beside them and never into them); the warehouse copied, with
    every record of a later night taken out."""
    dest = Path(dest)
    shutil.rmtree(dest, ignore_errors=True)
    out = backfill.sandbox_env(dest)
    Path(out.root).mkdir(parents=True)
    Path(out.archive).mkdir(parents=True)
    for folder, _, names in os.walk(env.root):
        for n in names:
            src = Path(folder) / n
            rel = src.relative_to(env.root)
            d = _part_day(rel)
            if d is None or d > day or n.startswith("."):
                continue
            (out.root / rel).parent.mkdir(parents=True, exist_ok=True)
            os.link(src, out.root / rel)
    shutil.copy(env.warehouse, out.warehouse)
    wal = Path(f"{env.warehouse}.wal")
    if wal.exists():
        shutil.copy(wal, f"{out.warehouse}.wal")
    con = duckdb.connect(str(out.warehouse))
    con.execute("SET enable_progress_bar = false")
    try:
        have = {t for (t,) in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE "
            "table_schema = 'audit' AND table_type = 'BASE TABLE'"
        ).fetchall()}
        for table, where in TRIM.items():
            if table in have:
                con.execute(f"DELETE FROM audit.{table} WHERE {where}",
                            [day])
        try:
            con.execute("DELETE FROM snapshots.snp_relay__accounts "
                        "WHERE CAST(dbt_valid_from AS DATE) > ?", [day])
            con.execute("UPDATE snapshots.snp_relay__accounts SET "
                        "dbt_valid_to = NULL WHERE "
                        "CAST(dbt_valid_to AS DATE) > ?", [day])
        except duckdb.CatalogException:
            pass
        from groundwork.storage import raw_views
        raw_views(con, out.root)
    finally:
        con.close()
    return out


# ---------------------------------------------------- comparing raw
def _columns(con, path: Path) -> list[str]:
    return [r[0] for r in con.execute(
        "SELECT name FROM parquet_schema(?) WHERE num_children IS NULL",
        [str(path)]).fetchall()]


def same_rows(a: Path, b: Path) -> Check:
    """Two partitions of one table: rows in the first, and rows found
    in one and not the other, the columns a run adds aside."""
    con = duckdb.connect().execute("SET enable_progress_bar = false")
    try:
        ca = [c for c in _columns(con, a) if not c.startswith("_")]
        cb = [c for c in _columns(con, b) if not c.startswith("_")]
        (rows,) = con.execute("SELECT count(*) FROM read_parquet(?)",
                              [str(a)]).fetchone()
        if ca != cb:
            return Check("", rows, rows, "columns differ")
        cols = ", ".join(f'"{c}"' for c in ca)
        (differ,) = con.execute(f"""
            SELECT count(*) FROM (
              (SELECT {cols} FROM read_parquet($a) EXCEPT ALL
               SELECT {cols} FROM read_parquet($b))
              UNION ALL
              (SELECT {cols} FROM read_parquet($b) EXCEPT ALL
               SELECT {cols} FROM read_parquet($a)))""",
                                {"a": str(a), "b": str(b)}).fetchone()
    finally:
        con.close()
    return Check("", rows, differ)


def _pages(root: Path, day: date) -> dict[str, bytes]:
    """The desk's pages for a night, from its last attempt."""
    folder = root / "frontline" / "pages" / f"load_date={day}"
    tries = sorted(folder.glob("attempt-*"),
                   key=lambda p: int(p.name.split("-")[1]))
    if not tries:
        return {}
    return {p.name: p.read_bytes() for p in sorted(tries[-1].iterdir())}


def compare_raw(theirs: Path, ours: Path, day: date) -> list[Check]:
    """Every raw table's partition for `day` in Groundwork against the
    rebuilt one; then the desk's pages, byte for byte."""
    out = []
    tables = sorted({p.parent.parent.relative_to(root)
                     for root in (theirs, ours)
                     for p in root.glob(f"*/*/load_date={day}/part-0"
                                        ".parquet")})
    for t in tables:
        a = theirs / t / f"load_date={day}" / "part-0.parquet"
        b = ours / t / f"load_date={day}" / "part-0.parquet"
        name = t.as_posix().replace("/", ".")
        if not (a.exists() and b.exists()):
            n = "only rebuilt" if b.exists() else "only in Groundwork"
            out.append(Check(name, 0, 1, n))
            continue
        c = same_rows(a, b)
        out.append(Check(name, c.rows, c.differ, c.note))
    pa, pb = _pages(theirs, day), _pages(ours, day)
    if pa or pb:
        differ = sum(pa.get(k) != pb.get(k) for k in set(pa) | set(pb))
        out.append(Check("frontline pages", len(pa), differ))
    return out


def rebuild(env: runner.Env, day, scratch: Path,
            keep: bool = False) -> list[Check]:
    """Night `day` run again, alone, against Groundwork as it stood the
    night before and the sources as they stood when its run was due;
    then compared with what Groundwork holds."""
    from groundwork.ingest import frontline_comments
    from groundwork.ingest.__main__ import SOURCES, run
    day = date.fromisoformat(str(day))
    scratch = Path(scratch)
    try:
        before = day - timedelta(days=1)
        iso = as_of(env, before, scratch / "groundwork")
        rp = backfill.Replay(scratch / "replay", parallel=0)
        p = rp.prepare(day)
        e = rp.env(iso, p)
        backfill.load_relay(e, day, p.at)       # as the backfill did
        run(day, run_at=p.at, only=[s for s in SOURCES if s != "relay"],
            landing=e.landing, reference_dir=e.reference,
            tradeline_db=e.tradeline_db,
            frontline_base=e.frontline_base, root=e.root,
            warehouse=e.warehouse, archive=e.archive)
        frontline_comments.load(day, run_at=p.at, base=e.frontline_base,
                                root=e.root, warehouse=e.warehouse)
        return compare_raw(Path(env.root), Path(e.root), day)
    finally:
        if not keep:
            shutil.rmtree(scratch, ignore_errors=True)


# ------------------------------------------ comparing marts, snapshot
def compare_facts(env: runner.Env, days) -> dict:
    """{(day, model): Check}: each partitioned mart's rows for the days,
    worked out again by the model's SQL, against the rows held."""
    days = sorted(days)
    sqls = {m: backfill.compiled(m, env) for m in backfill.PARTITIONED}
    con = duckdb.connect(str(env.warehouse), read_only=True)
    con.execute("SET enable_progress_bar = false")
    out = {}
    try:
        for model, col in backfill.PARTITIONED.items():
            cols = ", ".join(f'"{c}"' for (c,) in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'marts' AND table_name = ? "
                "ORDER BY ordinal_position", [model]).fetchall())
            pick = f"{col} IN (SELECT unnest($days::DATE[]))"
            rows = dict(con.execute(
                f"SELECT {col}, count(*) FROM marts.{model} "
                f"WHERE {pick} GROUP BY 1", {"days": days}).fetchall())
            differ = dict(con.execute(f"""
                WITH now AS (SELECT {cols} FROM ({sqls[model]})
                             WHERE {pick}),
                     held AS (SELECT {cols} FROM marts.{model}
                              WHERE {pick})
                SELECT {col}, count(*) FROM (
                    (SELECT * FROM now EXCEPT ALL SELECT * FROM held)
                    UNION ALL
                    (SELECT * FROM held EXCEPT ALL SELECT * FROM now))
                GROUP BY 1""", {"days": days}).fetchall())
            for d in days:
                out[(d, model)] = Check(model, rows.get(d, 0),
                                        differ.get(d, 0))
    finally:
        con.close()
    return out


SNAPSHOT_SQL = """
WITH tonight AS (
    SELECT account_id, segment, owner_name, status
    FROM staging.stg_relay__accounts WHERE snapshot_date = $day),
before AS (
    SELECT account_id, segment, owner_name, status
    FROM staging.stg_relay__accounts
    WHERE snapshot_date = (SELECT max(snapshot_date)
                           FROM staging.stg_relay__accounts
                           WHERE snapshot_date < $day)),
expected AS (
    SELECT 'opened' AS what, account_id
    FROM (SELECT * FROM tonight EXCEPT SELECT * FROM before)
    UNION ALL
    SELECT 'closed', account_id
    FROM (SELECT * FROM before EXCEPT SELECT * FROM tonight)
    WHERE (SELECT count(*) FROM tonight) > 0),
held AS (
    SELECT 'opened' AS what, account_id
    FROM snapshots.snp_relay__accounts
    WHERE CAST(dbt_valid_from AS DATE) = $day
    UNION ALL
    SELECT 'closed', account_id FROM snapshots.snp_relay__accounts
    WHERE CAST(dbt_valid_to AS DATE) = $day)
SELECT (SELECT count(*) FROM held),
       (SELECT count(*) FROM (
           (SELECT * FROM expected EXCEPT ALL SELECT * FROM held)
           UNION ALL
           (SELECT * FROM held EXCEPT ALL SELECT * FROM expected)))
"""


def compare_snapshot(env: runner.Env, day: date) -> Check:
    """The snapshot's versions opened and closed on night `day`, against
    the change between that night's export and the one before."""
    con = duckdb.connect(str(env.warehouse), read_only=True)
    con.execute("SET enable_progress_bar = false")
    try:
        held, differ = con.execute(SNAPSHOT_SQL,
                                   {"day": day}).fetchone()
    finally:
        con.close()
    return Check("snapshot", held, differ)


# ------------------------------------------------------------ the drill
def pick(env: runner.Env, n: int = 20, seed: int = SEED) -> list[date]:
    """n nights a run finished, by a backfill or the nightly DAG,
    chosen by the book's one seed."""
    con = duckdb.connect(str(env.warehouse), read_only=True)
    con.execute("SET enable_progress_bar = false")
    try:
        done = [d for (d,) in con.execute(
            "SELECT DISTINCT logical_date FROM audit.dag_runs WHERE "
            "dag IN ('backfill', 'nightly') AND state = 'succeeded' "
            "ORDER BY 1").fetchall()]
    finally:
        con.close()
    return sorted(random.Random(seed).sample(done, min(n, len(done))))


def _quiet() -> None:
    """A rebuild prints nothing: what it found is in its Checks."""
    devnull = os.open(os.devnull, os.O_WRONLY)
    os.dup2(devnull, 1)


def drill(env: runner.Env, days=None, *, n: int = 20, seed: int = SEED,
          parallel: int = 2, scratch: Path | None = None
          ) -> list[Result]:
    """Rebuild each day in isolation, `parallel` at a time, and check
    the marts and the snapshot for it. One Result a day, in order."""
    days = sorted(days) if days else pick(env, n, seed)
    scratch = Path(scratch or Path(env.warehouse).parent / "drill")
    results = {d: Result(d) for d in days}
    ctx = mp.get_context("spawn")
    with ProcessPoolExecutor(max_workers=max(1, parallel),
                             mp_context=ctx,
                             initializer=_quiet) as pool:
        jobs = {d: pool.submit(rebuild, env, d, scratch / d.isoformat())
                for d in days}
        for d, job in jobs.items():
            results[d].checks += job.result()
    facts = compare_facts(env, days)
    for d in days:
        results[d].checks += [facts[(d, m)]
                              for m in backfill.PARTITIONED]
        results[d].checks.append(compare_snapshot(env, d))
    shutil.rmtree(scratch, ignore_errors=True)
    return [results[d] for d in days]


def report(results: list[Result]) -> str:
    out = [f"{'day':<12}{'raw':>6}{'rows':>10}{'facts':>8}"
           f"{'snapshot':>10}  verdict"]
    for r in results:
        raw = [c for c in r.checks if c.what not in
               (*backfill.PARTITIONED, "snapshot")]
        facts = [c for c in r.checks if c.what in backfill.PARTITIONED]
        snap = [c for c in r.checks if c.what == "snapshot"]
        out.append(f"{r.day!s:<12}{len(raw):>6}"
                   f"{sum(c.rows for c in raw):>10,}"
                   f"{sum(c.rows for c in facts):>8,}"
                   f"{sum(c.rows for c in snap):>10,}  "
                   + ("same" if r.passed else "DIFFERS"))
        for c in r.failures():
            out.append(f"  {c.what}: {c.differ:,} of {c.rows:,} rows "
                       "differ" + (f" ({c.note})" if c.note else ""))
    ok = sum(r.passed for r in results)
    out.append(f"{ok} of {len(results)} days rebuilt the same")
    return "\n".join(out)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="python -m groundwork.orchestrate drill",
        description="Rebuild random days in isolation and compare.")
    p.add_argument("--days", type=int, default=20,
                   help="how many days to pick")
    p.add_argument("--day", type=date.fromisoformat, nargs="*",
                   help="these days instead of a random pick")
    p.add_argument("--seed", type=int, default=SEED)
    p.add_argument("--parallel", type=int, default=2)
    p.add_argument("--sandbox", type=Path, help="drill the Groundwork "
                   "in this folder instead of data/groundwork")
    a = p.parse_args(argv)
    env = backfill.sandbox_env(a.sandbox) if a.sandbox else runner.Env()
    results = drill(env, a.day, n=a.days, seed=a.seed,
                    parallel=a.parallel,
                    scratch=(a.sandbox or GROUNDWORK) / "drill")
    print(report(results))
    return 0 if all(r.passed for r in results) else 1
