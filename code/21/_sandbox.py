"""
Chapter 21's sandbox: a copy of Groundwork as the chapter found it,
the dbt project compiled against it, and replays of the summer of 2024.

    base()       a copy of the warehouse, raw and the landed archive,
                 trimmed back to the chapter's start (below), with
                 raw's views pointed at the copy (kept)
    compiled()   (manifest, schema): dbt compile into base/target, and
                 every relation's columns in the copy
    work(name)   a fresh copy of base for one listing; tidy(name)
    replay(name) Groundwork's nightly DAG run night by night from
                 1 June to 31 July 2024 against the sources as they
                 stood, into replay/<name>/ (kept; built once)

The replays differ only in how the supplier lists are read:

    unwatched    the contract as first written (version 1) and Chapter
                 9's reader, as Groundwork would have had them before
                 Pemberton changed its header; the fix (version 2 and
                 price_lists.read) goes in on 16 July, after a buyer
                 asked, and the last failed night is rerun
    watched      the same, with the fix on 3 July, after the page
    chapter9     Chapter 9's loader, no contract

Tradeline, Relay (with its sensor) and the suppliers run for real; the
other tasks are stand-ins that succeed at once, so that each night
takes a second or two. Every run is on a simulated clock that starts
at 06:00 UTC on the morning after its logical day.

Why base() trims its copy. The listings were captured when the real
warehouse had run one night for 4 January 2026, nightly.2026-01-04.1.
The real warehouse has run that night again since: Chapter 21 itself,
at its end (nightly.2026-01-04.2, with the monitor and the first
watch), and Chapter 26 (nightly.2026-01-04.3, with the sessions,
compat, serve and govern tasks). Copied as it is today, it would give
listing 02 those runs' loads to count and listing 09 a third run to
number, so base() takes back out what those later runs left:

    their dag_runs and task_runs, and the loads they made (the newest
    load of 4 January for each table, one per later run), with those
    loads' contract checks, ledger rows, quarantine and raw pages
    audit.metrics, audit.alerts and audit.notifications (the monitor
    and the watch first ran in the chapter's real night)
    audit.published put back to the first run's publication
    what Chapters 22 to 26 added: audit.book_reconciliations, the
    compat, features, govern, metrics, masked and role_* schemas, and
    the sessions Chapter 26's night wrote into marts.fct_portal_sessions
    (empty when Chapter 21 began)

The audit rows those runs replace rather than add (quality checks and
reconciliations, keyed on their judging moment) come out the same.

Not printed in the book.
"""
import json
import shutil
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from pathlib import Path

import duckdb

from groundwork import contracts, storage
from groundwork.config import GROUNDWORK, SANDBOX, WAREHOUSE
from groundwork.observe import lineage
from groundwork.orchestrate import runner
from groundwork.orchestrate.dag import Fatal, ingest, nightly
from meridian.sources import landing as arrivals
from meridian.sources import relay, tradeline

HERE = SANDBOX / "ch21"
BASE = HERE / "base"
FIRST, LAST = date(2024, 6, 1), date(2024, 7, 31)
REAL = ("wait_relay", "ingest_relay", "ingest_tradeline",
        "ingest_suppliers", "publish")
FIXES = {"unwatched": datetime(2024, 7, 16, 15, 0),
         "watched": datetime(2024, 7, 3, 15, 0), "chapter9": None}
VERSION = "1"
FIRST_RUN = "nightly.2026-01-04.1"    # the night the listings saw
TRIM = "2"                            # base()'s version: rebuilt when changed
LATER_SCHEMAS = ("compat", "features", "govern", "metrics", "masked",
                 "role_analyst", "role_finance", "role_manager_southwest",
                 "role_support")


# ---------------------------------------------------------- today's copy
def base() -> Path:
    """The shared copy of Groundwork as Chapter 21 found it, made the
    first time."""
    stamp = BASE / "trimmed"
    if not (stamp.exists() and stamp.read_text() == TRIM):
        shutil.rmtree(BASE, ignore_errors=True)
        BASE.mkdir(parents=True)
        shutil.copy(WAREHOUSE, BASE / "warehouse.duckdb")
        wal = WAREHOUSE.with_name(WAREHOUSE.name + ".wal")
        if wal.exists():
            shutil.copy(wal, BASE / "warehouse.duckdb.wal")
        for part in ("raw", "landed"):
            shutil.copytree(GROUNDWORK / part, BASE / part)
        con = duckdb.connect(str(BASE / "warehouse.duckdb"))
        con.execute("SET enable_progress_bar = false")
        trim(con, BASE / "raw")
        storage.raw_views(con, (BASE / "raw").resolve())
        con.execute("CHECKPOINT")
        con.close()
        # Compile dbt now, while nothing holds the file: dbt opens it to
        # write, and a listing that already has it open read-only (02
        # does) would make dbt fail to take the lock.
        lineage.compile_project(BASE / "warehouse.duckdb", BASE / "target")
        stamp.write_text(TRIM)
    return BASE


def trim(con, raw: Path) -> None:
    """Take out of the copy what later runs and chapters left (see the
    module's docstring)."""
    day = FIRST_RUN.split(".")[1]
    later = [r for (r,) in con.execute(
        "SELECT run_id FROM audit.dag_runs WHERE run_id <> ? "
        "ORDER BY run_id", [FIRST_RUN]).fetchall()]
    loads = [x for (x,) in con.execute("""
        SELECT load_id FROM (
            SELECT load_id, row_number() OVER (
                PARTITION BY source, table_name ORDER BY
                CAST(split_part(load_id, '.', -1) AS INTEGER) DESC) AS k
            FROM audit.load_log WHERE load_date = ?)
        WHERE k <= ? ORDER BY load_id""", [day, len(later)]).fetchall()]
    for table in ("dag_runs", "task_runs"):
        con.execute(f"DELETE FROM audit.{table} WHERE run_id IN "
                    f"(SELECT unnest(?))", [later])
    for table in ("load_log", "contract_runs", "file_ledger",
                  "quarantine"):
        con.execute(f"DELETE FROM audit.{table} WHERE load_id IN "
                    f"(SELECT unnest(?))", [loads])
    pages = raw / "frontline" / "pages" / f"load_date={day}"
    for x in loads:
        if x.startswith("frontline.tickets."):
            shutil.rmtree(pages / f"attempt-{x.rsplit('.', 1)[1]}",
                          ignore_errors=True)
    con.execute("""
        UPDATE audit.published SET run_id = t.run_id,
               published_at = t.finished_at
        FROM audit.task_runs t
        WHERE t.run_id = ? AND t.task = 'publish'
          AND published.dag = t.dag
          AND published.logical_date = t.logical_date""", [FIRST_RUN])
    for table in ("metrics", "alerts", "notifications",
                  "book_reconciliations"):
        con.execute(f"DROP TABLE IF EXISTS audit.{table}")
    for schema in LATER_SCHEMAS:
        con.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    con.execute("DELETE FROM marts.fct_portal_sessions")


def compiled():
    """dbt's manifest, compiled against the copy, and its columns."""
    b = base()
    path = b / "target" / "manifest.json"
    if not path.exists():
        lineage.compile_project(b / "warehouse.duckdb", b / "target")
    con = duckdb.connect(str(b / "warehouse.duckdb"), read_only=True)
    try:
        schema = lineage.schema_of(con)
    finally:
        con.close()
    return json.loads(path.read_text()), schema


def work(name: str) -> Path:
    src, dst = base(), HERE / "work" / name
    shutil.rmtree(dst, ignore_errors=True)
    shutil.copytree(src, dst, ignore=shutil.ignore_patterns("target"))
    return dst


def env(copy: Path, **kw) -> runner.Env:
    return runner.Env(warehouse=copy / "warehouse.duckdb",
                      root=copy / "raw", archive=copy / "landed",
                      dbt_target="dev", dbt_target_path=copy / "dbt",
                      **kw)


def tidy(name: str) -> None:
    shutil.rmtree(HERE / "work" / name, ignore_errors=True)


# --------------------------------------------------- the 2024 replays
def first_contract():
    """The price lists' contract as first written (Chapter 10)."""
    c = contracts.load("suppliers.price_lists")
    return replace(c, version=1, changes=c.changes[:1],
                   formats={k: v for k, v in c.formats.items()
                            if k != "pemberton-2024"})


def standin(ctx) -> str:
    return "stand-in"


def suppliers_v1(ctx) -> str:
    """The supplier lists through Chapter 9's reader, held to version
    1 of the contract: what Groundwork had before the rename."""
    from groundwork.ingest import files, suppliers
    from groundwork.ingest.__main__ import land
    e = ctx.env
    con = duckdb.connect(str(e.warehouse))
    try:
        x = land(con, "suppliers", "price_lists",
                 files.discover(e.landing, "suppliers", by=ctx.now()),
                 ctx.day, ctx.now(), suppliers.read, "suppliers",
                 e.root, e.archive, contract=first_contract())
        storage.raw_views(con, e.root)
    finally:
        con.close()
    if x.status == "broken":
        raise Fatal(f"suppliers.price_lists broken: {x.detail}")
    if x.status == "failed":
        raise RuntimeError(x.detail)
    return f"{x.loaded:,} rows, 1 table" + (
        f", {x.parked:,} parked" if x.parked else "")


def suppliers_fixed(ctx) -> str:
    """Today's: version 2 and price_lists.read (Chapter 10)."""
    return ingest(("suppliers",), ctx)


def suppliers_ch9(ctx) -> str:
    """Chapter 9's loader, with no contract."""
    from groundwork.ingest import suppliers
    e = ctx.env
    x = suppliers.load(ctx.day, run_at=ctx.now(), landing=e.landing,
                       root=e.root, warehouse=e.warehouse,
                       archive=e.archive)
    return f"{x.rows_written or 0:,} rows, 1 table"


def _dag(name: str, fixed: bool):
    real = set(REAL)
    if name == "chapter9":
        real = {"ingest_suppliers", "publish"}
    fns = {t: standin for t in nightly().tasks if t not in real}
    fns["ingest_suppliers"] = (suppliers_ch9 if name == "chapter9" else
                               suppliers_fixed if fixed else suppliers_v1)
    return nightly().stubbed(fns)


def nights():
    d = FIRST
    while d <= LAST:
        yield d
        d += timedelta(days=1)


def replay(name: str) -> Path:
    """The summer of 2024, night by night, as `name` (kept)."""
    folder = HERE / "replay" / name
    stamp = folder / "done"
    if stamp.exists() and stamp.read_text() == VERSION:
        return folder
    shutil.rmtree(folder, ignore_errors=True)
    folder.mkdir(parents=True)
    land, tdb = folder / "landing", HERE / "replay" / "tradeline.db"
    e = runner.Env(warehouse=folder / "warehouse.duckdb",
                   root=folder / "raw", archive=folder / "landed",
                   landing=land, tradeline_db=tdb, dbt_target="dev")
    fix, fixed = FIXES[name], False
    for day in nights():
        run_at = datetime.combine(day + timedelta(days=1), time(6))
        if fix is not None and not fixed and fix <= run_at:
            fixed = True                  # rerun the last night that failed
            con = duckdb.connect(str(e.warehouse), read_only=True)
            (bad,) = con.execute(
                "SELECT max(logical_date) FROM audit.dag_runs WHERE "
                "state = 'failed'").fetchone()
            con.close()
            runner.run(_dag(name, True), bad, run_at=fix, env=e,
                       clock=runner.SimClock(fix), rerun=True)
        arrivals.stage(run_at, land, under="suppliers")
        relay.export(day, land / "relay")
        if name != "chapter9":
            tradeline.at(run_at, tdb)
        runner.run(_dag(name, fixed), day, env=e,
                   clock=runner.SimClock(run_at))
    tdb.unlink(missing_ok=True)
    stamp.write_text(VERSION)
    return folder


def morning(day: date) -> datetime:
    """07:00 UTC on the day after `day`: when a night is judged."""
    return datetime.combine(day + timedelta(days=1), time(7))


def copy_of(name: str, to: str) -> Path:
    """A throwaway copy of a replay's warehouse, for a listing that
    writes (the watch records its alerts)."""
    src = replay(name) / "warehouse.duckdb"
    dst = HERE / "work" / to / "warehouse.duckdb"
    shutil.rmtree(dst.parent, ignore_errors=True)
    dst.parent.mkdir(parents=True)
    shutil.copy(src, dst)
    return dst
