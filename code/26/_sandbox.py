"""
Chapter 26's sandbox: one Groundwork built from nothing, which every
listing after the first reads.

02_from_nothing.py builds it, in data/sandbox/ch26/fresh, with the
command a reader types (`make warehouse FOLDER=...`, which is
groundwork.nightly.build). If a later listing finds no build there, it
makes one the same way first, which takes several minutes. Nothing
here touches data/groundwork.
"""
import duckdb

from groundwork import nightly
from groundwork.config import SANDBOX

HERE = SANDBOX / "ch26"
FRESH = HERE / "fresh"
WAREHOUSE = FRESH / "warehouse.duckdb"


def built() -> bool:
    """A build that published its night."""
    if not WAREHOUSE.exists():
        return False
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    try:
        (n,) = con.execute("SELECT count(*) FROM duckdb_tables() WHERE "
                           "schema_name = 'audit' AND table_name = "
                           "'published'").fetchone()
        return n == 1 and con.execute(
            "SELECT count(*) FROM audit.published").fetchone()[0] == 1
    finally:
        con.close()


def fresh():
    """The from-nothing build's warehouse, built if need be."""
    if not built():
        if FRESH.exists():         # being built, or a build that failed
            raise SystemExit(f"{FRESH} holds no published night: wait "
                             "for its build, or delete it and rerun")
        run = nightly.build(FRESH)
        if run.state != "succeeded":
            raise SystemExit(run.report())
    return WAREHOUSE


def connect(read_only: bool = True):
    con = duckdb.connect(str(fresh()), read_only=read_only)
    con.execute("SET enable_progress_bar = false")
    return con
