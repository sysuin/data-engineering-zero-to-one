"""
Chapter 22's sandbox: a fresh copy of Groundwork's warehouse for each
listing that needs one, so that nothing here writes to the real file
(DuckDB allows one writer, and the nightly run is it).

The real warehouse's fct_portal_sessions is still the empty table
Chapter 4 created until Chapter 19's stream writes to it; the copy
takes the sessions that stream built in Chapter 19's sandbox.
"""
import shutil

import duckdb

from groundwork.config import SANDBOX, WAREHOUSE

HERE = SANDBOX / "ch22"
PORTAL = SANDBOX / "ch19" / "portal.duckdb"


def warehouse(name: str = "warehouse.duckdb"):
    """A fresh copy of the warehouse; its path."""
    HERE.mkdir(parents=True, exist_ok=True)
    path = HERE / name
    wal = path.with_name(path.name + ".wal")
    path.unlink(missing_ok=True)
    wal.unlink(missing_ok=True)
    shutil.copy(WAREHOUSE, path)
    real_wal = WAREHOUSE.with_name(WAREHOUSE.name + ".wal")
    if real_wal.exists():
        shutil.copy(real_wal, wal)
    con = duckdb.connect(str(path))
    (n,) = con.execute("SELECT count(*) FROM marts.fct_portal_sessions"
                       ).fetchone()
    if n == 0:
        if not PORTAL.exists():
            raise SystemExit("run code/19/09_sessions.py first: the "
                             "portal's sessions come from its sandbox")
        con.execute(f"ATTACH '{PORTAL}' AS portal (READ_ONLY)")
        con.execute("CREATE OR REPLACE TABLE marts.fct_portal_sessions "
                    "AS SELECT * FROM portal.marts.fct_portal_sessions")
        con.execute("DETACH portal")
    con.close()
    return path


def tidy(*names: str) -> None:
    for name in names:
        p = HERE / name
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        else:
            p.unlink(missing_ok=True)
            p.with_name(p.name + ".wal").unlink(missing_ok=True)
