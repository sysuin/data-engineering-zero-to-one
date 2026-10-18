"""
Chapter 12's sandbox: a copy of the warehouse, its Relay raw tables
beside it, the old CRM's last night restored from IT's archive, and
everything above staging dropped, so that a listing builds what it
shows. built() also runs the runner and fills dim_date.
"""
import shutil
from pathlib import Path

import duckdb

from groundwork import dimensions, identity
from groundwork.config import RAW, SANDBOX, WAREHOUSE
from groundwork.storage import raw_views

HERE = SANDBOX / "ch12"
SNAPSHOTS = HERE / "snapshots" / "snapshots.duckdb"   # listing 06's


def sandbox(name: str) -> Path:
    """A warehouse for one listing, made afresh; returns its path."""
    root = HERE / name
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    shutil.copytree(RAW / "relay", root / "raw" / "relay")
    shutil.copy(WAREHOUSE, root / "warehouse.duckdb")
    con = duckdb.connect(str(root / "warehouse.duckdb"))
    for schema in ("intermediate", "marts"):
        con.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    con.execute("DELETE FROM audit.model_runs WHERE relation "
                "LIKE 'intermediate.%' OR relation LIKE 'marts.%'")
    raw_views(con, root / "raw")
    con.close()
    identity.restore(warehouse=root / "warehouse.duckdb",
                     root=root / "raw", archive=root / "landed")
    return root / "warehouse.duckdb"


def built(name: str):
    """A sandbox with every dimension built; returns a connection."""
    warehouse = sandbox(name)
    dimensions.build(warehouse)
    return duckdb.connect(str(warehouse))


def tidy(name: str) -> None:
    shutil.rmtree(HERE / name, ignore_errors=True)
