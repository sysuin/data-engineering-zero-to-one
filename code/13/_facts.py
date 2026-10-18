"""
Chapter 13's sandbox: a copy of the warehouse with everything above
staging dropped, rebuilt by the function behind `python -m
groundwork.transform` (the runner, dim_date, the checks). One build is
shared by the listings that only read it, and made again when a model
or a seed changes.
"""
import hashlib
import shutil

import duckdb

from groundwork.config import SANDBOX, WAREHOUSE
from groundwork.transform import runner
from groundwork.transform.__main__ import transform

HERE = SANDBOX / "ch13"
SHARED = HERE / "shared"


def _stamp() -> str:
    h = hashlib.sha256()
    for path in sorted(runner.MODELS.rglob("*")):
        if path.suffix in (".sql", ".csv"):
            h.update(path.name.encode() + path.read_bytes())
    return h.hexdigest()


def sandbox(name: str):
    """A fresh copy of the warehouse with no intermediate or marts."""
    root = HERE / name
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    shutil.copy(WAREHOUSE, root / "warehouse.duckdb")
    con = duckdb.connect(str(root / "warehouse.duckdb"))
    for schema in ("intermediate", "marts"):
        con.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    con.execute("DELETE FROM audit.model_runs WHERE relation "
                "LIKE 'intermediate.%' OR relation LIKE 'marts.%'")
    con.close()
    return root / "warehouse.duckdb"


def built(read_only: bool = True):
    """The shared sandbox, every mart built; a connection to it."""
    path, stamp = SHARED / "warehouse.duckdb", SHARED / "stamp"
    if not (path.exists() and stamp.exists()
            and stamp.read_text() == _stamp()):
        warehouse = sandbox("shared")
        transform(warehouse)
        stamp.write_text(_stamp())
    return duckdb.connect(str(path), read_only=read_only)


def tidy(name: str) -> None:
    shutil.rmtree(HERE / name, ignore_errors=True)
