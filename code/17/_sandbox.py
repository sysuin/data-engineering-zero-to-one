"""
Chapter 17's sandbox. The orchestrator writes raw, the load log, the
marts and its own history, so listings that run real tasks work on a
copy of Groundwork, never on data/groundwork.

    base/       built once: a copy of the warehouse, of raw and of the
                landed archive, with the raw views pointed at the copy
    work/NAME/  a fresh copy of base for one listing, deleted after

Not printed in the book.
"""
import shutil
from pathlib import Path

import duckdb

from groundwork import storage
from groundwork.config import GROUNDWORK, SANDBOX, WAREHOUSE
from groundwork.orchestrate import runner

HERE = SANDBOX / "ch17"
BASE = HERE / "base"


def base() -> Path:
    """The shared copy, made the first time it is asked for."""
    if not (BASE / "warehouse.duckdb").exists():
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
    storage.raw_views(con, (BASE / "raw").resolve())
    con.close()
    return BASE


def work(name: str) -> Path:
    """A fresh copy of base at work/NAME."""
    src, dst = base(), HERE / "work" / name
    shutil.rmtree(dst, ignore_errors=True)
    shutil.copytree(src, dst)
    return dst


def env(copy: Path, **kw) -> runner.Env:
    """Where a run on this copy reads and writes; dbt builds the copy
    (the dev target) and keeps its target/ folder inside it."""
    return runner.Env(warehouse=copy / "warehouse.duckdb",
                      root=copy / "raw", archive=copy / "landed",
                      dbt_target="dev", dbt_target_path=copy / "dbt",
                      **kw)


def tidy(name: str) -> None:
    shutil.rmtree(HERE / "work" / name, ignore_errors=True)
