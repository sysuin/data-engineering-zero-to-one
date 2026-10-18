"""
Chapter 15's sandboxes. Every listing works on a copy of the
warehouse, never on the warehouse itself: `base()` copies it once into
data/sandbox/ch15/base/ (delete that folder to take a fresh copy), and
`work(name)` gives a listing its own copy of the base to change, which
`tidy(name)` deletes. dbt runs here as Chapter 14's helper ran it: in
the project folder, quietly, against a copy named by
GROUNDWORK_DBT_PATH. The file must be called warehouse.duckdb, because
dbt names the database after the file and the staging views say
"warehouse".
"""
import json
import os
import shutil
from contextlib import contextmanager
from pathlib import Path

import duckdb

from groundwork.config import ROOT, SANDBOX, WAREHOUSE

PROJECT = ROOT / "dbt" / "groundwork"
HERE = SANDBOX / "ch15"
os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")


def base() -> Path:
    """The chapter's copy of the warehouse, made the first time."""
    path = HERE / "base" / "warehouse.duckdb"
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(WAREHOUSE, path)
        wal = Path(str(WAREHOUSE) + ".wal")
        if wal.exists():
            shutil.copy(wal, str(path) + ".wal")
    return path


def work(name: str) -> Path:
    """A fresh copy of the base at data/sandbox/ch15/<name>/."""
    root = HERE / name
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    shutil.copy(base(), root / "warehouse.duckdb")
    return root / "warehouse.duckdb"


def tidy(name: str) -> None:
    shutil.rmtree(HERE / name, ignore_errors=True)


def connect(path, read_only=True):
    con = duckdb.connect(str(path), read_only=read_only)
    con.execute("SET enable_progress_bar = false")
    return con


@contextmanager
def _inside(path):
    was = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(was)


def dbt(*args, path, vars=None):
    """Invoke dbt in the project, against the copy at `path`."""
    from dbt.adapters.duckdb.connections import DuckDBConnectionManager
    from dbt.cli.main import dbtRunner
    argv = ["--log-level", "none", *args, "--profiles-dir", "."]
    if vars:
        argv += ["--vars", json.dumps(vars)]
    os.environ["GROUNDWORK_DBT_PATH"] = str(path)
    with _inside(PROJECT):
        res = dbtRunner().invoke(argv)
    DuckDBConnectionManager.close_all_connections()
    if res.exception is not None:
        raise res.exception
    return res


def save(listing: str, data) -> None:
    """Numbers a figure needs, beside the listing's output."""
    out = ROOT / "code" / "15" / f"{listing}.json"
    out.write_text(json.dumps(data, indent=1, default=str) + "\n")
