"""
Chapter 14's helpers: run dbt on Groundwork's project, quietly, into a
sandbox copy of the warehouse, and say what it did in a few lines.

dbt runs in the project folder (profiles.yml is there, and its paths
are relative to it), with its console logging off: a listing prints
what dbt's results say, not dbt's banner, timings or versions. The
dev target's file is chosen with GROUNDWORK_DBT_PATH, so each listing
can have its own copy. One fully built copy is shared by the listings
that only read it, and made again when the project or the warehouse
changes.

Chapter 15 adds a quality suite to the same project: tests and a seed,
every one tagged quality, and the load log declared as a source. These
listings show the project as this chapter leaves it, so every command
that selects nodes leaves the suite out, and chapter_14() tells a
listing reading the manifest which nodes are this chapter's.
"""
import hashlib
import json
import os
import shutil
from collections import Counter
from contextlib import contextmanager
from pathlib import Path

import duckdb
from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.cli.main import dbtRunner

from groundwork.config import ROOT, SANDBOX, WAREHOUSE

PROJECT = ROOT / "dbt" / "groundwork"
HERE = SANDBOX / "ch14"
SHARED = HERE / "built"
os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")

LATER = ("tests/quality/", "tests/_quality.yml", "seeds/finance/")
EXCLUDE = ["tag:quality", *(f"path:{p.rstrip('/')}" for p in LATER)]
SELECTING = {"build", "run", "test", "seed", "snapshot", "ls", "list",
             "compile", "docs"}


@contextmanager
def _inside(path):
    was = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(was)


def dbt(*args, path=None, vars=None, project=PROJECT, check=True):
    """Invoke dbt as the command line would; the result object."""
    argv = ["--log-level", "none", *args, "--profiles-dir", "."]
    if args[0] in SELECTING:
        argv += ["--exclude", *EXCLUDE]
    if vars:
        argv += ["--vars", json.dumps(vars)]
    if path is not None:
        os.environ["GROUNDWORK_DBT_PATH"] = str(path)
    with _inside(project):
        res = dbtRunner().invoke(argv)
    DuckDBConnectionManager.close_all_connections()   # let go of it
    if check and res.exception is not None:
        raise res.exception
    if check and not res.success:
        raise RuntimeError("; ".join(
            f"{r.node.name}: {r.message}" for r in res.result.results
            if str(r.status) in ("error", "fail")))
    return res


def chapter_14(node) -> bool:
    """Whether a manifest node, object or dict, is this chapter's."""
    get = node.get if isinstance(node, dict) else \
        (lambda key: getattr(node, key, None))
    return ("quality" not in (get("tags") or [])
            and not (get("original_file_path") or "").startswith(LATER))


def statuses(res) -> Counter:
    """How many nodes ended in each status: success, pass, warn ..."""
    return Counter(str(r.status) for r in res.result.results)


def copy(name: str, source=WAREHOUSE) -> Path:
    """A fresh copy of a warehouse at data/sandbox/ch14/<name>/."""
    root = HERE / name
    shutil.rmtree(root, ignore_errors=True)
    root.mkdir(parents=True)
    shutil.copy(source, root / "warehouse.duckdb")
    return root / "warehouse.duckdb"


def tidy(name: str) -> None:
    shutil.rmtree(HERE / name, ignore_errors=True)


def _stamp() -> str:
    h = hashlib.sha256()
    for p in sorted(PROJECT.rglob("*")):
        if p.is_file() and not {"target", "logs"} & set(p.parts):
            h.update(str(p.relative_to(PROJECT)).encode())
            h.update(p.read_bytes())
    st = WAREHOUSE.stat()
    h.update(f"{st.st_size}:{st.st_mtime_ns}".encode())
    return h.hexdigest()


NIGHT = {"night": "2026-01-04"}       # the last export in raw


def built() -> Path:
    """
    The shared copy, every seed, model, snapshot and test of this
    chapter's built by `dbt build --full-refresh`. What each node did
    is kept beside it in results.json, and the manifest of that build
    in state/.
    """
    path, stamp = SHARED / "warehouse.duckdb", SHARED / "stamp"
    if not (path.exists() and stamp.exists()
            and stamp.read_text() == _stamp()):
        copy("built")
        res = dbt("build", "--full-refresh", path=path, vars=NIGHT)
        (SHARED / "results.json").write_text(json.dumps(
            [[r.node.resource_type, r.node.schema, r.node.name,
              str(r.status)] for r in res.result.results], indent=0))
        (SHARED / "state").mkdir()
        shutil.copy(PROJECT / "target" / "manifest.json",
                    SHARED / "state" / "manifest.json")
        stamp.write_text(_stamp())
    return path


def results() -> list:
    """What the shared build did: [type, schema, name, status] each."""
    built()
    return json.loads((SHARED / "results.json").read_text())


def connect(path, read_only=True):
    con = duckdb.connect(str(path), read_only=read_only)
    con.execute("SET enable_progress_bar = false")
    return con
