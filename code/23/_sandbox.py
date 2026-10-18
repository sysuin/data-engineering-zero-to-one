"""
Chapter 23's sandbox. Nothing here writes the real warehouse or the
real dbt project.

    base()               the warehouse, copied once to
                         data/sandbox/ch23/base/; every build below
                         starts from a copy of it, so every build reads
                         the same raw layer. Its fct_portal_sessions
                         is emptied: at this chapter it is still the
                         empty table Chapter 4 made, and only Chapter
                         26's sessions task fills production's
    project(name, edit)  the dbt project copied to
                         data/sandbox/ch23/<name>/project, with an edit:
                         (file, the text it had, the text it gets)
    built(name, edit)    that project built from scratch, by
                         groundwork.diff.build, into
                         data/sandbox/ch23/<name>/warehouse.duckdb; kept
                         for the next listing, and built again only when
                         the project or the base has changed. dbt's
                         statuses are kept beside it.

Delete data/sandbox/ch23/ freely: the next listing makes it again,
from the warehouse as it then is.
"""
import hashlib
import json
import shutil
from pathlib import Path

import duckdb

from groundwork import diff
from groundwork.config import SANDBOX, WAREHOUSE

HERE = SANDBOX / "ch23"
PROJECT = diff.PROJECT

# The one-line change of the chapter's opening.
CHANGE = ("models/staging/stg_relay__contracts.sql",
          "    CAST(nullif(c.discount_pct, '') AS INTEGER)     "
          "AS discount_pct,",
          "    coalesce(CAST(nullif(c.discount_pct, '') AS INTEGER), 0) "
          "AS discount_pct,")


def base() -> Path:
    path = HERE / "base" / "warehouse.duckdb"
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(WAREHOUSE, path)
        wal = Path(f"{WAREHOUSE}.wal")
        if wal.exists():
            shutil.copy(wal, f"{path}.wal")
        con = duckdb.connect(str(path))
        con.execute("DELETE FROM marts.fct_portal_sessions")
        con.close()
    return path


def project(name: str, edit=None) -> Path:
    into = HERE / name / "project"
    shutil.rmtree(into, ignore_errors=True)
    shutil.copytree(PROJECT, into,
                    ignore=shutil.ignore_patterns("target", "logs"))
    if edit:
        file, old, new = edit
        f = into / file
        text = f.read_text()
        assert text.count(old) == 1, (file, old)
        f.write_text(text.replace(old, new))
    return into


def _stamp(folder: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(folder.rglob("*")):
        if p.is_file():
            h.update(str(p.relative_to(folder)).encode())
            h.update(p.read_bytes())
    st = base().stat()
    h.update(f"{st.st_size}:{st.st_mtime_ns}".encode())
    return h.hexdigest()


def built(name: str, edit=None) -> tuple[Path, dict]:
    """(the built warehouse, dbt's statuses counted)."""
    folder = project(name, edit)
    path = HERE / name / "warehouse.duckdb"
    kept = HERE / name / "built.json"
    stamp = _stamp(folder)
    if path.exists() and kept.exists():
        got = json.loads(kept.read_text())
        if got["stamp"] == stamp:
            return path, got["statuses"]
    statuses = diff.build(folder, path, base())
    kept.write_text(json.dumps({"stamp": stamp, "statuses": statuses}))
    return path, statuses


def tidy(*names: str) -> None:
    for n in names:
        shutil.rmtree(HERE / n, ignore_errors=True)
