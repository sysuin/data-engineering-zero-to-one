"""
Chapter 24's sandbox. Nothing here touches the real warehouse or the
real raw layer: the listings delete and rewrite personal data, so they
work on copies.

    base/       built once: a copy of the warehouse, of raw and of the
                landed archive, plus the portal's hourly files landed
                as raw.portal_events (one partition per day), with the
                raw views pointed at the copy
    work/NAME/  a fresh copy of base for a listing that changes things,
                deleted when it is done

The pseudonym key comes from .env, copied from .env.example, whose demo
value is set here too when the environment has none, as Chapter 8's
demo token is. The book's pseudonyms are made with it. A real key is
never that one: it lives in a secrets manager, never in a file git can
see.
"""
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

from groundwork import storage
from groundwork.config import GROUNDWORK, LANDING, RAW, SANDBOX, WAREHOUSE

os.environ.setdefault("GROUNDWORK_PSEUDONYM_KEY",
                      "change-me-to-a-long-random-string")

HERE = SANDBOX / "ch24"
BASE = HERE / "base"
KEEP = ("raw", "staging", "intermediate", "marts", "seeds", "audit")


class Copy:
    """One copy of Groundwork: its warehouse, raw and landed folders."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.warehouse = self.root / "warehouse.duckdb"
        self.raw = self.root / "raw"
        self.landed = self.root / "landed"

    def connect(self, read_only: bool = False):
        con = duckdb.connect(str(self.warehouse), read_only=read_only)
        con.execute("SET enable_progress_bar = false")
        return con


def _land_portal(raw: Path) -> None:
    """The portal's hourly files as raw.portal_events, as sent."""
    con = duckdb.connect()
    con.execute("SET enable_progress_bar = false")
    files = (LANDING / "portal" / "*" / "*" / "*" / "*.jsonl.gz").as_posix()
    con.execute(f"""
        CREATE TABLE ev AS
        SELECT event_id, event_type, schema_version, occurred_at,
               session_id, account_id, user_id, device, user_agent,
               CAST(properties AS VARCHAR) AS properties, received_at,
               regexp_extract(filename, 'portal/.*$') AS file
        FROM read_json('{files}', filename = true, format =
            'newline_delimited', columns = {{
              event_id: 'VARCHAR', event_type: 'VARCHAR',
              schema_version: 'INTEGER', occurred_at: 'VARCHAR',
              session_id: 'VARCHAR', account_id: 'INTEGER',
              user_id: 'VARCHAR', device: 'VARCHAR',
              user_agent: 'VARCHAR', properties: 'JSON',
              received_at: 'VARCHAR'}})""")
    days = [d for (d,) in con.execute(
        "SELECT DISTINCT CAST(strptime(regexp_extract(file, "
        "'portal/(\\d+/\\d+/\\d+)', 1), '%Y/%m/%d') AS DATE) "
        "FROM ev ORDER BY 1").fetchall()]
    for day in days:
        stamp = day.strftime("%Y/%m/%d")
        rel = con.sql(f"""
            SELECT * EXCLUDE (file) FROM ev
            WHERE file LIKE 'portal/{stamp}/%'
            ORDER BY file, received_at, event_id""")
        storage.write_partition(
            rel, "portal", "events", day,
            loaded_at=datetime.combine(day + timedelta(days=1),
                                       datetime.min.time()).replace(hour=6),
            load_id=f"portal.events.{day}.1",
            source_file=f"portal/{stamp}", root=raw)
    con.close()


def _repoint(copy: Copy) -> None:
    con = copy.connect()
    storage.raw_views(con, root=copy.raw)
    con.close()


def base() -> Copy:
    """The pristine copy every listing starts from (built once)."""
    copy = Copy(BASE)
    if (BASE / "ready").exists():
        return copy
    shutil.rmtree(BASE, ignore_errors=True)
    BASE.mkdir(parents=True)
    shutil.copy(WAREHOUSE, copy.warehouse)
    shutil.copytree(RAW, copy.raw)
    shutil.copytree(GROUNDWORK / "landed", copy.landed)
    con = copy.connect()
    for (schema,) in con.execute(
            "SELECT schema_name FROM duckdb_schemas() WHERE NOT internal"
            " AND database_name = current_database()").fetchall():
        if schema not in KEEP + ("main",):
            con.execute(f"DROP SCHEMA {schema} CASCADE")
    con.close()
    _land_portal(copy.raw)
    _repoint(copy)
    (BASE / "ready").write_text("ok\n")
    return copy


def work(name: str) -> Copy:
    """A fresh copy of base, for a listing that changes things."""
    src = base()
    copy = Copy(HERE / "work" / name)
    shutil.rmtree(copy.root, ignore_errors=True)
    shutil.copytree(src.root, copy.root)
    (copy.root / "ready").unlink(missing_ok=True)
    _repoint(copy)
    return copy


def tidy(name: str) -> None:
    shutil.rmtree(HERE / "work" / name, ignore_errors=True)
