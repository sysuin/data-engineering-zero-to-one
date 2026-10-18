"""
Tradeline, incrementally: each night, only what changed since the last.

Two modes share one raw layout. Both begin with a snapshot, a whole
copy of the table taken in the same read transaction as the bookmark
the next night starts from, and both write one raw partition per load
date through write_partition, so a rerun replaces it whole.

  cdc          the default. Reads the replication log's entries after
               the last log position (lsn), inserts, updates and
               deletes alike, into raw/tradeline/<table>_log/. If the
               log no longer reaches back to that position (the
               loader was away longer than the log is kept), the load
               takes a new snapshot and hands over from it.
  incremental  for a source with no log. Reads rows whose updated_at
               is at or after the last watermark less a lookback
               window, so a row that commits minutes after its
               timestamp is read on a later night, into
               raw/tradeline/<table>/. Deletes are invisible to it.

The bookmark (a position or a watermark) is kept in the load log's
`extra`. A load for date D starts from the last good load *before* D,
so a rerun of D reads the same changes again, and more, and replaces
D's partition: it can never skip what the first attempt read.

Raw keeps what each night read. The table as it stands is the view
raw.tradeline_<table>_current: the last snapshot with every later
change applied, one row per key, deleted rows gone. Every load
reconciles that view with the source, counts and sums, in the same
read transaction, and writes the result to `extra["check"]`.

    load(load_date, run_at=..., mode="cdc")        both tables
    python -m groundwork.ingest.tradeline [--load-date D] [--at T]
                                          [--mode cdc|incremental]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from groundwork import loadlog
from groundwork.config import NOW, TRADELINE_DB, WAREHOUSE
from groundwork.storage import partitions, raw_views, write_partition

SOURCE = "tradeline"
KEYS = {"orders": ("id",), "order_lines": ("order_id", "line_no")}
MEASURES = {                       # what "the copy agrees" means
    "orders": ("count(*)", "sum(CASE WHEN status = 'cancelled' "
                           "THEN 1 ELSE 0 END)"),
    "order_lines": ("count(*)", "sum(qty * unit_price_cents)"),
}
MODES = ("cdc", "incremental")
LOOKBACK = timedelta(hours=1)
STAMP = "%Y-%m-%d %H:%M:%S"
TYPES = {"INTEGER": pa.int64(), "TEXT": pa.string()}
LOG = [("lsn", pa.int64()), ("committed_at", pa.string()),
       ("op", pa.string())]
BATCH = 100_000


class LoadError(Exception):
    """A load whose counts do not reconcile, or that cannot start."""


@dataclass
class Plan:
    """What one table's load will read, and where it starts from."""
    mode: str
    snapshot: bool
    target: str                  # the raw table the rows land in
    sql: str
    params: tuple
    count_sql: str               # the same rows, counted separately
    bookmark: object             # after this load: position or stamp
    base: tuple                  # (snapshot's load date, its position)
    since: object = None         # where this load started reading


def schema(src: sqlite3.Connection, table: str) -> pa.Schema:
    """The table's columns, typed as Tradeline declares them."""
    cols = src.execute(f"PRAGMA table_info({table})").fetchall()
    return pa.schema([(name, TYPES[kind.upper()])
                      for _, name, kind, *_ in cols])


def previous(con, table: str, load_date: date,
             mode: str) -> loadlog.Load | None:
    """The last good load of this table in this mode, before the day."""
    loads = [x for x in loadlog.history(con, SOURCE, table)
             if x.status == "succeeded" and x.load_date < load_date
             and x.extra.get("mode") == mode]
    if not loads:
        return None
    return max(loads, key=lambda x: (x.load_date,
                                     int(x.load_id.rsplit(".", 1)[1])))


def plan(src, con, table, load_date, mode, lookback) -> Plan:
    """Decide tonight's read from the log of what earlier nights did."""
    keys = ", ".join(KEYS[table])
    before = previous(con, table, load_date, mode)
    whole = (f"SELECT * FROM {table} ORDER BY {keys}",
             f"SELECT count(*) FROM {table}")
    if mode == "incremental":
        (mark,) = src.execute(          # the newest row we can see
            f"SELECT max(updated_at) FROM {table}").fetchone()
        if before is None or "watermark" not in before.extra:
            return Plan(mode, True, table, whole[0], (), whole[1],
                        mark, (load_date, None))
        last = before.extra["watermark"]
        mark = max(mark, last)
        since = (datetime.strptime(last, STAMP) - lookback
                 ).strftime(STAMP)
        where = "WHERE updated_at >= ?"
        return Plan(mode, False, table,
                    f"SELECT * FROM {table} {where} ORDER BY {keys}",
                    (since,), f"SELECT count(*) FROM {table} {where}",
                    mark, tuple(before.extra["base"]), since)
    lo, hi = src.execute("SELECT min(lsn), max(lsn) "
                         "FROM replication_log").fetchone()
    if hi is None:
        raise LoadError("the replication log is empty: there is no "
                        "position to start a snapshot from")
    at = None if before is None else before.extra.get("position")
    if at is None or lo > at + 1:           # never started, or a gap
        return Plan(mode, True, table, whole[0], (), whole[1], hi,
                    (load_date, hi), at)
    where = "WHERE table_name = ? AND lsn > ? AND lsn <= ?"
    return Plan(mode, False, f"{table}_log",
                "SELECT lsn, committed_at, op, row_json FROM "
                f"replication_log {where} ORDER BY lsn",
                (table, at, hi),
                f"SELECT count(*) FROM replication_log {where}",
                hi, tuple(before.extra["base"]), at)


def read(src, p: Plan, cols: pa.Schema) -> pa.Table:
    """Run the plan's query in batches, typed as the source types it."""
    logged = p.target.endswith("_log")
    out = pa.schema(LOG + list(zip(cols.names, cols.types))
                    ) if logged else cols
    cur = src.execute(p.sql, p.params)
    batches = []
    while rows := cur.fetchmany(BATCH):
        if logged:                  # a log entry carries its row as JSON
            rows = [(lsn, at, op, *map(json.loads(js).get, cols.names))
                    for lsn, at, op, js in rows]
        batches.append(pa.record_batch(
            [pa.array(c, f.type) for c, f in zip(zip(*rows), out)],
            schema=out))
    return pa.Table.from_batches(batches, schema=out)


def current(con, table: str, base: tuple, names: list[str],
            root=None) -> str:
    """
    (Re)create raw.tradeline_<table>_current: the latest version of
    every row, from the last snapshot and the changes loaded since.
    """
    day, at = date.fromisoformat(str(base[0])), base[1]
    keys = ", ".join(KEYS[table])
    cols = ", ".join(names)
    view = f"raw.{SOURCE}_{table}_current"
    rows = (f"SELECT {cols}, _load_id, _loaded_at FROM "
            f"raw.{SOURCE}_{table} WHERE load_date >= DATE '{day}'")
    if at is None:                          # incremental: latest read
        sql = (f"{rows} QUALIFY row_number() OVER (PARTITION BY "
               f"{keys} ORDER BY _loaded_at DESC, load_date DESC) = 1")
    else:                                   # cdc: snapshot, then log
        sql = (f"SELECT {cols}, _load_id, _loaded_at, {at} AS lsn, "
               f"'I' AS op FROM raw.{SOURCE}_{table} "
               f"WHERE load_date = DATE '{day}'")
        if partitions(SOURCE, f"{table}_log", root):
            sql += (f" UNION ALL SELECT {cols}, _load_id, _loaded_at, "
                    f"lsn, op FROM raw.{SOURCE}_{table}_log "
                    f"WHERE load_date >= DATE '{day}' AND lsn > {at}")
        sql = (f"SELECT {cols}, _load_id, _loaded_at FROM ({sql}) "
               f"QUALIFY row_number() OVER (PARTITION BY {keys} "
               "ORDER BY lsn DESC) = 1 AND op <> 'D'")
    con.execute(f"CREATE OR REPLACE VIEW {view} AS {sql}")
    return view


def reconcile(src, con, table: str) -> dict:
    """Counts and sums of the copy against the source's own."""
    sql = f"SELECT {', '.join(MEASURES[table])} FROM {{}}"
    theirs = list(src.execute(sql.format(table)).fetchone())
    ours = list(con.execute(
        sql.format(f"raw.{SOURCE}_{table}_current")).fetchone())
    ours = [None if v is None else int(v) for v in ours]
    return {"measures": list(MEASURES[table]), "source": theirs,
            "copy": ours, "agrees": theirs == ours}


def load_table(src, con, table, load_date, *, mode, lookback,
               run_at, now, root=None) -> str:
    """Load one table: plan, read, check, write, apply, reconcile."""
    load_id = loadlog.start(con, SOURCE, table, load_date,
                            started_at=now())
    counts = {}
    try:
        p = plan(src, con, table, load_date, mode, lookback)
        (counts["source_count"],) = src.execute(
            p.count_sql, p.params).fetchone()
        cols = schema(src, table)
        data = read(src, p, cols)
        counts["rows_read"] = data.num_rows
        if data.num_rows != counts["source_count"]:
            raise LoadError(f"read {data.num_rows:,} rows, the source "
                            f"counts {counts['source_count']:,}")
        path = write_partition(data, SOURCE, p.target, load_date,
                               loaded_at=run_at, load_id=load_id,
                               source_query=_text(p), root=root)
        counts["rows_written"] = pq.ParquetFile(path).metadata.num_rows
        if counts["rows_written"] != counts["rows_read"]:
            raise LoadError(f"wrote {counts['rows_written']:,} rows, "
                            f"read {counts['rows_read']:,}")
        raw_views(con, root)
        current(con, table, p.base, cols.names, root)
        check = reconcile(src, con, table)
    except Exception as e:
        loadlog.fail(con, load_id, finished_at=now(),
                     error=f"{type(e).__name__}: {e}", **counts)
        raise
    mark = "position" if mode == "cdc" else "watermark"
    loadlog.finish(con, load_id, finished_at=now(), **counts, extra={
        "mode": mode, "snapshot": p.snapshot, mark: p.bookmark,
        "since": p.since, "base": [str(p.base[0]), p.base[1]],
        "check": check, "bytes": path.stat().st_size})
    return load_id


def _text(p: Plan) -> str:
    """The query as it ran, with its parameters written in."""
    sql = p.sql
    for v in p.params:
        sql = sql.replace("?", repr(v), 1)
    return sql


def load(load_date, *, run_at: datetime, mode: str = "cdc",
         lookback: timedelta = LOOKBACK, source=TRADELINE_DB,
         root: Path | None = None,
         warehouse: Path = WAREHOUSE) -> list[loadlog.Load]:
    """
    Load what changed in orders and order_lines, logging each table.

    `run_at` is the run's moment. `mode` is "cdc" (from the log) or
    "incremental" (by updated_at, with `lookback`). Both tables are
    read in one transaction, so they and the log agree. Returns the
    two Loads; each one's extra["check"] says whether the copy now
    agrees with the source.
    """
    if mode not in MODES:
        raise ValueError(f"mode must be one of {MODES}")
    day = date.fromisoformat(str(load_date))
    clock = time.perf_counter()

    def now():                        # the run's moment, plus elapsed
        return run_at + timedelta(seconds=time.perf_counter() - clock)

    Path(warehouse).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse))
    src = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    try:
        src.execute("BEGIN")          # one snapshot: tables and log
        ids = [load_table(src, con, t, day, mode=mode,
                          lookback=lookback, run_at=run_at, now=now,
                          root=root) for t in KEYS]
        return [x for x in loadlog.history(con, SOURCE)
                if x.load_id in ids]
    finally:
        src.close()
        con.close()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--at", default=NOW.strftime("%Y-%m-%d %H:%M"),
                   help="the run's moment, UTC (default: NOW)")
    p.add_argument("--load-date", type=date.fromisoformat,
                   help="the logical date (default: the day before)")
    p.add_argument("--mode", choices=MODES, default="cdc")
    a = p.parse_args(argv)
    run_at = datetime.fromisoformat(a.at)
    day = a.load_date or (run_at.date() - timedelta(days=1))
    for x in load(day, run_at=run_at, mode=a.mode):
        kind = "snapshot" if x.extra["snapshot"] else x.extra["mode"]
        agrees = "agrees" if x.extra["check"]["agrees"] else "DIFFERS"
        print(f"{x.load_id:<34} {kind:<11} {x.rows_written:>9,} rows;"
              f" copy {agrees}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
