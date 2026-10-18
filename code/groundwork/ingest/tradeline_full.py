"""
Tradeline, copied whole: every order and every order line, each night.

A full refresh reads both tables from the orders database in one read
transaction, so the two agree with each other, and writes each as one
raw partition for the load date:

    raw/tradeline/orders/load_date=YYYY-MM-DD/part-0.parquet
    raw/tradeline/order_lines/load_date=YYYY-MM-DD/part-0.parquet

Each table's load is a row in audit.load_log. Before anything is
written, the rows read are checked against a count the source gives
in the same transaction; after, the rows written are counted from
the file's own footer. A mismatch fails the load. Running the same
load date again replaces its partitions whole, so a rerun is safe.

Every partition is a complete copy of the table on that night. The
view raw.tradeline_orders reads all of them, so a query over it must
pick one load date (usually the latest good one: see loadlog.last).

    load(load_date, run_at=...)       both tables; returns their Loads
    python -m groundwork.ingest.tradeline_full [--load-date D] [--at T]
"""
from __future__ import annotations

import argparse
import sqlite3
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from groundwork import loadlog
from groundwork.config import NOW, TRADELINE_DB, WAREHOUSE
from groundwork.storage import raw_views, write_partition

SOURCE = "tradeline"
QUERIES = {
    "orders": "SELECT * FROM orders ORDER BY id",
    "order_lines": "SELECT * FROM order_lines "
                   "ORDER BY order_id, line_no",
}
TYPES = {"INTEGER": pa.int64(), "TEXT": pa.string()}
BATCH = 100_000


class LoadError(Exception):
    """A load whose counts do not reconcile."""


def _schema(src: sqlite3.Connection, table: str) -> pa.Schema:
    """The table's columns, typed as Tradeline declares them."""
    cols = src.execute(f"PRAGMA table_info({table})").fetchall()
    return pa.schema([(name, TYPES[kind.upper()])
                      for _, name, kind, *_ in cols])


def extract(src: sqlite3.Connection, table: str) -> pa.Table:
    """Every row of one Tradeline table, as the source typed it."""
    schema = _schema(src, table)
    cur = src.execute(QUERIES[table])
    batches = []
    while rows := cur.fetchmany(BATCH):
        columns = zip(*rows)
        batches.append(pa.record_batch(
            [pa.array(c, f.type) for c, f in zip(columns, schema)],
            schema=schema))
    return pa.Table.from_batches(batches, schema=schema)


def load_table(src, con, table, load_date, run_at, now, root=None):
    """Load one table: log it, read it, check it, write it, check it."""
    load_id = loadlog.start(con, SOURCE, table, load_date,
                            started_at=now())
    counts = {}
    try:
        (counts["source_count"],) = src.execute(
            f"SELECT count(*) FROM {table}").fetchone()
        data = extract(src, table)
        counts["rows_read"] = data.num_rows
        if data.num_rows != counts["source_count"]:
            raise LoadError(f"read {data.num_rows:,} rows, the source "
                            f"holds {counts['source_count']:,}")
        path = write_partition(data, SOURCE, table, load_date,
                               loaded_at=run_at, load_id=load_id,
                               source_query=QUERIES[table], root=root)
        counts["rows_written"] = pq.ParquetFile(path).metadata.num_rows
        if counts["rows_written"] != counts["rows_read"]:
            raise LoadError(f"wrote {counts['rows_written']:,} rows, "
                            f"read {counts['rows_read']:,}")
    except Exception as e:
        loadlog.fail(con, load_id, finished_at=now(),
                     error=f"{type(e).__name__}: {e}", **counts)
        raise
    loadlog.finish(con, load_id, finished_at=now(),
                   extra={"bytes": path.stat().st_size}, **counts)
    return load_id


def load(load_date, *, run_at: datetime, source=TRADELINE_DB,
         root: Path | None = None,
         warehouse: Path = WAREHOUSE) -> list[loadlog.Load]:
    """
    Copy orders and order_lines for one load date, logging each.

    `run_at` is the run's moment: every row's _loaded_at, and where
    the log's clock starts. `source` is the Tradeline database file,
    opened read-only. Raises if either table fails; the load log
    records which, and why.
    """
    clock = time.perf_counter()

    def now():                        # the run's moment, plus elapsed
        return run_at + timedelta(seconds=time.perf_counter() - clock)

    Path(warehouse).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse))
    src = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    try:
        src.execute("BEGIN")          # one snapshot for both tables
        ids = [load_table(src, con, t, load_date, run_at, now, root)
               for t in QUERIES]
        raw_views(con, root)
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
    a = p.parse_args(argv)
    run_at = datetime.fromisoformat(a.at)
    day = a.load_date or (run_at.date() - timedelta(days=1))
    for x in load(day, run_at=run_at):
        print(f"{x.load_id:<34} {x.status:<9} "
              f"{x.rows_written:>10,} rows, source "
              f"{x.source_count:>10,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
