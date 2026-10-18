"""Tradeline's order lines with their orders, as one table.

Several of Chapter 5's listings measure formats on the same rows. This
reads them from today's Tradeline (read-only), in order, and writes
one Parquet file in the chapter's sandbox the first time it is asked.
"""
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from groundwork.config import SANDBOX, THREADS, TRADELINE_DB

HERE = SANDBOX / "ch05"
ROWS_PER_GROUP = 100_000


def order_lines() -> pa.Table:
    """Every order line, with its order's columns, in order_id order."""
    con = duckdb.connect(config={"threads": THREADS})
    con.execute(f"ATTACH '{TRADELINE_DB}' AS t "
                "(TYPE sqlite, READ_ONLY)")
    return con.sql("""
        SELECT l.order_id, l.line_no, o.account_id,
               o.placed_at::TIMESTAMP AS placed_at, o.channel,
               o.status, l.sku, l.qty, l.unit_price_cents,
               l.discount_pct
        FROM t.order_lines l JOIN t.orders o ON o.id = l.order_id
        ORDER BY l.order_id, l.line_no""").to_arrow_table()


def lines_parquet() -> Path:
    """The order lines as Parquet: zstd, 100,000 rows a row group."""
    path = HERE / "order_lines.parquet"
    if not path.exists():
        HERE.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        pq.write_table(order_lines(), tmp, compression="zstd",
                       row_group_size=ROWS_PER_GROUP)
        tmp.replace(path)
    return path


def lines_csv() -> Path:
    """The same rows as a plain CSV file, written by DuckDB."""
    path = HERE / "order_lines.csv"
    if not path.exists():
        con = duckdb.connect(config={"threads": THREADS})
        tmp = path.with_suffix(".tmp")
        con.execute(f"COPY (SELECT * FROM '{lines_parquet()}') "
                    f"TO '{tmp}' (FORMAT csv, HEADER)")
        tmp.replace(path)
    return path


def lines_parquet_by_sku() -> Path:
    """The same rows and row groups, sorted by SKU instead of time."""
    path = HERE / "order_lines_by_sku.parquet"
    if not path.exists():
        t = pq.read_table(lines_parquet()).sort_by(
            [("sku", "ascending"), ("order_id", "ascending"),
             ("line_no", "ascending")])
        tmp = path.with_suffix(".tmp")
        pq.write_table(t, tmp, compression="zstd",
                       row_group_size=ROWS_PER_GROUP)
        tmp.replace(path)
    return path
