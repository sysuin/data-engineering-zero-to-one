"""The smallest pipeline that does real work: copy Tradeline's orders
and lines into a DuckDB file, then add up November's sales there."""
import sqlite3

import pyarrow as pa

from groundwork.config import SANDBOX, TRADELINE_DB

WAREHOUSE = SANDBOX / "ch06" / "first.duckdb"
QUERIES = {
    "orders": "SELECT id, placed_at, status FROM orders",
    "order_lines": "SELECT order_id, line_no, qty, unit_price_cents "
                   "FROM order_lines",
}


def extract(source=TRADELINE_DB):
    """Both tables, read-only, in one read transaction."""
    src = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    src.execute("BEGIN")
    tables = {}
    for name, query in QUERIES.items():
        cur = src.execute(query)
        names = [d[0] for d in cur.description]
        columns = [pa.array(c) for c in zip(*cur.fetchall())]
        tables[name] = pa.Table.from_arrays(columns, names=names)
    src.close()
    return tables


def load(tables, con):
    """Add each table's rows to a warehouse table of the same name."""
    for name, rows in tables.items():
        con.execute(f"CREATE TABLE IF NOT EXISTS {name} AS "
                    "FROM rows LIMIT 0")
        con.execute(f"INSERT INTO {name} FROM rows")


def transform(con):
    """November's sales: placed orders, by the UTC month, in cents."""
    return con.execute("""
        SELECT count(DISTINCT o.id), count(*),
               sum(l.qty * l.unit_price_cents)
        FROM orders o JOIN order_lines l ON l.order_id = o.id
        WHERE o.status = 'placed'
          AND o.placed_at >= '2025-11-01'
          AND o.placed_at <  '2025-12-01'""").fetchone()


def dollars(cents: int) -> str:
    """Integer cents as dollars, without passing through a float."""
    sign = "-" if cents < 0 else ""
    return f"{sign}${abs(cents) // 100:,}.{abs(cents) % 100:02d}"


def run(con):
    """Extract, load, transform: one run of the pipeline."""
    load(extract(), con)
    return transform(con)
