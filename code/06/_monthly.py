"""What §6.5 and §6.6's listings share: a warehouse of orders and
lines kept by month, one month's batch staged from Tradeline, a
scheduler that retries after a timeout, and sales by month."""
import json
from pathlib import Path

import duckdb
from _first_pipeline import WAREHOUSE, dollars

from groundwork.config import TRADELINE_DB

HISTORY = []                    # the table's state after each attempt


def warehouse():
    """An empty warehouse, with Tradeline attached read-only."""
    WAREHOUSE.unlink(missing_ok=True)
    con = duckdb.connect(str(WAREHOUSE))
    con.execute(f"ATTACH '{TRADELINE_DB}' AS tl "
                "(TYPE sqlite, READ_ONLY)")
    con.execute("CREATE TABLE orders (month VARCHAR, id BIGINT, "
                "placed_at VARCHAR, status VARCHAR)")
    con.execute("CREATE TABLE order_lines (month VARCHAR, "
                "order_id BIGINT, line_no BIGINT, qty BIGINT, "
                "unit_price_cents BIGINT)")
    return con


def stage(con, month):
    """Extract one month from Tradeline into new_orders, new_lines."""
    y, m = map(int, month.split("-"))
    lo, hi = f"{month}-01", f"{y + m // 12}-{m % 12 + 1:02d}-01"
    con.execute("""CREATE OR REPLACE TEMP TABLE new_orders AS
        SELECT ? AS month, id, placed_at, status FROM tl.orders
        WHERE placed_at >= ? AND placed_at < ?""", [month, lo, hi])
    con.execute("""CREATE OR REPLACE TEMP TABLE new_lines AS
        SELECT ? AS month, l.order_id, l.line_no, l.qty,
               l.unit_price_cents
        FROM tl.order_lines l JOIN new_orders o ON o.id = l.order_id
        ORDER BY l.order_id, l.line_no""", [month])


class Timeout(Exception):
    pass


def state(con):
    (rows,) = con.execute("SELECT count(*) FROM order_lines").fetchone()
    return {"rows": rows, "november": sales(con).get("2025-11", 0)}


def scheduler(con, task, month, slow_first=False, tries=2):
    """Run a task; if no reply comes in time, run it again."""
    for attempt in range(1, tries + 1):
        try:
            done = task(con, month)         # this commits ...
            HISTORY.append(state(con))
            if slow_first and attempt == 1:
                raise Timeout("timed out")
            print(f"  {task.__name__} {attempt}: {done}")
            return
        except Timeout as e:                # ... but nobody heard
            print(f"  {task.__name__} {attempt}: {done}; {e}")


def sales(con):
    """Placed orders' sales by month, in cents."""
    return dict(con.execute("""
        SELECT o.month, sum(l.qty * l.unit_price_cents)
        FROM orders o JOIN order_lines l ON l.order_id = o.id
        WHERE o.status = 'placed' GROUP BY 1 ORDER BY 1""").fetchall())


def report(con):
    for month, cents in sales(con).items():
        print(f"  {month}  {dollars(cents):>16}")


def save(con, listing):
    """Keep what the figure of §6.6 draws, beside the listing."""
    rows = con.execute("SELECT order_id, line_no, qty, "
                       "unit_price_cents FROM new_lines LIMIT 3"
                       ).fetchall()
    out = {"attempts": HISTORY[-2:], "sample": rows}
    Path(listing).with_suffix(".json").write_text(
        json.dumps(out, indent=1) + "\n")
