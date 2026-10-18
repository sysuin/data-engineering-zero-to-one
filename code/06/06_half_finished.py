# A full refresh that empties the table and refills it in batches is
# killed half way: once as a series of statements, once as one
# transaction. A reader looks in while it runs.
import json
from pathlib import Path

import duckdb
from _first_pipeline import WAREHOUSE, dollars

from groundwork.config import TRADELINE_DB

BATCH = 250_000
LINES = """INSERT INTO order_lines SELECT order_id, line_no, qty,
               unit_price_cents FROM tl.order_lines
           ORDER BY order_id, line_no LIMIT ? OFFSET ?"""


class Killed(Exception):
    pass


def look(con):
    """What a reader on another connection sees right now."""
    reader = con.cursor()
    (rows,) = reader.execute("SELECT count(*) FROM order_lines"
                             ).fetchone()
    (cents,) = reader.execute("""
        SELECT coalesce(sum(l.qty * l.unit_price_cents), 0)
        FROM orders o JOIN order_lines l ON l.order_id = o.id
        WHERE o.status = 'placed' AND o.placed_at >= '2025-11-01'
          AND o.placed_at < '2025-12-01'""").fetchone()
    return rows, cents


def refresh(con, die_after):
    """Empty order_lines, then refill it BATCH rows at a time."""
    seen = [("before", *look(con))]
    con.execute("DELETE FROM order_lines")
    seen.append(("emptied", *look(con)))
    for i in range(die_after):
        con.execute(LINES, [BATCH, i * BATCH])
        seen.append((f"batch {i + 1}", *look(con)))
    raise Killed(seen)


WAREHOUSE.unlink(missing_ok=True)
con = duckdb.connect(str(WAREHOUSE))
con.execute(f"ATTACH '{TRADELINE_DB}' AS tl (TYPE sqlite, READ_ONLY)")
con.execute("CREATE TABLE orders AS SELECT id, placed_at, status "
            "FROM tl.orders")

saved = {}
for how in ("statements", "transaction"):
    con.execute("CREATE OR REPLACE TABLE order_lines AS SELECT "
                "order_id, line_no, qty, unit_price_cents "
                "FROM tl.order_lines")        # whole, to start with
    if how == "transaction":
        con.execute("BEGIN")
    try:
        refresh(con, die_after=2)
    except Killed as e:
        seen = e.args[0]
        if how == "transaction":
            con.execute("ROLLBACK")
    seen.append(("after", *look(con)))
    saved[how] = seen
    print(f"as {how}: a reader saw")
    for step, rows, cents in seen:
        print(f"  {step:<9} {rows:>10,} lines   November "
              f"{dollars(cents):>15}")

Path(__file__).with_suffix(".json").write_text(
    json.dumps(saved, indent=1) + "\n")
