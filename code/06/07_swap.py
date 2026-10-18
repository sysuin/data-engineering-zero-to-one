# Build the new copy beside the live table, check it against the
# source, and only then swap the names. Killed half way, the build
# leaves the live table as it was.
import json
from pathlib import Path

import duckdb
from _first_pipeline import WAREHOUSE, dollars

from groundwork.config import TRADELINE_DB

BATCH = 250_000
LINES = """INSERT INTO order_lines_new SELECT order_id, line_no, qty,
               unit_price_cents FROM tl.order_lines
           ORDER BY order_id, line_no LIMIT ? OFFSET ?"""


class Killed(Exception):
    pass


def look(con, table="order_lines"):
    reader = con.cursor()                  # another connection
    return reader.execute(f"""
        SELECT (SELECT count(*) FROM {table}),
               coalesce(sum(l.qty * l.unit_price_cents), 0)
        FROM orders o JOIN {table} l ON l.order_id = o.id
        WHERE o.status = 'placed' AND o.placed_at >= '2025-11-01'
          AND o.placed_at < '2025-12-01'""").fetchone()


def build(con, seen, die_after=None):
    """Fill order_lines_new from the source; the live table waits."""
    con.execute("DROP TABLE IF EXISTS order_lines_new")
    con.execute("CREATE TABLE order_lines_new AS "
                "FROM order_lines LIMIT 0")
    (total,) = con.execute("SELECT count(*) FROM tl.order_lines"
                           ).fetchone()
    for i in range(-(-total // BATCH)):
        if i == die_after:
            raise Killed
        con.execute(LINES, [BATCH, i * BATCH])
        seen.append((f"batch {i + 1}", *look(con)))
    (built,) = con.execute("SELECT count(*) FROM order_lines_new"
                           ).fetchone()
    if built != total:
        raise ValueError(f"built {built:,} rows, source has {total:,}")
    return built


def swap(con):
    """Put the new table in the old one's place, all at once."""
    con.execute("BEGIN")
    con.execute("ALTER TABLE order_lines RENAME TO order_lines_old")
    con.execute("ALTER TABLE order_lines_new RENAME TO order_lines")
    con.execute("DROP TABLE order_lines_old")
    con.execute("COMMIT")


WAREHOUSE.unlink(missing_ok=True)
con = duckdb.connect(str(WAREHOUSE))
con.execute(f"ATTACH '{TRADELINE_DB}' AS tl (TYPE sqlite, READ_ONLY)")
con.execute("CREATE TABLE orders AS SELECT id, placed_at, status "
            "FROM tl.orders")
con.execute("CREATE TABLE order_lines AS SELECT order_id, line_no, "
            "qty, unit_price_cents FROM tl.order_lines")

saved = {}
for how, die_after in (("killed", 2), ("finished", None)):
    seen = [("before", *look(con))]
    try:
        built = build(con, seen, die_after)
        swap(con)
        note = f"checked {built:,} rows against the source; swapped"
    except Killed:
        rows, _ = look(con, "order_lines_new")
        note = f"killed; order_lines_new left with {rows:,} rows"
    seen.append(("after", *look(con)))
    saved[how] = seen
    print(f"{how}: {note}")
    for step, rows, cents in seen:
        print(f"  {step:<9} {rows:>10,} lines   November "
              f"{dollars(cents):>15}")

Path(__file__).with_suffix(".json").write_text(
    json.dumps(saved, indent=1) + "\n")
