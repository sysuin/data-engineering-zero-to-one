# Run the smallest pipeline once, and check what it loaded against
# what Tradeline holds.
import json
from pathlib import Path

import duckdb
from _first_pipeline import WAREHOUSE, dollars, run

from meridian.sources import tradeline

WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
WAREHOUSE.unlink(missing_ok=True)          # start from nothing
con = duckdb.connect(str(WAREHOUSE))

orders, lines, cents = run(con)

src = tradeline.connect()
counts = {}
print("table         in Tradeline  in warehouse")
for table in ("orders", "order_lines"):
    (have,) = src.execute(f"SELECT count(*) FROM {table}").fetchone()
    (got,) = con.execute(f"SELECT count(*) FROM {table}").fetchone()
    print(f"{table:<12} {have:>13,} {got:>13,}")
    counts[table] = got
print(f"November: {orders:,} orders, {lines:,} lines, "
      f"{dollars(cents)}")
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {**counts, "orders_nov": orders, "cents_nov": cents}) + "\n")
