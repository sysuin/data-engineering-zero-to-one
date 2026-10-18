# Deletes leave nothing for a watermark to find: the orders the fixed
# incremental copy still holds after Tradeline removed them.
import json
from datetime import timedelta
from pathlib import Path

from _watermark import Loader, compare, nights

loader = Loader("updated_at", ">=", lookback=timedelta(hours=1))
for night, moment, src in nights():
    loader.run(night, src)
    if night == 30:                     # a full refresh's view tonight
        (ids,) = src.execute("SELECT count(*) FROM orders").fetchone()

wrong, kinds = compare(loader)
gone = [w[0] for w in wrong if w[3] is None]
db = loader.db
rows = db.execute(
    "SELECT id, order_number, account_id, placed_at, count(*), "
    "sum(qty), sum(qty * unit_price_cents) FROM copy_orders o "
    "JOIN copy_order_lines l ON l.order_id = o.id WHERE id IN "
    f"({', '.join(map(str, gone))}) GROUP BY ALL ORDER BY id"
).fetchall()
print(f"in the copy on {moment:%Y-%m-%d}, not in Tradeline: "
      f"{len(gone)} orders")
print(f"{'id':>7} {'number':>7} {'account':>7}  {'placed_at':<19} "
      f"{'lines':>5} {'qty':>5}")
for oid, number, account, placed, n, qty, _ in rows:
    print(f"{oid:>7} {number:>7} {account:>7}  {placed} {n:>5} "
          f"{qty:>5}")
value = sum(r[-1] for r in rows)
print(f"their lines add ${value / 100:,.2f} to the copy's sales")

keys = db.execute(
    "SELECT count(*) FROM (SELECT id FROM copy_orders "
    "EXCEPT SELECT id FROM src.orders)").fetchone()[0]
print(f"a full refresh tonight: {ids:,} orders, none of the "
      f"{len(gone)},")
print("  and nothing to say they were ever there")
print(f"comparing keys: {ids:,} ids read from Tradeline, "
      f"{keys} missing from it")
Path(__file__).with_suffix(".json").write_text(
    json.dumps({"gone": rows}, indent=1) + "\n")
