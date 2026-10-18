# The tiny Meridian: written in the real formats, ingested by
# Groundwork's loaders, built by dbt, and every line of the result.
import shutil
import time

import duckdb
from _sandbox import HERE

from groundwork.testing import example

root = HERE / "tiny"
shutil.rmtree(root, ignore_errors=True)
start = time.perf_counter()
s = example(root)
s.write()
files = sorted(p.relative_to(root).as_posix() for p in root.rglob("*")
               if p.is_file())
print(f"written: {len(files)} files, {len(s.orders)} orders in "
      f"{s.tradeline_db.name}")
lines = s.ingest()
print(f"ingest: {len(lines)} tables, "
      f"{sum(x.loaded for x in lines)} rows landed, "
      f"{sum(x.parked for x in lines)} parked; "
      f"{', '.join(sorted({x.status for x in lines}))}")
got = s.transform()
print(f"dbt: {got.get('success', 0)} built, {got.get('pass', 0)} tests "
      f"passed, {got.get('fail', 0) + got.get('error', 0)} failed")
seconds = time.perf_counter() - start

con = duckdb.connect(str(s.warehouse), read_only=True)
rows = con.execute("""
    SELECT f.order_id, f.line_no, f.placed_at, f.order_date,
           a.account_id, f.revenue, f.unit_cost
    FROM marts.fct_order_lines AS f
    JOIN marts.dim_account AS a USING (account_key)
    ORDER BY 1, 2""").fetchall()
print(f"\n{'order':>5} {'line':>4}  {'placed (UTC)':<16} "
      f"{'order day':<10} {'acct':>4} {'revenue':>7} {'unit cost':>10}")
for o, n, at, day, acct, rev, cost in rows:
    print(f"{o:>5} {n:>4}  {at:%Y-%m-%d %H:%M} {day}"
          f" {acct:>4} {rev:>7} {cost:>10}")
(cancelled,) = con.execute(
    "SELECT count(*) FROM marts.fct_orders WHERE order_id = 3"
).fetchone()
contracts = con.execute("""
    SELECT contract_id, count(*), any_value(discount_pct)
    FROM marts.fct_contract_month GROUP BY 1 ORDER BY 1""").fetchall()
con.close()
print(f"cancelled order 3 in the facts: {cancelled}")
for c, months, discount in contracts:
    print(f"contract {c}: in force on {months} month ends, "
          f"discount {discount}")
print(f"\nsources to marts in under two minutes: {seconds < 120}")
shutil.rmtree(root)
