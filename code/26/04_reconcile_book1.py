# Book 1's twelve quarters of key-account revenue, cost and orders,
# rebuilt from Groundwork; then Book 1's own tables, row by row.
import json
from pathlib import Path

from _sandbox import connect

from groundwork import reconcile_books as rb

con = connect()
quarters = rb.quarters(con)
tables = rb.book1(con)
con.close()

print("Book 1's quarters (its manifest) and Groundwork's, key accounts")
print(f"{'quarter':<9}{'revenue':>15}{'cost':>15}{'orders':>8}"
      f"{'units':>9}  same")
for q in quarters:
    g = q.ours
    same = "yes" if not q.differing else ", ".join(q.differing)
    print(f"{q.quarter:<9}{g['revenue']:>15,.2f}{g['cost']:>15,.2f}"
          f"{g['orders']:>8,}{g['units']:>9,}  {same}")
rev = sum(q.ours["revenue"] for q in quarters)
print(f"{len(quarters)} quarters, {round(rev, 2):,.2f} of revenue; "
      f"{sum(bool(q.differing) for q in quarters)} differ")

print("\nBook 1's tables, as the slice of compat they are")
print(f"{'table':<16}{'Book 1':>12}{'Groundwork':>12}{'differing':>11}")
for x in tables:
    print(f"{x.item:<16}{x.theirs:>12,}{x.ours:>12,}{x.differing:>11,}")

Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"quarters": [[q.quarter, q.book1["revenue"], q.ours["revenue"],
                   len(q.differing)] for q in quarters],
     "tables": [[x.item, x.theirs, x.ours, x.differing]
                for x in tables]}))
