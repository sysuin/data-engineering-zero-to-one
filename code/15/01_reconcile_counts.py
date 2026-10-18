# Each source's own count beside the warehouse's, every difference
# named; then the warehouse's revenue beside the ledger's, cell by cell.
from groundwork import reconcile

from _sandbox import base, connect

con = connect(base())
print(f"{'':<18}{'source':>10}   {'warehouse':>9}  left out, and why")
for c in reconcile.counts(con):
    why = ", ".join(f"{n:,} {k}" for k, n in c.explained)
    print(f"{c.name:<18}{c.source:>10,} {'=' if c.ties else '!'} "
          f"{c.warehouse:>9,}  {why}".rstrip())

cells = reconcile.revenue(con)
d = reconcile.dollars
months = sorted({c.period for c in cells})
print(f"\nrevenue, {months[0]:%Y-%m} to {months[-1]:%Y-%m}: "
      f"{len(cells)} months x regions")
print(f"  warehouse = ledger's total:  "
      f"{sum(c.warehouse == c.ledger for c in cells)}")
for c in cells:
    if c.warehouse != c.ledger:
        print(f"    {c.period:%Y-%m} {c.region}  warehouse "
              f"{d(c.warehouse):>13,}  ledger {d(c.ledger):>13,}")
print(f"  warehouse = sales journal:   "
      f"{sum(c.ties for c in cells)}")
