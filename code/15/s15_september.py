# Exercise 1: the bridge for September 2024, the month of the other
# journal, from the dashboard's own query on Tradeline.
from datetime import date

from groundwork import reconcile
from groundwork.config import TRADELINE_DB

from _sandbox import base, connect

SEP = date(2024, 9, 1)
con = connect(base())
start, exact, orders = reconcile.dashboard(con, SEP,
                                           tradeline=TRADELINE_DB)
b = reconcile.bridge(con, SEP, start=start, exact=exact, orders=orders)
print(f"{'September 2024':<28}{'orders':>7}{'step':>15}")
print(f"{'the dashboard':<28}{orders:>7,}{b.start!s:>32}")
for s in b.steps:
    n = f"{s.orders:,}" if s.orders is not None else ""
    print(f"{s.name:<28}{n:>7}{reconcile.money(s.amount):>15}")
print(f"{'the ledger':<28}{reconcile.money(b.ledger):>39}")
print(f"{'remaining':<28}{reconcile.money(b.remaining):>39}")
print("journal:", b.steps[-1].note)
