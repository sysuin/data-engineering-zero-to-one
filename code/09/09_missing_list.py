# One list a month from every supplier: which months have none, did
# the gap matter, and the month-end check that would have said so.
from collections import defaultdict
from datetime import datetime

from groundwork.config import LANDING
from groundwork.ingest import suppliers
from groundwork.ingest.files import discover

lists = defaultdict(dict)                # supplier -> month -> rows
arrived = {}                             # (supplier, month) -> time
for a in discover(LANDING, "suppliers"):
    p = suppliers.read(a.path.read_bytes(), a)
    who, month = p.logical_key.split("/")
    lists[who][month] = p.rows           # a later version replaces
    arrived.setdefault((who, month), a.arrived_at)

months = [f"{y}-{m:02d}" for y in range(2022, 2026)
          for m in range(1, 13)]
gaps = [(who, m) for who in sorted(lists) for m in months
        if m not in lists[who]]
print(f"{len(lists)} suppliers x {len(months)} months = "
      f"{len(lists) * len(months)} lists expected; "
      f"{sum(len(v) for v in lists.values())} present")
for who, m in gaps:
    print(f"  no list: {who} {m}")

# Did it matter? Compare the lists either side of the gap.
who = gaps[0][0]
before, after = lists[who]["2023-07"], lists[who]["2023-09"]
cost = {r["sku"]: r["cost"] for r in before}
same = sum(cost.get(r["sku"]) == r["cost"] for r in after)
print(f"\n{who}: July and September 2023 lists, "
      f"{len(before)} and {len(after)} products")
print(f"  same cost in both: {same} of {len(after)}")
have = [m for m in months if m in lists[who]]
changed = sum({r["sku"]: r["cost"] for r in lists[who][m]}
              != {r["sku"]: r["cost"] for r in lists[who][prev]}
              for prev, m in zip(have, have[1:]))
print(f"  lists whose costs differ from the one before: {changed} "
      f"of {len(have) - 1}")

# The check a month-end run makes: every supplier that sent a list
# last month should have sent one this month.
def month_end_check(month, by):
    prior = months[months.index(month) - 1]
    return [w for w in sorted(lists)
            if prior in lists[w]
            and not (month in lists[w] and arrived[(w, month)] <= by)]


alerts = []
for m in months[1:]:
    y, mo = map(int, m.split("-"))
    end = datetime(y + mo // 12, mo % 12 + 1, 1)   # midnight, 1st
    alerts += [(m, w) for w in month_end_check(m, end)]
print(f"\nmonth-end checks run: {len(months) - 1}, alerts: "
      f"{len(alerts)}")
for m, w in alerts:
    print(f"  ALERT {m}: no list from {w} by month end")
