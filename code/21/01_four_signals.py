# The four signals for every night of July 2024, measured from the
# records a replay of Groundwork's nightly run left behind.
import json
import textwrap
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import duckdb
from _sandbox import morning, replay

from groundwork.observe import metrics

folder = replay("unwatched")
con = duckdb.connect(str(folder / "warehouse.duckdb"), read_only=True)
july = [date(2024, 7, 1) + timedelta(days=i) for i in range(31)]
nights = {d: metrics.collect(con, d, morning(d), folder / "raw")
          for d in july}

count, first, quiet = defaultdict(int), {}, defaultdict(int)
for d, signals in nights.items():
    for s in signals:
        key = (s.signal, s.subject, s.measure)
        count[key] += s.alarm
        if s.alarm and key not in first:
            first[key] = d
print(f"July 2024: {len(july)} nights, {len(nights[july[0]])} "
      "signals on the first")
print(f"{'signal':<13}{'subject':<26}{'measure':<16}{'nights':>6}"
      " first")
for key, n in count.items():
    if key[0] == "schema" and not n:
        quiet["schema"] += 1
        continue
    when = f" {first[key].day} Jul" if key in first else ""
    print(f"{key[0]:<13}{key[1]:<26}{key[2]:<16}{n:>6}{when}")
print(f"{'schema':<13}{quiet['schema']} more tables: no change on any "
      "night")

d = date(2024, 7, 2)
print(f"\nin alarm on the night of {d.day} July, judged at 07:00 "
      "on the 3rd:")
for s in nights[d]:
    if s.alarm:
        print(textwrap.fill(f"{s.signal} {s.subject}: {s.detail}", 66,
                            initial_indent="  ",
                            subsequent_indent="    "))
sup = [s for s in nights[d] if s.signal == "freshness"
       and s.subject == "suppliers.price_lists"][0]
print(f"  but freshness {sup.subject}: {sup.status}, {sup.detail}")


def one(signals, signal, subject, measure=None):
    for s in signals:
        if (s.signal, s.subject) == (signal, subject) and \
                measure in (None, s.measure):
            return [s.value, s.low, s.high, s.status]
    return None


Path(__file__).with_suffix(".json").write_text(json.dumps([
    {"day": d.isoformat(),
     "published": one(s, "freshness", "published"),
     "relay": one(s, "freshness", "relay.accounts"),
     "orders": one(s, "volume", "tradeline.orders"),
     "schema": one(s, "schema", "suppliers.price_lists"),
     "cancelled": one(s, "distribution", "tradeline.orders_log")}
    for d, s in nights.items()], indent=1))
con.close()
