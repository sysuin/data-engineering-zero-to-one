# Exercise 10.1: a contract for the portal's events, written from one
# week of September 2025 and held to every hour of that week.
import gzip
import json
import shutil
from collections import Counter
from datetime import date, timedelta


from groundwork import contracts
from groundwork.config import LANDING, SANDBOX
from groundwork.contracts import check

CONTRACT = """
table: portal.events
version: 1
description: One row per event the portal's web or app client sent.
producer: {who: the portal team, contact: portal@meridian.example}
owner: data engineering (Groundwork stream, Chapter 19)
grain: one row per event as delivered
columns:
  event_id: {pattern: '[0-9a-f-]{36}'}
  event_type: {allowed: [session_start, search, product_view,
                         add_to_cart, checkout_started,
                         order_submitted]}
  schema_version: {type: integer, allowed: ['1']}
  occurred_at: {type: timestamp, unit: UTC}
  session_id: {pattern: 's-[0-9a-f]{12}'}
  account_id: {type: integer, min: 1}
  user_id: {pattern: 'u-[0-9]{6}-[0-9]+'}
  device: {allowed: [desktop, mobile]}
  user_agent: {}
  properties: {}
  received_at: {type: timestamp, unit: UTC}
policy: {bad_rows: quarantine, max_bad_share: 0.01}
"""
here = SANDBOX / "ch10" / "s10_portal"
here.mkdir(parents=True, exist_ok=True)
(here / "portal.events.yaml").write_text(CONTRACT)
c = contracts.load("portal.events", here)

hours, rows, bad, status = 0, 0, Counter(), Counter()
day = date(2025, 9, 8)                       # Monday to Sunday
while day <= date(2025, 9, 14):
    folder = LANDING / "portal" / f"{day:%Y/%m/%d}"
    for f in sorted(folder.glob("*.gz")):
        events = [json.loads(ln) for ln in gzip.open(f, "rt")]
        for e in events:                     # a nested field as text
            e["properties"] = json.dumps(e["properties"],
                                         sort_keys=True)
        res = check.check(c, events)
        hours, rows = hours + 1, rows + res.rows
        status[res.status] += 1
        bad.update(f"{b.column}: {b.rule}" for b in res.bad)
    day += timedelta(days=1)
print(f"{c.table} v{c.version}: {len(c.columns)} columns")
print(f"{hours} hours, {rows:,} events: {dict(status)}")
print(f"rules broken: {dict(bad) or 'none'}")
print(f"types used: {sorted({x.type for x in c.columns.values()})}")
print("properties' keys by event type:")
keys = Counter()
for f in sorted((LANDING / "portal/2025/09/10").glob("*.gz")):
    for ln in gzip.open(f, "rt"):
        e = json.loads(ln)
        keys[(e["event_type"], tuple(sorted(e["properties"])))] += 1
for (t, k), n in sorted(keys.items()):
    print(f"  {t:<17} {', '.join(k):<24} {n:>5,}")
shutil.rmtree(here)
