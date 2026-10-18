# The funnel, its orders reconciled with Tradeline, and carts, live.
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from _stream import HERE, fresh_group, offset_at, portal_log, region_of
from groundwork.config import DEPOT_TZ, TRADELINE_DB
from groundwork.stream import sessions
from groundwork.stream.consumer import Consumer

LAUNCH = datetime(2025, 7, 1, 4)          # midnight in Newark, in UTC
con = duckdb.connect(str(HERE / "portal.duckdb"), read_only=True)
f = sessions.funnel(con, LAUNCH, datetime(2026, 1, 5))
print("Every portal session since launch, bots left out")
stages = [("sessions", "sessions"), ("added to a cart", "carted"),
          ("started checkout", "checked_out"),
          ("submitted an order", "ordered")]
for label, key in stages:
    print(f"  {label:<20} {f[key]:>7,}  {f[key] / f['sessions']:6.1%}")

con.execute(f"ATTACH '{TRADELINE_DB}' AS tl (TYPE sqlite, READ_ONLY)")
con.execute("""
    CREATE TEMP TABLE web AS SELECT order_number, status, placed_at
    FROM tl.orders WHERE channel = 'WEB'
      AND placed_at::TIMESTAMP >= ?""", [LAUNCH])
con.execute("""
    CREATE TEMP TABLE sub AS SELECT order_number, value_cents
    FROM stream.portal_events WHERE event_type = 'order_submitted'""")
(web, subs, only_portal, only_tl, cancelled, before) = con.execute("""
    SELECT (SELECT count(*) FROM web), (SELECT count(*) FROM sub),
      (SELECT count(*) FROM sub ANTI JOIN web USING (order_number)),
      (SELECT count(*) FROM web ANTI JOIN sub USING (order_number)),
      (SELECT count(*) FROM web WHERE status = 'cancelled'),
      (SELECT count(*) FROM tl.orders WHERE channel = 'WEB'
         AND placed_at >= '2025-07-01'
         AND placed_at < '2025-07-01 04:00')
    """).fetchone()
first = con.execute("""SELECT min(order_number) FROM web
                        WHERE status = 'cancelled'""").fetchone()[0]
print()
for label, n in (("Orders in the sessions, summed", f["orders"]),
                 ("order_submitted events", subs),
                 ("Tradeline's web orders since launch", web)):
    print(f"{label:<36} {n:>7,}")
print(f"  only in the portal {only_portal}, only in Tradeline "
      f"{only_tl}")
print(f"  cancelled since in Tradeline: {cancelled} (the first, "
      f"order {first})")
print(f"  web orders on 1 July before 04:00 UTC (30 June in Newark): "
      f"{before}")
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {**f, "tradeline_web": web, "cancelled": cancelled}))
priced, agree = con.execute("""
    SELECT count(*), count(*) FILTER (WHERE s.value_cents = t.cents)
    FROM sub s JOIN (SELECT o.order_number,
                            sum(l.qty * l.unit_price_cents) AS cents
                     FROM tl.orders o JOIN tl.order_lines l
                       ON l.order_id = o.id GROUP BY 1) t
      USING (order_number)
    WHERE s.value_cents IS NOT NULL""").fetchone()
print(f"Schema 2 orders with a value: {priced:,}; equal to the sum of "
      f"their\n  lines in Tradeline: {agree:,}")

# Live: two streams as they stood at each hour of 9 December, one
# started at midnight on the 8th, one four days earlier; then the
# answer once every late event had arrived.
log, regions = portal_log(), region_of()
live = {"cold": (datetime(2025, 12, 8), duckdb.connect()),
        "warm": (datetime(2025, 12, 4), duckdb.connect())}
for group, (since, _) in live.items():
    fresh_group(log, group)
    c = Consumer(log, group)
    c.seek(offset_at(log, since))
    c.commit()
print("\nCarts abandoned in the hour to the watermark, 9 December")
print("  read to  watermark (Newark)   cold  warm  final")
for h in range(13, 23):
    now = datetime(2025, 12, 9, h)
    counts = []
    for group, (_, db) in live.items():
        wm = sessions.run(log, db, regions, group=group,
                          until=offset_at(log, now)).watermark
        counts.append(sessions.abandoned_carts(db, wm))
    newark = wm.replace(tzinfo=UTC).astimezone(ZoneInfo(DEPOT_TZ[1]))
    print(f"  {now:%H:%M}    {wm:%H:%M:%S} ({newark:%H:%M})"
          f"  {counts[0]:>5} {counts[1]:>5}"
          f"  {sessions.abandoned_carts(con, wm):>5}")
for group in live:
    fresh_group(log, group)

# Every hour of December, from a stream started four days before it.
fresh_group(log, "month")
c, db = Consumer(log, "month"), duckdb.connect()
c.seek(offset_at(log, datetime(2025, 11, 27)))
c.commit()
hours, total, over, under = 0, 0, [], []
now = datetime(2025, 12, 1)
while now < datetime(2026, 1, 1):
    wm = sessions.run(log, db, regions, group="month",
                      until=offset_at(log, now)).watermark
    final = sessions.abandoned_carts(con, wm)
    gap = sessions.abandoned_carts(db, wm) - final
    hours, total = hours + 1, total + final
    (over if gap > 0 else under if gap < 0 else []).append(abs(gap))
    now += timedelta(hours=1)
fresh_group(log, "month")
print(f"December, {hours} hours, {total:,} carts abandoned in all")
print(f"  live above final in {len(over)} hours "
      f"(by {sum(over)}), below it in {len(under)} (by {sum(under)})")
