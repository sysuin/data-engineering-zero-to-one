# Exercise 2.3: early West orders on Sacramento's calendar, twice.
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import duckdb

from groundwork.config import DEPOT_TZ, LANDING, TRADELINE_DB

accounts = LANDING / "relay" / "2026-01-04" / "accounts.csv"
zone = DEPOT_TZ[4]                      # the West's depot, Sacramento
con = duckdb.connect()
con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline "
            "(TYPE sqlite, READ_ONLY)")
rows = con.execute(f"""
    SELECT o.id, o.placed_at,
           CAST((CAST(o.placed_at AS TIMESTAMP) AT TIME ZONE 'UTC')
                AT TIME ZONE '{zone}' AS DATE) AS local_date
    FROM tradeline.orders o
    JOIN read_csv('{accounts}') a ON a.account_id = o.account_id
    WHERE a.region = 'West'
      AND o.placed_at >= '2025-12-09 00:00:00'
      AND o.placed_at <  '2025-12-09 08:00:00'
    ORDER BY o.placed_at, o.id""").fetchall()

agree = 0
for order_id, placed_at, in_sql in rows:
    naive = datetime.fromisoformat(placed_at)
    moment = naive.replace(tzinfo=timezone.utc)
    in_python = moment.astimezone(ZoneInfo(zone)).date()
    agree += in_python == in_sql
print(f"orders {len(rows)}, Python and SQL agree on {agree}")
on_8th = sum(1 for *_, d in rows if str(d) == "2025-12-08")
print(f"on 8 December in Sacramento: {on_8th}")
