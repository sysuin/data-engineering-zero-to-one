# One order's moment on three clocks, a 23-hour day, and a session trap.
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

from groundwork.config import DEPOT_TZ
from meridian.sources import tradeline

ORDER_ID = 474653                       # placed from the West
con = tradeline.connect()
(placed_at,) = con.execute("SELECT placed_at FROM orders WHERE id = ?",
                           (ORDER_ID,)).fetchone()
con.close()
print(f"Tradeline stores {placed_at!r}")

naive = datetime.fromisoformat(placed_at)       # a reading, no zone
moment = naive.replace(tzinfo=timezone.utc)     # Tradeline means UTC
clocks = {}
for place, zone in [("UTC", "UTC"), ("Newark", DEPOT_TZ[1]),
                    ("Sacramento", DEPOT_TZ[4])]:
    local = moment.astimezone(ZoneInfo(zone))
    clocks[place] = f"{local:%Y-%m-%d %H:%M}"
    print(f"  {place:10} {local:%H:%M %Z}  on {local:%a %d %b %Y}"
          f"   same instant: {local == moment}")

try:
    print(naive < moment)
except TypeError as e:
    print(f"TypeError: {e}")

ny = ZoneInfo(DEPOT_TZ[1])
print("length of a day in Newark:")
for day in ["2025-03-09", "2025-06-09", "2025-11-02"]:
    start = datetime.fromisoformat(day).replace(tzinfo=ny)
    end = start + timedelta(days=1)             # next local midnight
    wall = end - start                          # same zone: wall clock
    real = end.astimezone(timezone.utc) - start.astimezone(timezone.utc)
    print(f"  {day}  subtracted {wall}   in UTC {real}")

db = duckdb.connect()
stamp = f"TIMESTAMPTZ '{placed_at}+00'"
for zone in ["UTC", "America/Los_Angeles"]:
    db.execute(f"SET TimeZone = '{zone}'")
    (shown,) = db.execute(f"SELECT {stamp}::VARCHAR").fetchone()
    print(f"session {zone:19} shows {shown}")
(local,) = db.execute(f"""
    SELECT CAST((TIMESTAMP '{placed_at}' AT TIME ZONE 'UTC')
                AT TIME ZONE '{DEPOT_TZ[4]}' AS DATE)""").fetchone()
print(f"Sacramento's date, in SQL, any session: {local}")

Path(__file__).with_suffix(".json").write_text(json.dumps(clocks))
