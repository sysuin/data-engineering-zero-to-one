# Exercise 19.3: abandoned carts in sliding windows over one day.
from datetime import datetime, timedelta

import duckdb

from _stream import HERE

con = duckdb.connect(str(HERE / "portal.duckdb"), read_only=True)
ends = [t for (t,) in con.execute("""
    SELECT ended_at + INTERVAL 30 MINUTE FROM marts.fct_portal_sessions
    WHERE NOT is_bot AND add_to_carts > 0 AND orders_submitted = 0
      AND ended_at >= '2025-12-08 23:00' AND ended_at < '2025-12-10'
    ORDER BY 1""").fetchall()]
day, hour, step = datetime(2025, 12, 9), timedelta(hours=1), \
    timedelta(minutes=15)
counted, sessions, now = 0, set(), day + step
while now <= day + timedelta(days=1):
    inside = [t for t in ends if now - hour <= t < now]
    counted += len(inside)
    sessions.update(inside)
    now += step
print(f"96 sliding windows on 9 December count {counted} carts;")
print(f"  {len(sessions)} distinct abandoned sessions, each counted "
      f"{counted / len(sessions):.1f} times on average")
