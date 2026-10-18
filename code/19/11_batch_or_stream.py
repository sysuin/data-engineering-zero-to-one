# The same sessions from one batch query, and what each way can see.
from datetime import UTC, datetime

import duckdb

from _stream import HERE, offset_at, portal_log
from groundwork.config import LANDING
from groundwork.stream.consumer import parse
from meridian.sources import landing

con = duckdb.connect()
con.execute(f"ATTACH '{HERE / 'portal.duckdb'}' AS p (READ_ONLY)")
con.execute(f"""
    CREATE TABLE ev AS
    SELECT DISTINCT ON (event_id) event_id, session_id, event_type,
      strptime(occurred_at, '%Y-%m-%dT%H:%M:%S.%gZ') AS occurred_at,
      strptime(received_at, '%Y-%m-%dT%H:%M:%S.%gZ') AS received_at
    FROM read_json('{LANDING}/portal/*/*/*/*.jsonl.gz',
                   columns = {{event_id: 'VARCHAR',
                              session_id: 'VARCHAR',
                              event_type: 'VARCHAR',
                              occurred_at: 'VARCHAR',
                              received_at: 'VARCHAR'}})
    ORDER BY event_id, received_at""")
con.execute("""
    CREATE TABLE batch AS
    WITH s AS (
      SELECT session_id, min(received_at - occurred_at) AS quickest,
             min(occurred_at) AS first_at, max(occurred_at) AS last_at,
             count(*) AS event_count,
             count(*) FILTER (WHERE event_type = 'search') AS searches,
             count(*) FILTER (WHERE event_type = 'product_view')
                 AS product_views,
             count(*) FILTER (WHERE event_type = 'add_to_cart')
                 AS add_to_carts,
             count(*) FILTER (WHERE event_type = 'checkout_started')
                 AS checkouts_started,
             count(*) FILTER (WHERE event_type = 'order_submitted')
                 AS orders_submitted
      FROM ev GROUP BY session_id)
    SELECT * EXCLUDE (quickest, first_at, last_at),
      first_at + CASE WHEN abs(epoch(quickest)) > 60 THEN quickest
                      ELSE INTERVAL 0 SECOND END AS started_at,
      last_at + CASE WHEN abs(epoch(quickest)) > 60 THEN quickest
                     ELSE INTERVAL 0 SECOND END AS ended_at
    FROM s""")
cols = ("session_id, event_count, searches, product_views, "
        "add_to_carts, checkouts_started, orders_submitted, "
        "started_at, ended_at")
stream = "p.marts.fct_portal_sessions"
diff = con.execute(f"""
    SELECT 'batch ', * FROM (SELECT {cols} FROM batch
                            EXCEPT SELECT {cols} FROM {stream})
    UNION ALL
    SELECT 'stream', * FROM (SELECT {cols} FROM {stream}
                            EXCEPT SELECT {cols} FROM batch)
    ORDER BY 2, 1""").fetchall()
n = con.execute("SELECT count(*) FROM batch").fetchone()[0]
print(f"One batch query over every file: {n:,} sessions")
print(f"  rows that differ from the stream's: {len(diff)}")
for way, sid, events, *_, started, _ in diff:
    print(f"  {way} {sid}  {events} events, started "
          f"{started:%Y-%m-%d %H:%M:%S}")

(occurred, received, offset), = con.execute(f"""
    SELECT e.occurred_at, e.received_at, s.clock_offset_s
    FROM ev e JOIN {stream} s USING (session_id)
    WHERE session_id = ? ORDER BY e.received_at DESC LIMIT 1""",
    [diff[0][1]]).fetchall()
queued = (received - occurred).total_seconds() - offset
print(f"  its last event: phone clock {offset / 3600:.1f} h slow, then "
      f"{queued / 3600:.1f} h in the queue")

# What each could know at 15:00 UTC on 9 December.
now = datetime(2025, 12, 9, 15)
files = landing.arrived(by=now, under="portal")
log = portal_log()
(_, newest), = log.read(offset_at(log, now) - 1, 1)
print(f"\nAt {now:%H:%M} UTC on 9 December:")
arrived = datetime.fromtimestamp(landing.arrival(files[-1]), UTC)
print(f"  the newest file: {files[-1].relative_to(LANDING).as_posix()},"
      f" arrived {arrived:%H:%M}")
print(f"  the log's newest event: received "
      f"{parse(newest)['received_at']:%H:%M:%S}")
