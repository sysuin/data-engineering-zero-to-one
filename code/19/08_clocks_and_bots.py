# Phones with the wrong time, checked against Tradeline; then the bot.
import duckdb

from _stream import HERE
from groundwork.config import TRADELINE_DB
from groundwork.stream.sessions import BOT_EVENTS, BOT_PER_MINUTE

con = duckdb.connect(str(HERE / "portal.duckdb"), read_only=True)
con.execute(f"ATTACH '{TRADELINE_DB}' AS tl (TYPE sqlite, READ_ONLY)")
con.execute("""
    CREATE TEMP TABLE s AS
    SELECT session_id, any_value(device) AS device, count(*) AS events,
           min(epoch(received_at) - epoch(occurred_at)) AS quickest,
           count(*) FILTER (WHERE occurred_at > received_at) AS future
    FROM stream.portal_events GROUP BY session_id""")
n, sessions, most = con.execute("""
    SELECT sum(future), count(*) FILTER (WHERE future > 0),
           max(-quickest) / 3600 FROM s""").fetchone()
print(f"Events that happened after the portal received them: {n:,}\n"
      f"  in {sessions} sessions; furthest ahead {most:.1f} hours")
print("\nSessions by their quickest event's delay")
for label, rows, dev in con.execute("""
        SELECT CASE WHEN quickest < -60 THEN 'clock ahead, over 1 min'
                    WHEN quickest > 60 THEN 'clock behind, over 1 min'
                    ELSE 'within a minute' END, count(*),
               string_agg(DISTINCT device, ', ' ORDER BY device)
        FROM s GROUP BY 1 ORDER BY 1""").fetchall():
    print(f"  {label:<26} {rows:>7,}  {dev}")

# Tradeline says when each web order was placed: a second clock.
con.execute("""
    CREATE TEMP TABLE o AS
    SELECT epoch(e.occurred_at) - epoch(t.placed_at::TIMESTAMP) AS off,
           CASE WHEN abs(s.quickest) > 60 THEN s.quickest ELSE 0 END
               AS shift
    FROM stream.portal_events e JOIN s USING (session_id)
    JOIN tl.orders t ON t.order_number = e.order_number
    WHERE e.event_type = 'order_submitted'""")
print("\norder_submitted against Tradeline's placed_at")
for label, expr in (("as sent", "off"),
                    ("clock corrected", "off + shift")):
    total, wrong, worst = con.execute(f"""
        SELECT count(*), count(*) FILTER (WHERE abs({expr}) > 5),
               max(abs({expr})) FROM o""").fetchone()
    worst = f"{worst / 3600:.1f} h" if worst > 60 else f"{worst:.1f} s"
    print(f"  {label:<16} {wrong:>4} of {total:,} more than 5 s out;"
          f" worst {worst}")

print("\nThe busiest accounts")
for acct, events, sess in con.execute("""
        SELECT account_id, count(*), count(DISTINCT session_id)
        FROM stream.portal_events GROUP BY 1 ORDER BY 2 DESC LIMIT 3
        """).fetchall():
    print(f"  account {acct:<5} {events:>7,} events in {sess} sessions")
rest = con.execute("""
    SELECT count(*), count(DISTINCT session_id)
    FROM stream.portal_events
    WHERE account_id = 2040 AND session_id NOT IN (
        SELECT session_id FROM s WHERE events >= ?)""", [BOT_EVENTS])
print("  2040 less its three biggest sessions: "
      "{:,} events in {} sessions".format(*rest.fetchone()))
print(f"\nSessions of {BOT_EVENTS}+ events at {BOT_PER_MINUTE}+ a "
      "minute")
for sid, events, start, mins, carts, agent in con.execute("""
        SELECT session_id, count(*), min(occurred_at),
               (epoch(max(occurred_at)) - epoch(min(occurred_at))) / 60,
               count(*) FILTER (WHERE event_type <> 'product_view'),
               any_value(user_agent)
        FROM stream.portal_events GROUP BY 1
        HAVING count(*) >= ? AND count(*) / (epoch(max(occurred_at))
               - epoch(min(occurred_at))) * 60 >= ?
        ORDER BY 3""", [BOT_EVENTS, BOT_PER_MINUTE]).fetchall():
    print(f"  {start:%d %b %H:%M} {events:,} in {mins:.0f} min "
          f"({events / mins:.0f}/min), {carts} not views\n    {agent}")
