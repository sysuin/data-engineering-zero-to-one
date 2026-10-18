# The whole log sessionised into fct_portal_sessions, stopping half way.
import duckdb

from _stream import HERE, fresh_group, portal_log, region_of
from groundwork.stream import sessions

log, regions = portal_log(), region_of()
con = duckdb.connect(str(HERE / "portal.duckdb"))
con.execute("DROP TABLE IF EXISTS marts.fct_portal_sessions")
fresh_group(log, "sessions")

first = sessions.run(log, con, regions, until=350_000)  # then stops
print(f"First run stopped at offset 350,000 with "
      f"{len(first.open):,} sessions open")
second = sessions.run(log, con, regions)        # as a new process
sessions.SessionSink(con).upsert(second.close_all())      # input ended
c = second.counts
print(f"Second run resumed from its checkpoint and read to "
      f"{log.end_offset:,}")
print(f"  events {c['events']:,}: duplicates {c['duplicates']:,}, "
      f"late but taken {c['late']:,}, too late {c['too_late']}")

rows, events, bots, shifted, revised = con.execute("""
    SELECT count(*), sum(event_count), count(*) FILTER (WHERE is_bot),
           count(*) FILTER (WHERE clock_offset_s <> 0),
           count(*) FILTER (WHERE late_events > 0)
    FROM marts.fct_portal_sessions""").fetchone()
stored, ids = con.execute("""SELECT count(*), count(DISTINCT session_id)
                             FROM stream.portal_events""").fetchone()
print(f"\nfct_portal_sessions: {rows:,} rows holding {events:,} events")
print(f"  stream.portal_events: {ids:,} session ids, {stored:,} events")
print(f"  bots {bots}, clocks corrected {shifted}, "
      f"rows revised by late events {revised:,}")

print("\nOne row: the session whose search arrived a day late")
row = con.execute("""SELECT * FROM marts.fct_portal_sessions
                     WHERE session_id = 's-7d09eab930c9'""")
for (name, *_), value in zip(row.description, row.fetchone()):
    print(f"  {name:<18} {value}")
con.close()

# The same log in one uninterrupted run gives the same table.
fresh_group(log, "straight")
other = duckdb.connect()
other.execute(f"ATTACH '{HERE / 'portal.duckdb'}' AS p (READ_ONLY)")
straight = sessions.run(log, other, regions, group="straight")
sessions.SessionSink(other).upsert(straight.close_all())
(differ,) = other.execute("""
    SELECT count(*) FROM (
        (SELECT * FROM p.marts.fct_portal_sessions
         EXCEPT SELECT * FROM marts.fct_portal_sessions)
        UNION ALL
        (SELECT * FROM marts.fct_portal_sessions
         EXCEPT SELECT * FROM p.marts.fct_portal_sessions))
    """).fetchone()
print(f"Rows that differ from one uninterrupted run: {differ}")
fresh_group(log, "straight")

