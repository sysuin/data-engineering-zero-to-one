# Exercise 19.5: clocks that run behind, seen by the stream as late.
import duckdb

from _stream import HERE

con = duckdb.connect(str(HERE / "portal.duckdb"), read_only=True)
sessions, events, late = con.execute("""
    SELECT count(*), sum(event_count), sum(late_events)
    FROM marts.fct_portal_sessions
    WHERE clock_offset_s > 60""").fetchone()
print(f"sessions with a clock behind: {sessions}, {events:,} events")
print(f"  events the stream took as late: {late:,}")
