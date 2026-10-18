# Exercise 19.4: how many sessions each lateness setting holds at once.
from datetime import datetime, timedelta

from _stream import offset_at, portal_log, region_of
from groundwork.stream.consumer import parse
from groundwork.stream.sessions import Sessioniser

log, regions = portal_log(), region_of()
start = offset_at(log, datetime(2025, 11, 1))
print(f"{'lateness':<9} {'most held':>10} {'events held':>12}")
for label, late in (("0", timedelta(0)), ("1 hour", timedelta(hours=1)),
                    ("1 day", timedelta(days=1)),
                    ("3 days", timedelta(days=3))):
    s = Sessioniser(regions, lateness=late)
    most, events, offset = 0, 0, start
    while batch := log.read(offset, 5000):
        offset += len(batch)
        for _, value in batch:
            s.add(parse(value))
        s.advance()
        if len(s.open) > most:
            most = len(s.open)
            events = sum(len(x.events) for x in s.open.values())
    print(f"{label:<9} {most:>10,} {events:>12,}")
