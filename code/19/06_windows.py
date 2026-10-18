# Tumbling, sliding and session windows over the portal's events.
from datetime import datetime, timedelta
from itertools import groupby

import duckdb

from _stream import HERE

HOUR, SLIDE, GAP = (timedelta(hours=1), timedelta(minutes=15),
                    timedelta(minutes=30))
EPOCH = datetime(2025, 1, 1)


def tumbling(t: datetime, size=HOUR) -> datetime:
    """The one window an event falls in: its start."""
    return t - (t - EPOCH) % size


def sliding(t: datetime, size=HOUR, slide=SLIDE) -> list[datetime]:
    """Every window an event falls in, by start."""
    last = t - (t - EPOCH) % slide
    return [last - k * slide for k in range(size // slide)]


def sessions(events: list[tuple], gap=GAP) -> list[list[tuple]]:
    """Split one key's events, (time, ...) in time order, wherever the
    silence between two of them is longer than gap."""
    out: list[list[tuple]] = []
    for e in events:
        if out and e[0] - out[-1][-1][0] <= gap:
            out[-1].append(e)
        else:
            out.append([e])
    return out


con = duckdb.connect(str(HERE / "portal.duckdb"), read_only=True)
carts = [t for (t,) in con.execute("""
    SELECT occurred_at FROM stream.portal_events
    WHERE event_type = 'add_to_cart'
      AND occurred_at >= '2025-12-09 13:00'
      AND occurred_at < '2025-12-09 16:00' ORDER BY occurred_at""")
         .fetchall()]
tumble, slide = {}, {}
for t in carts:
    tumble[tumbling(t)] = tumble.get(tumbling(t), 0) + 1
    for w in sliding(t):
        slide[w] = slide.get(w, 0) + 1
print("Carts added on 9 December 2025, 13:00 to 16:00 UTC")
print("  tumbling, 1 hour    sliding, 1 hour, every 15 minutes")
first, last = datetime(2025, 12, 9, 13), datetime(2025, 12, 9, 15)
for w in sorted(w for w in slide if first <= w <= last):
    left = f"{w:%H:%M}  {tumble[w]:>3}" if w in tumble else ""
    print(f"  {left:<18}  {w:%H:%M}-{w + HOUR:%H:%M}  {slide[w]:>3}")
print(f"  {len(carts)} carts: counted {sum(tumble.values())} times in "
      f"tumbling windows,\n  {sum(slide.values())} in sliding ones")

# Session windows per account over every event, beside the portal's ids.
rows = con.execute("""
    SELECT account_id, occurred_at, session_id FROM stream.portal_events
    ORDER BY account_id, occurred_at, event_id""").fetchall()
found = merged = 0
pieces: dict[str, int] = {}        # portal session: windows it is in
for _, events in groupby(rows, key=lambda r: r[0]):
    for window in sessions([(t, sid) for _, t, sid in events]):
        found += 1
        ids = {sid for _, sid in window}
        merged += len(ids) > 1
        for sid in ids:
            pieces[sid] = pieces.get(sid, 0) + 1
split = sum(n > 1 for n in pieces.values())
print(f"\nSession windows, a {GAP.seconds // 60}-minute gap, per "
      f"account: {found:,}")
print(f"  the portal's session ids: {len(pieces):,}")
print(f"  windows holding two or more portal sessions: {merged}")
print(f"  portal sessions cut in two by a gap: {split}")
