# How late is too late: one stream, sessionised with six limits.
import json
from datetime import datetime, timedelta
from pathlib import Path

from _stream import offset_at, portal_log, region_of
from groundwork.stream.consumer import parse
from groundwork.stream.sessions import Sessioniser

log = portal_log()
start = offset_at(log, datetime(2025, 11, 1))
regions = region_of()


def replay(**settings):
    """Sessionise November and December; return the sessioniser, each
    session's final event count, and the first late event it took."""
    s, final, example = Sessioniser(regions, **settings), {}, None
    offset = start
    while batch := log.read(offset, 5000):
        offset += len(batch)
        for _, value in batch:
            e = parse(value)
            wm = s.watermark
            if s.add(e) != "late" or example is not None:
                continue
            seen = s.open[e["session_id"]].events.values()
            if (e["received_at"] - e["occurred_at"] > timedelta(hours=2)
                    and min(r - o for _, o, r in seen)
                    < timedelta(minutes=1)):
                times = [o for _, o, _ in seen]
                example = {"event": e, "watermark": wm,
                           "first": min(times), "last": max(times)}
        final.update((r["session_id"], r["event_count"])
                     for r in s.advance())
    final.update((r["session_id"], r["event_count"])
                 for r in s.close_all())
    return s, final, example


def line(label, s, final) -> str:
    """One row of the table: what a run dropped, took late, wrote."""
    short = sum(final.get(sid, 0) < len(ids)
                for sid, ids in truth.items())
    c = s.counts
    return (f"{label:<9} {c['too_late']:>9,} {c['late']:>12,}"
            f" {c['rows']:>13,} {short:>15,}")


truth: dict[str, set] = {}
for _, value in log.read(start, log.end_offset - start):
    e = json.loads(value)
    truth.setdefault(e["session_id"], set()).add(e["event_id"])
print(f"November and December: offsets {start:,} to "
      f"{log.end_offset - 1:,},\n  {len(truth):,} sessions, "
      f"{sum(map(len, truth.values())):,} distinct events")

print(f"\n{'lateness':<9} {'too late':>9} {'late, taken':>12}"
      f" {'rows written':>13} {'sessions short':>15}")
limits = [("0", timedelta(0)), ("10 min", timedelta(minutes=10)),
          ("1 hour", timedelta(hours=1)),
          ("6 hours", timedelta(hours=6)),
          ("1 day", timedelta(days=1)), ("3 days", timedelta(days=3))]
for label, lateness in limits:
    s, final, example = replay(lateness=lateness)
    print(line(label, s, final))
    if label == "3 days":
        e, wm = example["event"], example["watermark"]
        Path(__file__).with_suffix(".json").write_text(json.dumps(
            {k: str(v) for k, v in {**e, "watermark": wm,
                                    "session_first": example["first"],
                                    "session_last": example["last"]
                                    }.items()}))

s, final, _ = replay(lateness=timedelta(hours=1), clamp=False)
print("\nThe same 1 hour, trusting device clocks that run ahead:")
print(line("1 hour", s, final))
print(f"\nThe late event saved for the figure: a {e['event_type']} "
      f"from {e['occurred_at']:%d %b %H:%M},\n  received "
      f"{e['received_at']:%d %b %H:%M}, watermark then "
      f"{wm:%d %b %H:%M}")
