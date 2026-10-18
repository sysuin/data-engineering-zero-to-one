# Events delivered twice, and a sink that stores each one once.
import hashlib
import json
from datetime import datetime

import duckdb

from _stream import HERE, fresh_group, portal_log
from groundwork.stream.consumer import archive

log = portal_log()
first: dict[str, tuple] = {}     # event_id -> (received_at, the rest)
twins = same = other_hour = 0
delays = []
offset = log.start_offset
while batch := log.read(offset, 50_000):
    offset += len(batch)
    for _, value in batch:
        e = json.loads(value)
        received = e.pop("received_at")
        rest = hashlib.md5(json.dumps(e).encode()).digest()
        seen = first.setdefault(e["event_id"], (received, rest))
        if seen == (received, rest):
            continue
        twins += 1
        same += seen[1] == rest
        other_hour += seen[0][:13] != received[:13]
        gap = (datetime.fromisoformat(received)
               - datetime.fromisoformat(seen[0]))
        delays.append(gap.total_seconds())
print(f"{log.end_offset:,} records, {len(first):,} distinct event_ids: "
      f"{twins:,} second copies")
print(f"  identical but for received_at: {same:,}")
print(f"  in a later hour's file than the first copy: {other_hour:,}")
print(f"  arrived {min(delays):.1f} to {max(delays):.1f} seconds after "
      f"the first")

# The idempotent sink: a table keyed on event_id, fed at least once.
db = HERE / "portal.duckdb"
db.unlink(missing_ok=True)
fresh_group(log, "archive")
con = duckdb.connect(str(db))
run1 = archive(log, con, stop_after=250_000)      # crashes, uncommitted
run2 = archive(log, con)                          # restarts
read = run1["read"] + run2["read"]
(rows,) = con.execute(
    "SELECT count(*) FROM stream.portal_events").fetchone()
print(f"\nArchive, crashing once: {read:,} records read, "
      f"{read - log.end_offset:,} of them twice")
print(f"  an append-only table would hold {read:,} rows")
print(f"  stream.portal_events holds {rows:,}")
