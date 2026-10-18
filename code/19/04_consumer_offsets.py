# Two consumer groups on one log, and what a crash does to each.
import json
from collections import Counter
from pathlib import Path

from _stream import fresh_group, portal_log
from groundwork.stream.consumer import Consumer

log = portal_log()
for group in ("tally", "archive", "careless"):
    fresh_group(log, group)

# Group "tally" counts event types: work first, then commit.
tally, counts = Consumer(log, "tally"), Counter()
for _ in range(3):
    batch = tally.poll(1000)
    counts.update(json.loads(v)["event_type"] for _, v in batch)
    tally.commit()
counts.update(json.loads(v)["event_type"] for _, v in tally.poll(1000))
print(f"tally read to {tally.position:,}, committed "
      f"{tally.committed():,}, then crashed")
offsets = log.path / "offsets" / "tally.json"
print(f"  {offsets.relative_to(log.path.parent).as_posix()}: "
      f"{offsets.read_text()}")

tally = Consumer(log, "tally")                    # the restart
print(f"tally restarts at {tally.position:,}")
counts.update(json.loads(v)["event_type"] for _, v in tally.poll(1000))
tally.commit()
print(f"  records in the log up to {tally.position:,}; events counted "
      f"{sum(counts.values()):,}")

# Group "careless" commits first and works second.
careless, seen = Consumer(log, "careless"), 0
for _ in range(4):
    batch = careless.poll(1000)
    careless.commit()
    if careless.position == 4000:
        break                               # crash before the work
    seen += len(batch)
careless = Consumer(log, "careless")
batch = careless.poll(1000)
careless.commit()
seen += len(batch)
print(f"careless: records up to {careless.position:,}; events counted "
      f"{seen:,}")

# A second group reads the same log at its own pace.
archive = Consumer(log, "archive")
archive.poll(1000)
archive.commit()
print(f"\n{'group':<9} {'committed':>10} {'lag':>9}")
for c in (tally, careless, archive):
    print(f"{c.group:<9} {c.committed():>10,} {c.lag:>9,}")
print(f"log end   {log.end_offset:>10,}")
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"end": log.end_offset,
     "groups": {c.group: c.committed() for c in (tally, archive)}}))
