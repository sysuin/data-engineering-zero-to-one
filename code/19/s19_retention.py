# Exercise 19.2: a consumer that falls behind retention.
import shutil

from _stream import HERE
from groundwork.stream.consumer import Consumer
from groundwork.stream.log import Log, OffsetOutOfRange, append_files
from meridian.sources import landing

path = HERE / "july-log"
shutil.rmtree(path, ignore_errors=True)
log = Log(path, segment_records=10_000)
july = landing.arrived(by="2025-08-01 01:05", under="portal")
append_files(log, july)
slow = Consumer(log, "slow")
slow.seek(5_000)
slow.commit()
print(f"July: {log.end_offset:,} records; slow committed at 5,000")
print(f"retention dropped {log.retain(keep_from=30_000)} segments; "
      f"oldest offset now {log.start_offset:,}")
slow = Consumer(log, "slow")
try:
    slow.poll(100)
except OffsetOutOfRange as e:
    print(f"slow polls: {e}")
# The reset Kafka calls auto.offset.reset = earliest.
if slow.position < log.start_offset:
    missed = log.start_offset - slow.position
    slow.seek(log.start_offset)
    print(f"reset to {slow.position:,}; {missed:,} records never seen")
print(f"next poll returns {len(slow.poll(100))} records")
log.close()
shutil.rmtree(path)
