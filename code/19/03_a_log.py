# The portal's events written to an append-only log, and read back.
import json
import shutil

from _stream import HERE, portal_log
from groundwork.stream.log import Log, OffsetOutOfRange, append_files
from meridian.sources import landing

log = portal_log(fresh=True)       # every hourly file, as they arrived
files = landing.arrived(under="portal")
print(f"Replayed {len(files):,} files: offsets {log.start_offset} to "
      f"{log.end_offset - 1:,}")
for seg in sorted(log.path.glob("*.log")):
    n = (seg.with_suffix(".idx").stat().st_size) // 8
    print(f"  {seg.name}  {n:>7,} records"
          f"  {seg.stat().st_size / 1e6:5.1f} MB")

print("\nThree records, read back by offset")
for offset, value in log.read(0, 2) + log.read(500_000, 1):
    e = json.loads(value)
    print(f"  {offset:>7,}  {e['received_at']}  {e['event_type']:<16}"
          f" {len(value)} bytes")

again = append_files(log, files)
print(f"\nReplaying the same files again appended {again} records")

# A small log, to show what a crash and retention do to the files.
small_dir = HERE / "small-log"
shutil.rmtree(small_dir, ignore_errors=True)
small = Log(small_dir, segment_records=2)
for word in (b"a", b"b", b"c", b"d", b"e"):
    small.append(word)
small.close()
active = sorted(small_dir.glob("*.log"))[-1]
with open(active, "ab") as f:          # half a record, then the "crash"
    f.write(b'{"event_id": "half')
small = Log(small_dir, segment_records=2)
print(f"\nAfter a torn write, the small log ends at offset "
      f"{small.end_offset};\n  the next append gets offset "
      f"{small.append(b'f')}")
print(f"Retention dropped {small.retain(keep_from=4)} segments; "
      f"oldest offset now {small.start_offset}")
try:
    small.read(0)
except OffsetOutOfRange as e:
    print(f"Reading offset 0: {type(e).__name__}: {e}")
small.close()
shutil.rmtree(small_dir)
