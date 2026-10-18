# One name in two encodings, a byte-order mark, and two line endings.
import csv
from pathlib import Path

from groundwork.config import LANDING, SANDBOX
from meridian.sources import relay

name = "Zoë"
for enc in ("cp1252", "utf-8"):
    print(f"{name} in {enc:<6}  {name.encode(enc).hex(' ')}")

old = name.encode("cp1252")
new = name.encode("utf-8")
print(f"cp1252 bytes read as utf-8   "
      f"{old.decode('utf-8', errors='replace')!r}")
print(f"utf-8 bytes read as cp1252   {new.decode('cp1252')!r}")

# Before the migration, and the most recent export on disk.
before = Path(relay.export("2024-02-29", SANDBOX / "ch05" / "relay")[0])
after = LANDING / "relay" / "2026-01-04" / "accounts.csv"
for label, p in (("2024-02-29", before), ("2026-01-04", after)):
    b = p.read_bytes()
    print(f"\n{label}  starts {b[:4].hex(' ')}  "
          f"CRLF {b.count(b'\r\n'):,}  LF {b.count(b'\n'):,}")

# Today's file opened as plain utf-8 keeps the byte-order mark.
for enc in ("utf-8", "utf-8-sig"):
    with open(after, encoding=enc, newline="") as f:
        first = next(csv.reader(f))[0]
    print(f"{enc:<9}  first column {first!r}")

# The old file split on \n alone keeps a \r on every last field.
with open(before, encoding="cp1252", newline="") as f:
    row = f.read().split("\n")[1].split(";")
print(f"last field split on \\n   {row[-1]!r}")

# A wrong guess may fail, or may not.
b = after.read_bytes()
try:
    b.decode("cp1252")
except UnicodeDecodeError as e:
    print(f"today's file as cp1252:  fails at byte {b[e.start]:#x}")
text = b.decode("latin-1")
print(f"today's file as latin-1: no error, {text.count('Ã'):,} x 'Ã'")
