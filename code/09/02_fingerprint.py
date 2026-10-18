# A file's name, size and time describe it; its SHA-256 identifies it.
import hashlib
import os
import shutil

from groundwork.config import LANDING, NOW, SANDBOX
from groundwork.ingest.files import arrived_at, fingerprint

here = SANDBOX / "ch09" / "fingerprint"
shutil.rmtree(here, ignore_errors=True)
here.mkdir(parents=True)
aldridge = LANDING / "suppliers" / "aldridge-co"


def show(label, path):
    print(f"  {label:<24} {path.stat().st_size:>6,} bytes  "
          f"{fingerprint(path)[:16]}")


print("Two names, the same size, different contents:")
first = aldridge / "Aldridge Trade Prices Mar 2024.xlsx"
again = aldridge / "Aldridge Trade Prices Mar 2024 (corrected).xlsx"
show("Mar 2024", first)
show("Mar 2024 (corrected)", again)

print("\nFourteen nights of Relay, fewer distinct files:")
for table in ("accounts", "contracts"):
    nights = sorted((LANDING / "relay").glob(f"20*/{table}.csv"))
    distinct = {fingerprint(p) for p in nights}
    print(f"  {table + '.csv':<14} {len(nights)} nights, "
          f"{len(distinct)} distinct fingerprints")

print("\nA copy, a new time, one changed byte:")
copy = here / first.name
shutil.copy2(first, copy)                # keeps the arrival time
show("copied", copy)
t = NOW.timestamp()
os.utime(copy, (t, t))                   # "touched": a new time
print(f"  {'touched':<24} {arrived_at(copy):%Y-%m-%d %H:%M}  "
      f"{fingerprint(copy)[:16]}")
data = bytearray(copy.read_bytes())
data[-100] ^= 1                          # flip one bit
copy.write_bytes(data)
show("one bit flipped", copy)

# Every empty file has the same fingerprint.
print(f"\nSHA-256 of an empty file: "
      f"{hashlib.sha256(b'').hexdigest()[:16]}")
shutil.rmtree(here)
