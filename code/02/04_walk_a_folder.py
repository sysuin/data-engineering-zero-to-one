# Walk the landing zone, then copy one night's files and prove the copy.
import hashlib
import shutil
from datetime import datetime, timezone

from groundwork.config import LANDING, SANDBOX

print(f"{'folder':10} {'files':>6} {'KB':>7}  newest arrival (UTC)")
for folder in sorted(p for p in LANDING.iterdir() if p.is_dir()):
    files = [p for p in folder.rglob("*") if p.is_file()]
    size = sum(p.stat().st_size for p in files)
    newest = max(p.stat().st_mtime for p in files)
    when = datetime.fromtimestamp(newest, tz=timezone.utc)
    print(f"{folder.name:10} {len(files):>6,} {size / 1000:>7,.0f}  "
          f"{when:%Y-%m-%d %H:%M}")


def sha256(path):
    """A fingerprint of the file's bytes: equal only if they are."""
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


night = LANDING / "relay" / "2026-01-04"
dest = SANDBOX / "ch02" / "landing" / "relay" / night.name
dest.mkdir(parents=True, exist_ok=True)
print(f"\ncopying relay/{night.name}/ to the sandbox")
for src in sorted(night.glob("*.csv")):
    copy = shutil.copy2(src, dest / src.name)   # copy2 keeps the time
    same = sha256(src) == sha256(copy)
    print(f"  {src.name:13} {src.stat().st_size:>10,} bytes  "
          f"identical: {same}")
