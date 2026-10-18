# The suppliers' folders: how many layouts the newest files arrive in.
import csv
from datetime import datetime, timezone

from groundwork.config import LANDING, REFERENCE

folders = sorted(p for p in (LANDING / "suppliers").iterdir())
files = [f for d in folders for f in d.iterdir()]
print(f"{len(folders)} supplier folders")

with open(REFERENCE / "suppliers.csv", encoding="utf-8") as f:
    master = [row["name"] for row in csv.DictReader(f)]
print(f"procurement's master lists {len(master)} suppliers")

# The newest arrival in each folder, and the first line it starts with.
layouts: dict[str, list[str]] = {}
for d in folders:
    newest = max(d.iterdir(), key=lambda f: f.stat().st_mtime)
    if newest.suffix == ".xlsx":
        first = "(a spreadsheet, .xlsx)"
    else:
        first = newest.read_text(encoding="utf-8").splitlines()[0]
    layouts.setdefault(first, []).append(d.name)

when = datetime.fromtimestamp(max(f.stat().st_mtime for f in files),
                              timezone.utc)
print(f"newest file arrived {when:%Y-%m-%d %H:%M} UTC")
print(f"\n{len(layouts)} layouts among the newest files")
for first, names in sorted(layouts.items(), key=lambda kv: -len(kv[1])):
    who = f"{len(names)} suppliers" if len(names) > 1 else names[0]
    print(f"  {who}\n    {first}")
