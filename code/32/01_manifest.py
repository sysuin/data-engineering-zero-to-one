# What the source generator wrote, from its manifest.
import json

from groundwork.config import SOURCES

m = json.loads((SOURCES / "manifest.json").read_text())
print(f"today {m['today']}, sources stand at {m['now_utc']}")
print(f"generated from Book 2's warehouse "
      f"{m['book2_warehouse_sha256'][:12]}...")
print()
for key, value in m["counts"].items():
    print(f"  {key:<26}{value:>12,}")
print()
print(f"planted problems: {len(m['problems'])}")
