# What is in the landing zone, who put it there, and when it arrived.
import json
import re
from collections import defaultdict
from pathlib import Path

from groundwork.config import LANDING
from groundwork.ingest.files import discover
from meridian.sources import landing

everything = discover(LANDING)           # every file, oldest first


def folder(name):
    """The part of the landing zone a file belongs to."""
    parts = name.split("/")
    if parts[0] == "relay":
        return ("relay/audit" if parts[1] == "audit" else
                "relay/<night>" if parts[1][:2] == "20" else name)
    return parts[0]


groups = defaultdict(list)
for a in everything:
    groups[folder(a.name)].append(a)
print(f"{'folder':<23} {'files':>6} {'MB':>6}  first and last "
      "arrival (UTC)")
for g, got in sorted(groups.items()):
    mb = sum(a.size for a in got) / 1e6
    print(f"{g:<23} {len(got):>6,} {mb:>6.1f}  "
          f"{got[0].arrived_at:%Y-%m-%d}  "
          f"{got[-1].arrived_at:%Y-%m-%d %H:%M}")

# How each supplier names its files: the name with the dates taken out.
shapes = defaultdict(set)
for a in groups["suppliers"]:
    who, name = a.name.split("/")[1:]
    shape = re.sub(r"\d{4}-\d{2}|\d{6,8}|[A-Z][a-z]{2} \d{4}", "<date>",
                   name)
    shapes[shape.replace(who, "<supplier>")].add(who)
print(f"\nsupplier file names, {len(shapes)} patterns:")
for shape, who in sorted(shapes.items(), key=lambda kv: -len(kv[1])):
    print(f"  {len(who):>2}  {shape}")

# What a run at 06:00 on 5 March 2024 could have seen.
seen = landing.arrived(by="2024-03-05 06:00", under="suppliers")
march = [p for p in seen if re.search(r"2024-03|202403|Mar 2024",
                                      p.name)]
print(f"\nby 2024-03-05 06:00: {len(seen)} supplier files, "
      f"{len(march)} of them for March 2024")

Path(__file__).with_suffix(".json").write_text(json.dumps(
    {g: {"files": len(v), "bytes": sum(a.size for a in v)}
     for g, v in sorted(groups.items())}, indent=1))
