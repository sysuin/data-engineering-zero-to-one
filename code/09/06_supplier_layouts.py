# Sixteen suppliers' March 2024 lists: four layouts, one raw shape.
import json
import textwrap
from collections import defaultdict
from pathlib import Path

from groundwork.config import LANDING
from groundwork.ingest import suppliers
from groundwork.ingest.files import discover

march = {}                               # supplier -> first March list
for a in discover(LANDING, "suppliers"):
    p = suppliers.read(a.path.read_bytes(), a)
    if p.logical_key.endswith("/2024-03"):
        march.setdefault(p.logical_key.split("/")[0], (a, p))

by_layout = defaultdict(list)
for who, (a, p) in sorted(march.items()):
    by_layout[p.note].append((who, a, p))

shown = {}
for layout, members in sorted(by_layout.items(),
                              key=lambda kv: -len(kv[1])):
    who, a, p = members[0]
    if a.path.suffix == ".xlsx":
        first = "(spreadsheet) Product Code | Description | Trade Price"
    else:
        first = a.path.read_text(encoding="utf-8-sig").splitlines()[0]
    rows = sum(len(q.rows) for _, _, q in members)
    print(f"{layout}: {len(members)} supplier(s), {rows} rows")
    print(f"  {first}")
    r = p.rows[0]
    print(f"  -> {r['sku']} | {r['description']} | {r['cost']} "
          f"{r['cost_unit']} | {r['effective_from']}")
    shown[layout] = {"suppliers": [m[0] for m in members],
                     "header": first, "row": r, "file": a.name}

total = sum(len(p.rows) for _, p in march.values())
print(f"\n{len(march)} lists for March 2024, {total} rows, in columns")
columns = ", ".join(march["voss-industrial"][1].rows[0])
print(textwrap.fill(columns, 64, initial_indent="  ",
                    subsequent_indent="  "))
Path(__file__).with_suffix(".json").write_text(
    json.dumps(shown, indent=1))
