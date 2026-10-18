# Column-level lineage from the models' compiled SQL, parsed with
# sqlglot: where revenue comes from, and what chooses its rows.
import json
from pathlib import Path

from _sandbox import compiled

from groundwork.observe import lineage

manifest, schema = compiled()
g = lineage.columns(manifest, schema)
value = [e for e in g.edges if e[2] == "value"]
rows = [e for e in g.edges if e[2] == "filter"]
print(f"{len(lineage.models(manifest))} models and snapshots; "
      f"{len(value)} value edges, {len(rows)} filter edges")
print(f"columns sqlglot could not follow: {len(g.lost)}, all in\n  "
      f"{', '.join(sorted({c.rsplit('.', 1)[0] for c in g.lost}))}")

col = "marts.fct_order_lines.revenue"
print(f"\n{col} is computed from:")
level, steps = {col}, []
while True:
    up = set().union(*(g.parents(c, ("value",)) for c in level))
    if not up:
        break
    rels = {}
    for c in sorted(up):
        r, name = c.rsplit(".", 1)
        rels.setdefault(r, []).append(name)
    for r, names in rels.items():
        print(f"  <- {r}: {', '.join(names)}")
    steps.append(sorted(up))
    level = up

chain = {c.rsplit(".", 1)[0] for s in steps for c in s} | {
    col.rsplit(".", 1)[0]}
print("\nand its rows are chosen by:")
filters = []
for r in sorted(chain):
    for f in sorted(g.parents(r, ("filter",))):
        filters.append((f, r))
        print(f"  {f}\n    in {r}")
src = [f for f, _ in filters if f.startswith("staging.")]
for f in src:
    print(f"  ... which is {sorted(g.upstream(f, ('value',)))[-1]}")

down = sorted(g.downstream(col, ("value",)))
print(f"\nbuilt from it: {', '.join(down)}")

Path(__file__).with_suffix(".json").write_text(json.dumps({
    "column": col, "steps": steps, "filters": filters,
    "filter_sources": {f: sorted(g.upstream(f, ("value",)))
                       for f in src},
    "down": down}, indent=1))
