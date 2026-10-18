# The graph dbt reads from ref() and source(), set beside the one the
# runner reads from the same calls; then a ref to a model that is not
# there, which dbt refuses before it builds anything.
# timeout: 900
import json
import shutil
from collections import Counter
from pathlib import Path

from _dbt import HERE, PROJECT, chapter_14, dbt

from groundwork.transform import runner

manifest = dbt("parse").result
ids = {n.unique_id: n.name for n in [*manifest.nodes.values(),
                                     *manifest.sources.values()]}
dbt_refs, dbt_srcs, kinds, schema = set(), set(), {}, {}
for n in manifest.nodes.values():
    if n.resource_type not in ("model", "seed", "snapshot") \
            or not chapter_14(n):
        continue
    kinds[n.name], schema[n.name] = n.resource_type, n.schema
    for up in getattr(n.depends_on, "nodes", []):
        if up.startswith("source."):
            s = manifest.sources[up]
            dbt_srcs.add((f"{s.source_name}.{s.name}", n.name))
        else:
            dbt_refs.add((ids[up], n.name))

mine = runner.discover()
run_refs = {(r, m.name) for m in mine.values() for r in m.refs}
run_srcs = {(f"{s}.{t}", m.name) for m in mine.values()
            for s, t in m.sources}
print(f"{'':<16}{'runner':>7}{'dbt':>6}{'in both':>9}")
print(f"{'nodes':<16}{len(mine):>7}{len(kinds):>6}"
      f"{len(set(mine) & set(kinds)):>9}")
for what, a, b in (("ref() edges", run_refs, dbt_refs),
                   ("source() edges", run_srcs, dbt_srcs)):
    print(f"{what:<16}{len(a):>7}{len(b):>6}{len(a & b):>9}")
for name in sorted(set(kinds) - set(mine)):
    ups = sorted(u for u, d in dbt_refs | dbt_srcs if d == name)
    print(f"only in dbt: {name} ({kinds[name]}),\n  "
          f"reads {', '.join(ups) or 'nothing'}")

# Levels: a node's level is one more than the deepest thing it reads.
level = {}


def depth(name):
    if name not in level:
        ups = [u for u, d in dbt_refs if d == name]
        level[name] = 1 + max((depth(u) for u in ups), default=-1)
    return level[name]


for name in kinds:
    depth(name)
print("\nlevel  nodes  by schema")
for k in range(max(level.values()) + 1):
    names = sorted(n for n in level if level[n] == k)
    by = Counter(schema[n] for n in names)
    print(f"{k:>5}{len(names):>7}  " + ", ".join(
        f"{by[s]} {s}" for s in ("seeds", "staging", "intermediate",
                                 "marts", "snapshots") if by[s]))

Path(__file__).with_suffix(".json").write_text(json.dumps({
    "kinds": kinds, "levels": level, "schema": schema,
    "refs": sorted(dbt_refs), "sources": sorted(dbt_srcs)}, indent=1))

# A typo, in a copy of the project: dbt parses every model first.
copy = HERE / "project"
shutil.rmtree(copy, ignore_errors=True)
shutil.copytree(PROJECT, copy, ignore=shutil.ignore_patterns(
    "target", "logs"))
(copy / "models" / "staging" / "stg_typo.sql").write_text(
    "SELECT * FROM {{ ref('stg_catalogue__product') }}\n")
res = dbt("parse", project=copy, check=False)
print(f"\nwith a typo, dbt parse succeeded: {res.success}")
message = " ".join(str(res.exception).split())
start = message.index("Model 'model.")
print("  " + message[start:start + 140].replace(
    " which", "\n  which").replace(" depends", "\n  depends"))
shutil.rmtree(copy)
