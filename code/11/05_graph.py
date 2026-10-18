# The dependency graph, read from the SQL: every model, seed and source,
# who refs whom, and the order the runner builds them in.
import dataclasses
import json
import textwrap
from pathlib import Path

from groundwork.transform import runner

from _pack import sandbox, tidy

warehouse, folder = sandbox("graph")
models = runner.discover(folder)
kinds = {}
for m in models.values():
    kinds.setdefault(m.schema, []).append(m.name)
sources = sorted({f"{s}.{t}" for m in models.values()
                  for s, t in m.sources})
edges = sorted((r, m.name) for m in models.values() for r in m.refs)
print(", ".join(f"{len(v)} in {k}" for k, v in sorted(kinds.items())),
      f"| {len(sources)} sources, {len(edges)} refs")

pack = set()
for name in ("pack_channel", "pack_category", "pack_supplier",
             "pack_realisation"):
    stack = [name]
    while stack:
        n = stack.pop()
        pack.add(n)
        stack += models[n].refs
level = {}
for n in runner.order(models):
    level[n] = 1 + max((level[r] for r in models[n].refs), default=-1)
print("\nThe sales pack's part of the graph, in build order:")
for n in runner.order(models):
    if n in pack:
        print(f"  {level[n]}  {n}")
        if models[n].refs:
            print(textwrap.fill("<- " + ", ".join(models[n].refs), 66,
                                initial_indent=" " * 7,
                                subsequent_indent=" " * 10))

bad = dict(models)
m = models["int_pack__lines"]
bad[m.name] = dataclasses.replace(m, refs=m.refs + ("pack_channel",))
try:
    runner.order(bad)
except ValueError as e:
    print(f"\nint_pack__lines made to ref pack_channel:\n  {e}")

Path(__file__).with_suffix(".json").write_text(json.dumps(dict(
    nodes=[[n, models[n].schema, models[n].materialised, level[n],
            [f"{s}.{t}" for s, t in models[n].sources]]
           for n in runner.order(models) if n in pack],
    edges=[e for e in edges if e[1] in pack]), indent=1))
tidy("graph")
