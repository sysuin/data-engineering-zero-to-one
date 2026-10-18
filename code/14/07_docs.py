# dbt docs generate, without the web server: what it wrote, how much
# of the project is described, and fct_order_lines' lineage read out
# of manifest.json; then one compiled line the docs site shows.
# timeout: 1200
import json

from _dbt import PROJECT, built, chapter_14, copy, dbt, tidy

path = copy("docs", source=built())
dbt("docs", "generate", path=path)
target = PROJECT / "target"
print("written to target/:", ", ".join(
    f for f in ("index.html", "manifest.json", "catalog.json")
    if (target / f).exists()))

manifest = json.loads((target / "manifest.json").read_text())
catalog = json.loads((target / "catalog.json").read_text())
nodes = {**manifest["nodes"], **manifest["sources"]}
models = [n for n in nodes.values() if n["resource_type"] == "model"]
print(f"models described: "
      f"{sum(1 for n in models if n['description'])} of {len(models)}; "
      f"relations in the catalogue: {len(catalog['nodes'])}")


def name(uid):
    n = nodes[uid]
    return (f"{n['source_name']}.{n['name']}"
            if n["resource_type"] == "source" else n["name"])


parents, children = manifest["parent_map"], manifest["child_map"]
FACT = "model.groundwork.fct_order_lines"
level = {}


def depth(uid):
    if uid not in level:
        level[uid] = 1 + max((depth(p) for p in parents.get(uid, [])),
                             default=-1)
    return level[uid]


depth(FACT)
print(f"\nupstream of fct_order_lines: {len(level) - 1} nodes")
for k in range(max(level.values()) + 1):
    names = sorted(name(u) for u in level if level[u] == k)
    kinds = sorted({nodes[u]["resource_type"] for u in level
                    if level[u] == k})
    print(f"  level {k}: {len(names):>2} {'/'.join(kinds)}")
down = children[FACT]
models_down = sorted(name(u) for u in down if u.startswith("model."))
tests_down = [u for u in down
              if u.startswith("test.") and chapter_14(nodes[u])]
print(f"downstream: {', '.join(models_down)}, "
      f"and {len(tests_down)} tests")

edges = sorted({(name(p), name(c)) for c in level
                for p in parents.get(c, [])})
edges += sorted((name(FACT), m) for m in models_down)
up = {name(u): [nodes[u]["resource_type"], level[u]] for u in level}
open(__file__.replace(".py", ".json"), "w").write(json.dumps(
    {"upstream": up, "downstream": {m: "model" for m in models_down},
     "tests": len(tests_down),
     "edges": edges}, indent=1))

node = manifest["nodes"]["model.groundwork.dim_region"]
raw = [ln.strip() for ln in node["raw_code"].splitlines()
       if "AS region_key" in ln][0]
code = node["compiled_code"]
end = code.index("AS region_key")
compiled = " ".join(code[code.rindex("SELECT", 0, end) + 6:end].split())
print(f"\ndim_region, as written:\n  {raw}")
print(f"compiled:\n  {compiled} AS region_key")
tidy("docs")
