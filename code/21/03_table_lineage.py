# Table-level lineage: the nightly DAG's tasks joined to dbt's
# manifest, from a supplier's price list to the marts it reaches.
from collections import Counter

from _sandbox import compiled

from groundwork.observe import lineage, nightly

manifest, _ = compiled()
dag = nightly()
g = lineage.tables(dag, manifest)

kinds = Counter(g.kinds.values())
hows = Counter(h for _, _, h in g.edges)
parts = [f"{n} {k}" for k, n in sorted(kinds.items(),
                                       key=lambda x: (-x[1], x[0]))]
print(f"{len(g.kinds)} nodes: " + ", ".join(parts[:5]) + ",\n  "
      + ", ".join(parts[5:]))
print(f"{len(g.edges)} edges: " + ", ".join(
    f"{n} {h}" for h, n in sorted(hows.items(),
                                  key=lambda x: (-x[1], x[0]))))

start = "suppliers.price_lists"
print(f"\nfrom {start}, one level at a time:")
level, seen = {start}, {start}
while level:
    nxt = set()
    for n in level:
        nxt |= {c for c in g.children(n) if not c.startswith("test:")}
    nxt -= seen
    if nxt:
        print("  " + ", ".join(sorted(nxt)))
    seen |= nxt
    level = nxt
down = g.downstream(start)
tests = [n for n in down if n.startswith("test:")]
print(f"  and {len(tests)} dbt tests on those relations")

marts = sorted(n for n, k in g.kinds.items() if k == "marts")
reached = [m for m in marts if m in down]
print(f"\nmarts: {len(marts)}; built from the price lists: "
      f"{len(reached)}")
rest = [m.split(".")[1] for m in marts if m not in down]
print(f"  not: {', '.join(rest[:4])},\n       {', '.join(rest[4:])}")
stopped = sorted(dag.downstream(["ingest_suppliers"]))
print("tasks the DAG stops when ingest_suppliers fails: "
      f"{len(stopped)}")
print(f"  {', '.join(stopped[:4])},\n  {', '.join(stopped[4:])}")
