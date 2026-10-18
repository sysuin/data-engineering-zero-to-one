# Exercise 2: for each ingest task, the marts its failure need not
# stop, from the table lineage.
from _sandbox import compiled

from groundwork.observe import lineage, nightly

manifest, _ = compiled()
dag = nightly()
g = lineage.tables(dag, manifest)
marts = sorted(n for n, k in g.kinds.items() if k == "marts")
print(f"{'if this fails':<18}{'marts still sound':>18}"
      f"  of {len(marts)}")
for task in sorted(t for t in dag.tasks if t.startswith("ingest_")
                   or t == "load_comments"):
    hit = g.downstream(f"task:{task}")
    sound = [m for m in marts if m not in hit]
    print(f"{task:<18}{len(sound):>18}")
hit = g.downstream("task:ingest_suppliers")
print("\nwhen ingest_suppliers fails, still sound:")
for m in marts:
    if m not in hit:
        print(f"  {m}")
