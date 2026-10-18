# What dbt finds in Groundwork's project, beside what the runner finds
# in its models folder; then one staging model, ported and built.
# timeout: 900
import difflib
from collections import Counter

from _dbt import PROJECT, chapter_14, connect, copy, dbt, tidy

from groundwork.transform import runner

path = copy("first")
ok = dbt("debug", path=path).result
print("dbt debug:", "all checks passed" if ok else "failed")

manifest = dbt("parse", path=path).result
ours = [n for n in manifest.nodes.values()       # not Chapter 15's
        if n.package_name == "groundwork" and chapter_14(n)]
found = Counter(n.schema if n.resource_type in ("model", "seed")
                else n.resource_type for n in ours)
mine = runner.discover()
had = Counter(m.schema for m in mine.values())
had["sources"] = len({s for m in mine.values() for s in m.sources})
found["sources"] = sum(map(chapter_14, manifest.sources.values()))
print(f"\n{'':<14}{'runner':>7}{'dbt':>6}")
for kind in ("seeds", "staging", "intermediate", "marts", "sources",
             "snapshot", "test"):
    print(f"{kind:<14}{had[kind]:>7}{found[kind]:>6}")

name = "stg_relay__accounts"
before = mine[name].path.read_text().splitlines()
after = (PROJECT / "models" / "staging" / f"{name}.sql") \
    .read_text().splitlines()
print(f"\n{name}.sql, the runner's file against dbt's:")
print(f"  lines only in the runner's: "
      f"{[ln for ln in before if ln not in after]}")
print(f"  lines only in dbt's:        "
      f"{[ln for ln in after if ln not in before]}")

res = dbt("run", "--select", name, path=path)
node = res.result.results[0].node
compiled = node.compiled_code.splitlines()
mine_sql = runner.compile_sql(mine[name], mine)
print("\nCompiled, the lines where the two differ:")
for d in difflib.ndiff(mine_sql.splitlines(), compiled):
    line = d[2:].strip()
    if d[0] in "-+" and line and not line.startswith("--"):
        print(f"  {'runner' if d[0] == '-' else 'dbt   '} {line}")
print(f"dbt run: {res.result.results[0].status}, "
      f"{node.config.materialized} {node.schema}.{name}")

con = connect(path)
n = lambda sql: con.execute(sql).fetchone()[0]           # noqa: E731
rows = n(f"SELECT count(*) FROM staging.{name}")
extra = n(f"SELECT count(*) FROM (FROM staging.{name} "
          f"EXCEPT ALL ({mine_sql}))")
missing = n(f"SELECT count(*) FROM (({mine_sql}) "
            f"EXCEPT ALL FROM staging.{name})")
print(f"rows {rows:,}; only in dbt's {extra}; only in the runner's "
      f"{missing}")
con.close()
tidy("first")
