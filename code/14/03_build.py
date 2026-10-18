# dbt build: every seed, model, snapshot and test in one run, in the
# graph's order; then what a one-character change would rebuild.
# timeout: 1200
import shutil
from collections import Counter

from _dbt import HERE, NIGHT, PROJECT, SHARED, connect, dbt, results

done = results()                      # dbt build --full-refresh
ORDER = ["seeds", "staging", "intermediate", "marts", "snapshots", ""]
kinds = Counter((k, s if k != "test" else "") for k, s, _, _ in done)
status = Counter((k, s if k != "test" else "", st)
                 for k, s, _, st in done)
print(f"{'kind':<10}{'schema':<14}{'nodes':>6}  status")
for (k, s), n in sorted(kinds.items(),
                       key=lambda x: ORDER.index(x[0][1])):
    print(f"{k:<10}{s:<14}{n:>6}  " + ", ".join(
        f"{st} {m}" for (k2, s2, st), m in status.items()
        if (k2, s2) == (k, s)))
where = {name: i + 1 for i, (_, _, name, _) in enumerate(done)}
first = min((i, name) for name, i in where.items()
            if done[i - 1][0] == "test")
last = max((i, name) for name, i in where.items()
           if done[i - 1][0] == "model")
print(f"\nnode {where['dim_date']:>2} of {len(done)}: model dim_date")
print(f"node {first[0]:>2}: the first test, {first[1]}")
print(f"node {last[0]:>2}: the last model, {last[1]}")

con = connect(SHARED / "warehouse.duckdb")
print("\nrows in the marts it built:")
for _, schema, name, _ in done:
    if schema == "marts":
        n = con.execute(f"SELECT count(*) FROM marts.{name}")\
            .fetchone()[0]
        print(f"  {name:<22}{n:>10,}")
con.close()

# One space added to a comment, in a copy of the project.
copy = HERE / "project"
shutil.rmtree(copy, ignore_errors=True)
shutil.copytree(PROJECT, copy, ignore=shutil.ignore_patterns(
    "target", "logs"))
model = copy / "models" / "staging" / "stg_catalogue__products.sql"
model.write_text(model.read_text().replace("-- force.", "-- force. "))
res = dbt("ls", "--select", "state:modified+", "--state",
          str(SHARED / "state"), "--resource-type", "model",
          "--output", "name", project=copy, vars=NIGHT)
print(f"\nstate:modified+ after a space in a comment: "
      f"{len(res.result)}")
names = sorted(res.result)
print("  " + ", ".join(names[:3]) + ",\n  " + ", ".join(names[3:]))
shutil.rmtree(copy)
