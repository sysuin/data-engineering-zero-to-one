# Every relation dbt built, set beside the one Chapter 11's runner
# builds from the same sources: the same columns and types, and the
# same rows, EXCEPT ALL both ways. Then Chapters 12 and 13's checks,
# run over dbt's marts.
# timeout: 1800
from _dbt import HERE, built, connect, copy, results, tidy

from groundwork import dimensions, facts
from groundwork.transform import runner
from groundwork.transform.__main__ import transform

ran, problems, _ = transform(copy("runner"), full=True)
print(f"runner, --full: {sum(r.status == 'built' for r in ran)} of "
      f"{len(ran)} built; its checks: {len(problems)} problems")
done = results()
nodes = [d for d in done if d[0] != "test"]
tests = [d for d in done if d[0] == "test"]
print(f"dbt build --full-refresh: "
      f"{sum(d[3] == 'success' for d in nodes)} of {len(nodes)} built, "
      f"{sum(d[3] == 'pass' for d in tests)} of {len(tests)} tests "
      f"passed")

con = connect(built())
con.execute(f"ATTACH '{HERE / 'runner' / 'warehouse.duckdb'}' "
            "AS runner (READ_ONLY)")
n = lambda sql: con.execute(sql).fetchone()[0]           # noqa: E731
COLS = """SELECT column_name, data_type FROM information_schema.columns
          WHERE table_catalog = '{db}' AND table_schema = '{s}'
            AND table_name = '{t}' ORDER BY ordinal_position"""
LAYERS = ["seeds", "staging", "intermediate", "marts"]
relations = [(m.schema, m.name) for m in runner.discover().values()]
relations.append(("marts", "dim_date"))   # Python's, now a model
relations.sort(key=lambda r: (LAYERS.index(r[0]), r[1]))

print(f"\n{'':<20}{'rows':>10}{'columns':>9}{'only dbt':>9}"
      f"{'only runner':>12}")
layers = {}
for schema, name in relations:
    ours, theirs = f"{schema}.{name}", f"runner.{schema}.{name}"
    cols = [con.execute(COLS.format(db=db, s=schema, t=name)).fetchall()
            for db in ("warehouse", "runner")]
    same = cols[0] == cols[1]
    rows = n(f"SELECT count(*) FROM {ours}")
    a = n(f"SELECT count(*) FROM (FROM {ours} EXCEPT ALL "
          f"FROM {theirs})")
    b = n(f"SELECT count(*) FROM (FROM {theirs} EXCEPT ALL "
          f"FROM {ours})")
    total = layers.setdefault(schema, [0, 0, 0, 0, 0])
    for i, v in enumerate((1, rows, same, a, b)):
        total[i] += v
    if schema == "marts":
        print(f"{name:<20}{rows:>10,}{'same' if same else 'differ':>9}"
              f"{a:>9}{b:>12}")
print()
for schema, (k, rows, same, a, b) in layers.items():
    if schema != "marts":
        print(f"{f'{schema} ({k})':<20}{rows:>10,}"
              f"{f'{same} same':>9}{a:>9}{b:>12}")

con.execute("DETACH runner")
problems = dimensions.check(con) + facts.check(con)
print(f"\nChapters 12-13's checks on dbt's marts: "
      f"{'every promise kept' if not problems else problems}")
con.close()
tidy("runner")
