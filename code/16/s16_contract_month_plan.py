# Exercise 1: fct_contract_month's plan, with rows beside estimates.
from _plans import analyze, connect, model, name, operators

con = connect()
profile = analyze(con, model("fct_contract_month"))
print(f"{'operator, top down':<36}{'rows out':>11}{'estimated':>11}")
worst = None
for _, node in operators(profile["children"][0]["children"][0]):
    if name(node) in ("PROJECTION", "CTE", "CTE_SCAN"):
        continue
    rows = node["operator_cardinality"]
    est = int(node["extra_info"].get("Estimated Cardinality", 0))
    table = node["extra_info"].get("Table", "").split(".")[-1]
    label = f"{name(node)} {table}"
    print(f"{label:<36}{rows:>11,}{f'{est:,}' if est else '':>11}"
          .rstrip())
    if est and rows and (worst is None or
                         max(est / rows, rows / est) > worst[0]):
        worst = (max(est / rows, rows / est), name(node), est, rows)
print(f"furthest estimate: {worst[1]}, {worst[2]:,} for {worst[3]:,}")
