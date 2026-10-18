# The one-line change, built beside main from the same raw layer: what
# every data test and every number people read say about it.
# timeout: 900
import difflib
import json

import duckdb
from _sandbox import CHANGE, PROJECT, built

main, ran_main = built("main")
branch, ran_branch = built("branch", CHANGE)

file = CHANGE[0]
was = (PROJECT / file).read_text().splitlines()
now = (branch.parent / "project" / file).read_text().splitlines()
print(f"the change, dbt/groundwork/{file}:")
for line in difflib.unified_diff(was, now, lineterm="", n=0):
    if line[:1] in "+-" and line[:3] not in ("+++", "---"):
        text = " ".join(line[1:].split())
        head, _, alias = text.rpartition(" AS ")
        print(f"  {line[0]} {head}" + (f" AS {alias}" if len(text) < 62
                                       else f"\n      AS {alias}"))

print("\ndbt build --full-refresh, each into its own copy:")
for who, ran in (("main", ran_main), ("branch", ran_branch)):
    print(f"  {who:<7} {ran.get('success', 0)} built, "
          f"{ran.get('pass', 0)} tests passed, {ran.get('fail', 0)} "
          f"failed, {ran.get('warn', 0)} warned, "
          f"{ran.get('error', 0)} errors")

ASK = {
    "rows in the ten marts": """
        SELECT sum(n) FROM (
          SELECT count(*) AS n FROM marts.dim_date UNION ALL
          SELECT count(*) FROM marts.dim_region UNION ALL
          SELECT count(*) FROM marts.dim_supplier UNION ALL
          SELECT count(*) FROM marts.dim_product UNION ALL
          SELECT count(*) FROM marts.dim_account UNION ALL
          SELECT count(*) FROM marts.fct_order_lines UNION ALL
          SELECT count(*) FROM marts.fct_orders UNION ALL
          SELECT count(*) FROM marts.fct_tickets UNION ALL
          SELECT count(*) FROM marts.fct_contracts UNION ALL
          SELECT count(*) FROM marts.fct_contract_month)""",
    "revenue, 2025": """
        SELECT sum(revenue) FROM marts.fct_order_lines
        WHERE order_date BETWEEN '2025-01-01' AND '2025-12-31'""",
    "contracts in force, 31 Dec 2025": """
        SELECT sum(contracts_in_force) FROM marts.fct_contract_month
        WHERE month_end = '2025-12-31'""",
    "contracts not renewed, 2025": """
        SELECT count(*) FROM marts.fct_contracts
        WHERE outcome = 'not_renewed' AND end_date >= '2025-01-01'
          AND end_date <= '2025-12-31'""",
}
print(f"\n{'':<34}{'main':>16}{'branch':>16}")
same = 0
for label, sql in ASK.items():
    got = []
    for path in (main, branch):
        con = duckdb.connect(str(path), read_only=True)
        (v,) = con.execute(sql).fetchone()
        con.close()
        got.append(v)
    same += got[0] == got[1]
    print(f"  {label:<32}{got[0]:>16,}{got[1]:>16,}")
print(f"\nthe same in both: {same} of {len(ASK)}")
with open("code/23/01_green_build.json", "w") as f:
    json.dump({"main": ran_main, "branch": ran_branch}, f)
