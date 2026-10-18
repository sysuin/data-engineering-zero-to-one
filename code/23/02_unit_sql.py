# A unit test of the dbt model, run on main's project and the branch's;
# then the two tests Groundwork already had, run on the branch.
import json
import shutil
import sys

import duckdb
from _sandbox import CHANGE, HERE, project, tidy

from groundwork import testing

sys.path.insert(0, "tests")
import test_dbt_project  # noqa: E402
import test_sql_models  # noqa: E402
import test_staging  # noqa: E402

main, branch = project("unit-main"), project("unit-branch", CHANGE)


def outcome(test, *args) -> str:
    try:
        test(*args)
    except AssertionError:
        return "failed"
    return "passed"


print("tests/test_sql_models.py::\n"
      "  test_a_legacy_contract_keeps_no_discount")
rows = {}
for who, folder in (("main", main), ("branch", branch)):
    testing.PROJECT = folder              # the project the test reads
    con = duckdb.connect()
    test = test_sql_models.test_a_legacy_contract_keeps_no_discount
    said = outcome(test, con)
    rows[who] = testing.run_model(
        con, "stg_relay__contracts", order_by="contract_id",
        select="contract_id, is_legacy_terms, discount_pct")
    con.close()
    print(f"  on {who}'s model: {said}")
print(f"  {'contract':>10}{'legacy terms':>14}{'discount: main':>16}"
      f"{'branch':>8}")
for (c, legacy, d), (_, _, e) in zip(rows["main"], rows["branch"]):
    print(f"  {c:>10}{str(legacy):>14}{str(d):>16}{str(e):>8}")

print("\nGroundwork's earlier tests of the same model, on the branch:")
test_dbt_project.PROJECT = branch
tmp = HERE / "unit-tmp"
shutil.rmtree(tmp, ignore_errors=True)
tmp.mkdir()
for module, test, args in (
        (test_staging, "test_relay_contracts_read_both_vocabularies",
         (tmp,)),
        (test_dbt_project, "test_staging_and_intermediate_sql_is_"
         "unchanged_but_for_config", ())):
    said = outcome(getattr(module, test), *args)
    print(f"  {module.__name__}.py::\n    {test}\n    {said}")
model = testing.render("stg_relay__contracts", branch)
runner = open("code/groundwork/models/staging/stg_relay__contracts.sql")
print("\nfills the discount with coalesce():")
print(f"  the runner's copy, which test_staging runs: "
      f"{'coalesce(' in runner.read()}")
print(f"  dbt's, on the branch, which the night runs: "
      f"{'coalesce(' in model}")
runner.close()
tidy("unit-main", "unit-branch", "unit-tmp")
with open("code/23/02_unit_sql.json", "w") as f:
    json.dump(rows, f, default=str)
