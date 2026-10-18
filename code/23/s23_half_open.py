# Exercise 1: fct_contract_month's join made half-open, built beside
# main and diffed; what the tests and Chapter 22's metric say.
# timeout: 900
import sys

import duckdb
from _sandbox import built, tidy

from groundwork import diff, testing

sys.path.insert(0, "tests")
import test_sql_models  # noqa: E402

EDIT = ("models/marts/fct_contract_month.sql",
        "ON m.month_end BETWEEN c.start_date AND c.end_date",
        "ON m.month_end >= c.start_date AND m.month_end < c.end_date")
main, _ = built("main")
half, ran = built("half-open", EDIT)
print(f"dbt build: {ran.get('success', 0)} built, {ran.get('pass', 0)}"
      f" tests passed, {ran.get('fail', 0)} failed")
testing.PROJECT = half.parent / "project"
unit = test_sql_models
try:
    unit.test_a_contract_is_in_force_on_each_month_end_it_covers(
        duckdb.connect())
    print("unit test of fct_contract_month: passed")
except AssertionError:
    print("unit test of fct_contract_month: failed")
print(diff.report([d for d in diff.compare(main, half)
                   if d.name.startswith("marts.")]))
for path, who in ((main, "main"), (half, "half-open")):
    con = duckdb.connect(str(path), read_only=True)
    (n,) = con.execute("SELECT sum(contracts_in_force) FROM "
                       "marts.fct_contract_month WHERE month_end = "
                       "'2025-12-31'").fetchone()
    con.close()
    print(f"active_accounts' contracts in force, 31 Dec 2025, {who}: "
          f"{n or 0:,}")
tidy("half-open")
