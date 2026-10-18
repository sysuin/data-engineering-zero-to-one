# Voss's January 2025 list, effective on the 1st, arrived on the 6th:
# margins with the cost in hand at each night's load, and as of the
# order's day; then the stale lines restated by facts.incremental.
# timeout: 600
import json
import shutil
from pathlib import Path

import duckdb

from groundwork import facts
from groundwork.config import NOW
from groundwork.transform import runner

from _facts import HERE, SHARED, built, tidy

con = built()
print(f"{'Voss list':<10}{'effective':<12}{'arrived (UTC)':<18}")
for eff, arrived in con.execute("""
        SELECT DISTINCT p.effective_date, f.arrived_at
        FROM staging.stg_suppliers__price_lists AS p
        JOIN audit.file_ledger AS f ON f.name = p.source_file
        WHERE p.supplier_slug = 'voss-industrial'
          AND p.effective_date BETWEEN '2024-12-01' AND '2025-02-01'
        ORDER BY 1""").fetchall():
    print(f"{eff:%Y-%m}   {eff!s:<12}{arrived:%Y-%m-%d %H:%M}")

# The night's run for day D starts at 06:00 UTC on D + 1. The cost in
# hand then is the latest list that had arrived; as of the order, the
# latest list in force on day D.
DAILY = """
WITH lists AS (
    SELECT p.sku, p.effective_date, p.unit_cost_cents, f.arrived_at
    FROM staging.stg_suppliers__price_lists AS p
    JOIN audit.file_ledger AS f ON f.name = p.source_file
    WHERE p.supplier_slug = 'voss-industrial'),
lines AS (
    SELECT f.*, p.sku,
           CAST(f.order_date + 1 AS TIMESTAMP)
           + INTERVAL 6 HOUR AS run_at
    FROM marts.fct_order_lines AS f
    JOIN marts.dim_product AS p USING (product_key)
    JOIN marts.dim_supplier AS s ON s.supplier_key = f.supplier_key
    WHERE s.supplier_name LIKE 'Voss%'
      AND f.order_date BETWEEN '2025-01-01' AND '2025-01-10'),
loaded AS (
    SELECT l.*, (SELECT arg_max(unit_cost_cents, arrived_at)
                 FROM lists WHERE lists.sku = l.sku
                   AND lists.arrived_at <= l.run_at) AS cents_then
    FROM lines AS l)
SELECT order_date, count(*), sum(revenue), sum(gross_margin),
       sum(revenue) - CAST(sum(qty * cents_then) * 0.01
                           AS DECIMAL(18, 2))
FROM loaded GROUP BY 1 ORDER BY 1"""
print(f"\n{'day':<11}{'lines':>6}{'revenue':>11}{'margin, as of':>15}"
      f"{'as loaded':>13}")
days = con.execute(DAILY).fetchall()
for day, n, rev, true, then in days:
    print(f"{day:%a %d %b} {n:>5}{rev:>11,.0f}{true:>9,.0f}"
          f"{100 * true / rev:>5.1f}%{then:>8,.0f}"
          f"{100 * then / rev:>5.1f}%")
early = [r for r in days if r[0].day <= 5]
true, then = sum(r[3] for r in early), sum(r[4] for r in early)
print(f"1-5 January: margin overstated by {then - true:,.2f}")
Path(__file__).with_suffix(".json").write_text(json.dumps(
    [[str(d), float(r), float(t), float(h)] for d, _, r, t, h in days],
    indent=1))
con.close()

# The same thing inside Groundwork: build the facts before the list
# arrived, let it arrive, and ask facts.incremental to catch up.
tidy("late")
(HERE / "late").mkdir(parents=True)
shutil.copy(SHARED / "warehouse.duckdb", HERE / "late" / "w.duckdb")
con = duckdb.connect(str(HERE / "late" / "w.duckdb"))
models = runner.discover()
sql = {m: runner.compile_sql(models[m], models)
       for m in ("int_product_costs", "dim_product", "fct_order_lines",
                 "fct_orders")}
voss_jan = ("(SELECT * FROM staging.stg_suppliers__price_lists WHERE "
            "NOT (supplier_slug = 'voss-industrial' AND "
            "effective_date = DATE '2025-01-01'))")


run = "TIMESTAMP '2025-01-06 06:00'"       # the run for 5 January
placed = ("(SELECT * FROM intermediate.int_order_lines "
          f"WHERE placed_at < {run})")


def rebuild(*names, before=False):
    for m in names:
        body = sql[m]
        if before:            # only what had arrived by that run
            body = body.replace("staging.stg_suppliers__price_lists",
                                voss_jan).replace(
                "intermediate.int_order_lines", placed)
        con.execute(f"DELETE FROM {models[m].relation}")
        con.execute(f"INSERT INTO {models[m].relation} {body}")


def margin():
    return con.execute("""
        SELECT sum(gross_margin) FROM marts.fct_order_lines
        JOIN marts.dim_supplier USING (supplier_key)
        WHERE supplier_name LIKE 'Voss%'
          AND order_date BETWEEN '2025-01-01' AND '2025-01-05'
        """).fetchone()[0]


rebuild("int_product_costs", "dim_product", "fct_order_lines",
        "fct_orders", before=True)
print("\nVoss margin, 1-5 January, in fct_order_lines")
print(f"  built before the list arrived    {margin():>12,.2f}")
rebuild("int_product_costs", "dim_product")
print(f"  list arrived, facts untouched    {margin():>12,.2f}")
for problem in facts.check(con):
    print(f"  facts.check: {problem.split(': ')[1]}")
got = facts.incremental(con, since=NOW)
print(f"  after facts.incremental          {margin():>12,.2f}")
print(f"  orders restated: {got['stale']} stale, {got['changed']} "
      f"changed, {got['unknown']} unknown; {got['lines']} lines")
print(f"  facts.check: {facts.check(con) or 'every promise kept'}")
con.close()
tidy("late")
