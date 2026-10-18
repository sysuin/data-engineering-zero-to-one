# timeout: 1800
# The change of 13 January 2025, as the nightly builds from then to 16
# March ran it; what it touched, found by the build's own rule; and only
# those partitions rebuilt with the model as it should be.
import shutil
from dataclasses import replace

import duckdb
from _sandbox import HERE, copy_of_quarter

from groundwork.orchestrate import backfill, runner
from groundwork.orchestrate.dag import PROJECT

SINCE = "2025-01-13 06:00:00"       # the last read before the change
MODELS = ("fct_order_lines", "fct_orders")
VOSS = """
SELECT CASE WHEN f.order_date < DATE '2025-01-01' THEN '2024'
            WHEN f.order_date < DATE '2025-01-13' THEN '1-12 Jan'
            WHEN f.order_date < DATE '2025-02-01' THEN '13-31 Jan'
            WHEN f.order_date < DATE '2025-03-01' THEN 'February'
            ELSE '1-16 Mar' END AS days,
       round(100 * sum(f.gross_margin) / sum(f.revenue), 1)
FROM {} AS f JOIN marts.dim_supplier AS s USING (supplier_key)
WHERE s.supplier_name LIKE 'Voss%' AND f.order_date >= DATE '2024-01-01'
GROUP BY 1 ORDER BY min(f.order_date)
"""

if __name__ == "__main__":
    env = copy_of_quarter("05")         # built and right, 17 March
    work = HERE / "work" / "05"
    con = duckdb.connect(str(env.warehouse))
    con.execute("CREATE SCHEMA drill")
    for m in MODELS:                    # what a full build says
        con.execute(f"CREATE TABLE drill.{m}_truth AS "
                    f"SELECT * FROM marts.{m}")
    con.close()

    # The change goes out: the same project, one model different.
    broken = work / "project"
    shutil.copytree(PROJECT, broken, ignore=shutil.ignore_patterns(
        "target", "logs"))
    shutil.copy("code/18/fct_order_lines_cents.sql",
                broken / "models" / "marts" / "fct_order_lines.sql")
    benv = replace(env, dbt_target_path=work / "dbt-broken")
    backfill._dbt(benv, "run", "--select", "fct_order_lines+",
                  vars={"since": SINCE, "night": "2025-03-16"},
                  project=broken)
    res = backfill._dbt(benv, "test", "--select", "fct_order_lines+",
                        project=broken)
    tests = [str(r.status) for r in res.result.results]
    print(f"the nights since 13 January, run with the change:\n  "
          f"{tests.count('pass')} of {len(tests)} tests pass")
    con = duckdb.connect(str(env.warehouse))
    for m in MODELS:                    # what the dashboards were shown
        con.execute(f"CREATE TABLE drill.{m}_before AS "
                    f"SELECT * FROM marts.{m}")
    moved, skus = con.execute("""
        SELECT count(*) FILTER (WHERE n > 1), count(*) FROM (
            SELECT sku, count(*) AS n
            FROM intermediate.int_product_costs GROUP BY sku)"""
                              ).fetchone()
    print(f"products whose cost ever changed: {moved} of {skus}")
    print("Voss's margin, %:    as built   as it should be")
    shown = con.execute(VOSS.format("marts.fct_order_lines")).fetchall()
    truth = dict(con.execute(VOSS.format("drill.fct_order_lines_truth")
                             ).fetchall())
    for days, pct in shown:
        print(f"  {days:<18}{pct:>9}{truth[days]:>17}")

    # Which partitions did those builds write? The model says: every
    # order changed since the last read, less its hour's lookback.
    days = [d for (d,) in con.execute(f"""
        SELECT DISTINCT f.order_date FROM marts.fct_order_lines AS f
        SEMI JOIN intermediate.int_order_lines AS l
             ON l.order_id = f.order_id
            AND l.updated_at >= TIMESTAMP '{SINCE}' - INTERVAL 1 HOUR
        ORDER BY 1""").fetchall()]
    (orders,) = con.execute(f"""
        SELECT count(DISTINCT order_id)
        FROM intermediate.int_order_lines
        WHERE updated_at >= TIMESTAMP '{SINCE}' - INTERVAL 1 HOUR"""
                            ).fetchone()
    con.close()
    early = [d for d in days if str(d) < "2025-01-13"]
    print(f"\nwritten by those builds: {orders:,} orders, on "
          f"{len(days)} days,\n  {days[0]} to {days[-1]}; "
          f"{len(early)} of them before 13 January")
    res = backfill._dbt(env, "ls", "--select", "fct_order_lines+",
                        "--resource-type", "model", "--output", "name")
    print(f"marts built from it: {', '.join(sorted(res.result))}")

    # The fix: the model as it was, those days only, under the nightly
    # run's lock so tonight's run waits rather than collides.
    with runner.lock(env, "nightly"):
        for m in MODELS:
            sql = backfill.compiled(m, env)
            con = duckdb.connect(str(env.warehouse))
            gone, put = backfill.replace_days(con, m, days, sql)
            con.close()
            print(f"  {m:<16} {gone:>7,} rows out, {put:>7,} in")
    (work / "FIXED").write_text("05\n")
