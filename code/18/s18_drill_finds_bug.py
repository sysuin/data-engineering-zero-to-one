# timeout: 1800
# Exercise 2: the drill's facts check, run with the change of 13 January
# in place, on the night it went out and in March before the fix.
import io
import runpy
from contextlib import redirect_stdout

import duckdb
from _sandbox import HERE, quarter

from groundwork.config import SEED
from groundwork.orchestrate import backfill, drill

if __name__ == "__main__":
    days = drill.pick(quarter(), 20, SEED)      # the drill's twenty
    work = HERE / "work" / "05"
    if not (work / "FIXED").exists():           # listing 05's copy
        with redirect_stdout(io.StringIO()):
            runpy.run_path("code/18/05_bug_and_fix.py",
                           run_name="__main__")
    env = backfill.sandbox_env(work)
    sql = backfill.compiled("fct_order_lines", env,
                            project=work / "project")   # the change
    con = duckdb.connect(str(env.warehouse), read_only=True)
    con.execute("SET enable_progress_bar = false")
    cols = ", ".join(c for (c,) in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE "
        "table_schema = 'marts' AND table_name = 'fct_order_lines' "
        "ORDER BY ordinal_position").fetchall())
    pick = "order_date IN (SELECT unnest($days::DATE[]))"
    print(f"{'the rows held':<30}days that differ")
    for label, held in (("all right, as a full build",
                         "drill.fct_order_lines_truth"),
                        ("as the builds since left them",
                         "drill.fct_order_lines_before")):
        differ = con.execute(f"""
            WITH now AS (SELECT {cols} FROM ({sql}) WHERE {pick}),
                 held AS (SELECT {cols} FROM {held} WHERE {pick})
            SELECT order_date, count(*) FROM (
                (SELECT * FROM now EXCEPT ALL SELECT * FROM held)
                UNION ALL
                (SELECT * FROM held EXCEPT ALL SELECT * FROM now))
            GROUP BY 1 ORDER BY 1""", {"days": days}).fetchall()
        print(f"{label:<30}{len(differ)} of {len(days)}")
        for d, n in differ:
            print(f"  {d}  {n:>4} rows")
    con.close()
