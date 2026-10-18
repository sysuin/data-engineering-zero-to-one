# timeout: 1800
# What the fix changed, proved: both marts before and after, day by day
# and column by column, and the result against a full build.
import io
import runpy
from contextlib import redirect_stdout

import duckdb
from _sandbox import HERE

from groundwork.orchestrate import backfill

KEYS = {"fct_order_lines": ("order_id", "line_no"),
        "fct_orders": ("order_id",)}


def diff(con, model, a, b):
    """Rows of table a against table b, joined on the model's key:
    {column: rows where it differs}, rows only on one side, and the
    days (order_date) on which anything differs."""
    cols = [c for (c,) in con.execute(
        "SELECT column_name FROM information_schema.columns WHERE "
        "table_schema = 'marts' AND table_name = ? ORDER BY "
        "ordinal_position", [model]).fetchall()]
    on = " AND ".join(f"a.{k} = b.{k}" for k in KEYS[model])
    per = ", ".join(f"count(*) FILTER (WHERE a.{c} IS DISTINCT FROM "
                    f"b.{c}) AS {c}" for c in cols)
    row = con.execute(f"""
        SELECT {per}, count(*) FILTER (WHERE a.{KEYS[model][0]} IS NULL
                                        OR b.{KEYS[model][0]} IS NULL)
        FROM {a} AS a FULL JOIN {b} AS b ON {on}""").fetchone()
    moved = {c: n for c, n in zip(cols, row) if n}
    changed = " OR ".join(f"a.{c} IS DISTINCT FROM b.{c}" for c in cols)
    days = [d for (d,) in con.execute(f"""
        SELECT DISTINCT coalesce(a.order_date, b.order_date)
        FROM {a} AS a FULL JOIN {b} AS b ON {on}
        WHERE {changed} ORDER BY 1""").fetchall()]
    return moved, row[-1], days


if __name__ == "__main__":
    work = HERE / "work" / "05"
    if not (work / "FIXED").exists():      # listing 05 leaves it here
        with redirect_stdout(io.StringIO()):
            runpy.run_path("code/18/05_bug_and_fix.py",
                           run_name="__main__")
    env = backfill.sandbox_env(work)
    con = duckdb.connect(str(env.warehouse), read_only=True)
    for m in KEYS:
        (n_days, n_rows) = con.execute(
            f"SELECT count(DISTINCT order_date), count(*) "
            f"FROM marts.{m}").fetchone()
        moved, lone, days = diff(con, m, f"drill.{m}_before",
                                 f"marts.{m}")
        rows = max(moved.values(), default=0)
        print(f"{m}: {n_rows:,} rows on {n_days:,} days")
        print(f"  before and after the fix: {rows:,} rows differ, on "
              f"{len(days)} days,\n    {days[0]} to {days[-1]}; "
              f"{lone} rows on one side only")
        print("  moved: " + ", ".join(f"{c} {n:,}"
                                      for c, n in moved.items()))
        moved, lone, days = diff(con, m, f"drill.{m}_truth",
                                 f"marts.{m}")
        print(f"  against a full build: {len(moved)} columns differ, "
              f"{lone} rows on one side")
    (margin,) = con.execute("""
        SELECT sum(b.gross_margin) - sum(a.gross_margin)
        FROM marts.fct_order_lines AS a
        JOIN drill.fct_order_lines_before AS b USING (order_id, line_no)
    """).fetchone()
    con.close()
    print(f"margin the change had added, and the fix took away: "
          f"${margin:,.2f}")
