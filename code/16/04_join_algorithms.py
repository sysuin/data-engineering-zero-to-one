# nondeterministic: wall-clock timings, which differ by machine and run
# timeout: 900
# Four joins on the order lines, and the algorithm DuckDB picks.
import time

from _plans import analyze, connect, name, operators

con = connect()
con.execute("""
    CREATE TEMP TABLE lines AS        -- each line, its account's id
    SELECT f.order_id, f.line_no, a.account_id, f.order_date
    FROM marts.fct_order_lines AS f
    JOIN marts.dim_account AS a USING (account_key)""")
VERSION = """
    SELECT l.order_id, l.line_no, v.account_key
    FROM lines AS l LEFT JOIN marts.dim_account AS v
      ON v.account_id = l.account_id
     AND l.order_date >= v.valid_from AND {end}"""
COALESCE = "l.order_date < coalesce(v.valid_to, DATE '9999-12-31')"
OR = "(l.order_date < v.valid_to OR v.valid_to IS NULL)"
JOINS = {
    "versions, coalesce": VERSION.format(end=COALESCE),
    "versions, OR": VERSION.format(end=OR),
    "versions, OR, inner": VERSION.format(end=OR).replace(
        "LEFT JOIN", "JOIN"),
    "months, by range": """
        SELECT l.order_id, m.month_start FROM lines AS l
        JOIN (SELECT DISTINCT month_start, month_end
              FROM marts.dim_date) AS m
          ON l.order_date BETWEEN m.month_start AND m.month_end""",
}
n_lines, = con.execute("SELECT count(*) FROM lines").fetchone()
n_versions, = con.execute("SELECT count(*) FROM marts.dim_account"
                          ).fetchone()
print(f"probe: {n_lines:,} lines; build: {n_versions:,} versions")
print(f"{'join':<20}{'algorithm':<18}{'join out':>11}{'rows':>11}"
      f"{'seconds':>8}")
seconds = {}
for i, (label, sql) in enumerate(JOINS.items()):
    start = time.perf_counter()        # keep each result: run it once
    profile = analyze(con, f"CREATE TEMP TABLE r{i} AS {sql}")
    seconds[label] = time.perf_counter() - start
    join = [n for _, n in operators(profile) if "JOIN" in name(n)][0]
    rows, = con.execute(f"SELECT count(*) FROM r{i}").fetchone()
    print(f"{label:<20}{name(join):<18}"
          f"{join['operator_cardinality']:>11,}{rows:>11,}"
          f"{seconds[label]:>8.2f}")

differ, = con.execute("""
    SELECT count(*) FROM r0 AS a FULL JOIN r1 AS b
    USING (order_id, line_no)
    WHERE a.account_key IS DISTINCT FROM b.account_key""").fetchone()
print(f"lines whose version differs, coalesce vs OR: {differ}")
print(f"pairs the nested loop tested: {n_lines * n_versions:,}")
ratio = seconds["versions, OR"] / seconds["versions, coalesce"]
print(f"OR slower than coalesce: {ratio > 10} "
      f"(about {float(f'{ratio:.2g}'):,.0f} times, this run)")
