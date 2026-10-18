# A join that multiplies rows, how to see it coming, and how to see it.
from _plans import analyze, connect, name, operators

con = connect()
con.execute("""
    CREATE TEMP TABLE lines AS
    SELECT f.order_id, f.line_no, a.account_id, f.order_date, f.revenue
    FROM marts.fct_order_lines AS f
    JOIN marts.dim_account AS a USING (account_key);
    CREATE TEMP TABLE contracts AS
    SELECT c.contract_id, a.account_id, c.start_date, c.end_date
    FROM marts.fct_contracts AS c
    JOIN marts.dim_account AS a USING (account_key)""")
JOINS = {
    "versions, id only": """JOIN marts.dim_account AS v
        ON v.account_id = l.account_id""",
    "versions, id and day": """JOIN marts.dim_account AS v
        ON v.account_id = l.account_id AND l.order_date >= v.valid_from
       AND l.order_date < coalesce(v.valid_to, DATE '9999-12-31')""",
    "contracts, id and day": """JOIN contracts AS c
        ON c.account_id = l.account_id
       AND l.order_date BETWEEN c.start_date AND c.end_date""",
}
rows, revenue = con.execute(
    "SELECT count(*), sum(revenue) FROM lines").fetchone()
print(f"{'':<32}{'rows':>14}{'revenue':>18}")
print(f"{'order lines':<32}{rows:>14,}{revenue:>18,}")
for label, join in JOINS.items():
    out, total = con.execute(f"SELECT count(*), sum(l.revenue) "
                             f"FROM lines AS l {join}").fetchone()
    print(f"{'+ ' + label:<32}{out:>14,}{total:>18,}")

# Before running a join, count what it will make: per key, left x right.
per_key, = con.execute("""
    SELECT sum(n * m) FROM
      (SELECT account_id, count(*) AS n FROM lines GROUP BY 1)
    JOIN (SELECT account_id, count(*) AS m FROM marts.dim_account
          GROUP BY 1) USING (account_id)""").fetchone()
by_day, = con.execute("""
    SELECT sum(n) FROM (SELECT order_date, count(*) AS n
                        FROM lines GROUP BY 1) AS d
    JOIN contracts AS c
      ON d.order_date BETWEEN c.start_date AND c.end_date""").fetchone()
print()
print(f"predicted, versions on id only: {per_key:>16,}")
print(f"predicted, contracts on day only: {by_day:>14,}  (not run)")

# In a plan, an explosion is a join that emits more than it is fed.
profile = analyze(con, "SELECT sum(l.revenue) FROM lines AS l "
                       + JOINS["versions, id only"])
join = [n for _, n in operators(profile) if name(n) == "HASH_JOIN"][0]
fed = [c["operator_cardinality"] for c in join["children"]]
print(f"HASH_JOIN fed {fed[0]:,} and {fed[1]:,} rows, "
      f"emitted {join['operator_cardinality']:,}")
