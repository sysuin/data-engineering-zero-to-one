# One tile, three ways: computed from lines, from orders, or a rollup.
from _plans import analyze, connect, name, operators, scans

con = connect()
TILE = """SELECT r.region_name,
                 date_trunc('month', order_date) AS month,
                 sum(revenue) AS revenue
          FROM {source} JOIN marts.dim_region AS r USING (region_key)
          GROUP BY ALL"""
con.execute("""
    CREATE TEMP TABLE monthly AS    -- built once a night, by the run
    SELECT region_key, date_trunc('month', order_date) AS order_date,
           sum(revenue) AS revenue
    FROM marts.fct_order_lines GROUP BY ALL""")
SOURCES = {"the lines": "marts.fct_order_lines",
           "the orders": "marts.fct_orders",
           "a monthly rollup": "monthly"}
REFRESHES = 96                      # a tile refreshed every 15 minutes
build, = con.execute("SELECT count(*) FROM marts.fct_order_lines"
                     ).fetchone()
answers = {}
print(f"{'tile computed from':<20}{'rows a refresh':>15}"
      f"{f'rows a day ({REFRESHES})':>20}")
for label, source in SOURCES.items():
    sql = TILE.format(source=source)
    read = sum(s["operator_rows_scanned"]
               for s in scans(analyze(con, sql))
               if "dim_region" not in s["extra_info"].get("Table", ""))
    answers[label] = set(con.execute(sql).fetchall())
    daily = REFRESHES * read + (build if source == "monthly" else 0)
    print(f"{label:<20}{read:>15,}{daily:>20,}")
same = len({frozenset(a) for a in answers.values()}) == 1
cells = len(answers["the lines"])
print(f"the same {cells} cells all three ways: {same}")
print(f"(the rollup's day includes its one build: {build:,} rows)")

print()
profile = analyze(con, "SELECT count(*) "
                       "FROM intermediate.int_order_lines")
for _, node in operators(profile):
    if name(node) == "WINDOW":
        print(f"reading int_order_lines ranks "
              f"{node['operator_cardinality']:>9,} rows again")
