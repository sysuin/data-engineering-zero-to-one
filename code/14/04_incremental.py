# fct_order_lines as a dbt incremental model: built in full from
# Tradeline as raw saw it at 02:05 on 10 February 2025, then brought
# forward a night with no lookback and with an hour's, each set beside
# a full rebuild of the same night.
# timeout: 1800
import json
from datetime import datetime
from pathlib import Path

from _dbt import HERE, built, connect, copy, dbt, tidy
from meridian.sources import tradeline

A, B = datetime(2025, 2, 10, 2, 5), datetime(2025, 2, 11, 2, 5)
path = copy("incremental", source=built())
tl = HERE / "incremental" / "tradeline.db"
COLS = {"orders": "id, order_number, account_id, placed_at, channel, "
                  "status, created_at, updated_at",
        "order_lines": "order_id, line_no, sku, qty, unit_price_cents, "
                       "discount_pct, created_at, updated_at"}


def tradeline_at(moment):
    """Raw's two current views, replaced by Tradeline at a moment."""
    tradeline.at(moment, tl)
    con = connect(path, read_only=False)
    con.execute(f"ATTACH '{tl}' AS tl (TYPE sqlite, READ_ONLY)")
    for table, cols in COLS.items():
        rel = f"raw.tradeline_{table}_current"
        if con.execute("SELECT count(*) FROM duckdb_views() WHERE "
                       "schema_name = 'raw' AND view_name = ?",
                       [rel[4:]]).fetchone()[0]:
            con.execute(f"DROP VIEW {rel}")
        con.execute(f"CREATE OR REPLACE TABLE {rel} AS SELECT {cols}, "
                    "NULL::VARCHAR AS _load_id, "
                    f"NULL::TIMESTAMP AS _loaded_at FROM tl.{table}")
    con.close()


def keep(name):
    """Copy the fact aside, as scratch.<name>."""
    con = connect(path, read_only=False)
    con.execute("CREATE SCHEMA IF NOT EXISTS scratch")
    con.execute(f"CREATE OR REPLACE TABLE scratch.{name} AS "
                "FROM marts.fct_order_lines")
    con.close()


tradeline_at(A)
dbt("run", "--select", "fct_order_lines", "--full-refresh", path=path)
keep("night_a")

tradeline_at(B)
con = connect(path)
n = lambda sql: con.execute(sql).fetchone()[0]           # noqa: E731
CHANGED = """SELECT order_id FROM staging.stg_tradeline__orders
             WHERE updated_at >= {t} UNION
             SELECT order_id FROM staging.stg_tradeline__order_lines
             WHERE updated_at >= {t}"""
since = f"TIMESTAMP '{A:%Y-%m-%d %H:%M}'"
redo = {h: n("SELECT count(*) FROM (" + CHANGED.format(
    t=f"{since} - INTERVAL {h} HOUR") + ")") for h in (0, 1)}
new = n(f"""SELECT count(*) FROM staging.stg_tradeline__orders
            WHERE created_at >= {since}""")
gone = n("""SELECT count(DISTINCT order_id) FROM scratch.night_a
            ANTI JOIN intermediate.int_order_lines USING (order_id)""")
unknown = n("""SELECT count(*) FROM scratch.night_a AS f
               JOIN marts.dim_account AS a USING (account_key)
               JOIN marts.dim_product AS p USING (product_key)
               WHERE a.account_id = -1 OR p.sku = 'UNKNOWN'""")
orders = n("SELECT count(DISTINCT order_id) FROM scratch.night_a")
con.close()
print(f"full build, Tradeline at {A:%a %d %b %Y %H:%M}: "
      f"{orders:,} orders")
print(f"Tradeline at {B:%a %d %b %H:%M}: orders to take again")
print(f"  changed since {A:%H:%M} the night before, {new} of them new"
      f"{redo[0]:>6}")
print(f"  changed since an hour before that{redo[1]:>23}")
print(f"  on an unknown member{unknown:>36}")
print(f"  no longer placed: gone, for the post_hook{gone:>15}")

for hours in (0, 1):
    con = connect(path, read_only=False)
    con.execute("CREATE OR REPLACE TABLE marts.fct_order_lines AS "
                "FROM scratch.night_a")
    con.close()
    dbt("run", "--select", "fct_order_lines", path=path,
        vars={"since": f"{A:%Y-%m-%d %H:%M}", "lookback_hours": hours})
    keep(f"lookback_{hours}")
dbt("run", "--select", "fct_order_lines", "--full-refresh", path=path)

con, result = connect(path), {}
print(f"\n{'fct_order_lines':<26}{'lines':>8}{'revenue':>15}"
      f"{'wrong':>7}{'orders':>7}")
RUNS = (("incremental, no lookback", "scratch.lookback_0"),
        ("incremental, 1 h lookback", "scratch.lookback_1"),
        ("full rebuild", "marts.fct_order_lines"))
for label, table in RUNS:
    lines, revenue = con.execute(
        f"SELECT count(*), sum(revenue) FROM {table}").fetchone()
    diff = f"""(FROM {table} EXCEPT ALL FROM marts.fct_order_lines)
               UNION ALL
               (FROM marts.fct_order_lines EXCEPT ALL FROM {table})"""
    wrong = n(f"SELECT count(*) FROM ({diff})")
    bad = n(f"SELECT count(DISTINCT order_id) FROM ({diff})")
    print(f"{label:<26}{lines:>8,}{revenue:>15,.2f}{wrong:>7}{bad:>7}")
    result[label] = wrong
full = con.execute("SELECT count(*), count(DISTINCT order_id) "
                   "FROM marts.fct_order_lines").fetchone()
con.close()
Path(__file__).with_suffix(".json").write_text(json.dumps({
    "full_lines": full[0], "full_orders": full[1], "redo_0h": redo[0],
    "redo_1h": redo[1], "wrong_0h": result["incremental, no lookback"],
    "wrong_1h": result["incremental, 1 h lookback"]}, indent=1))
tidy("incremental")
