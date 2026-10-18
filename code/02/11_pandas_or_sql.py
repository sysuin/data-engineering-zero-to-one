# One question answered in pandas and in DuckDB, and proof they agree.
import duckdb
import pandas as pd

from groundwork.config import LANDING, TRADELINE_DB

accounts = LANDING / "relay" / "2026-01-04" / "accounts.csv"
con = duckdb.connect()
con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline "
            "(TYPE sqlite, READ_ONLY)")
orders = con.execute(f"""
    SELECT o.id, o.channel, a.region
    FROM tradeline.orders o
    JOIN read_csv('{accounts}') a ON a.account_id = o.account_id
    WHERE o.placed_at >= '2025-12-01' AND o.placed_at < '2026-01-01'
""").df()
print(f"{len(orders):,} orders in a DataFrame, "
      f"{orders.memory_usage(deep=True).sum() / 1e6:.1f} MB")

# pandas: one line, and the result is already a grid.
in_pandas = pd.crosstab(orders["region"], orders["channel"])

# SQL: DuckDB reads the DataFrame `orders` by its Python name.
in_sql = con.execute("""
    PIVOT orders ON channel USING count(*)
    GROUP BY region ORDER BY region""").df().set_index("region")

pd.testing.assert_frame_equal(in_pandas, in_sql, check_names=False)
print("crosstab and PIVOT agree, cell for cell")
print(in_pandas.to_string())

share = con.execute("""
    SELECT region, channel,
           count(*) / sum(count(*)) OVER (PARTITION BY region) AS share
    FROM orders GROUP BY region, channel
    QUALIFY ROW_NUMBER() OVER (PARTITION BY region
                               ORDER BY count(*) DESC, channel) = 1
    ORDER BY region""").fetchall()
print("each region's busiest channel:")
for region, channel, s in share:
    print(f"  {region:10} {channel}  {s:.1%}")
