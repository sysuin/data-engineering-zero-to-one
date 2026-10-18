# DuckDB reads CSV, gzipped JSON, SQLite and Parquet where they lie.
import duckdb

from groundwork.config import LANDING, SANDBOX, TRADELINE_DB

accounts = LANDING / "relay" / "2026-01-04" / "accounts.csv"
portal = LANDING / "portal" / "2025" / "0[789]" / "*" / "*.jsonl.gz"
parquet = SANDBOX / "ch02" / "accounts.parquet"

con = duckdb.connect()                  # in memory: nothing to set up

print("types DuckDB inferred from the CSV:")
for name, kind, *_ in con.execute(
        f"DESCRIBE SELECT * FROM read_csv('{accounts}')").fetchall():
    if name in ("account_id", "customer_since", "postcode",
                "key_account", "closed_on"):
        print(f"  {name:15} {kind}")

(n,) = con.execute(
    f"SELECT count(*) FROM read_json('{portal}')").fetchone()
print(f"portal events, July to September 2025: {n:,}")

con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline "
            "(TYPE sqlite, READ_ONLY)")
rows = con.execute(f"""
    SELECT a.region, count(*) AS orders
    FROM tradeline.orders o
    JOIN read_csv('{accounts}') a ON a.account_id = o.account_id
    WHERE o.placed_at >= '2025-12-01' AND o.placed_at < '2026-01-01'
    GROUP BY a.region
    ORDER BY a.region
""").fetchall()
print("Tradeline orders in December 2025, by Relay's region:")
for region, orders in rows:
    print(f"  {region:10} {orders:>6,}")
(total,) = con.execute("""
    SELECT count(*) FROM tradeline.orders
    WHERE placed_at >= '2025-12-01' AND placed_at < '2026-01-01'
""").fetchone()
print(f"  {'sum':10} {sum(r[1] for r in rows):>6,}   "
      f"(Tradeline alone: {total:,})")

con.execute(f"COPY (SELECT * FROM read_csv('{accounts}')) "
            f"TO '{parquet}' (FORMAT parquet)")
(back,) = con.execute(
    f"SELECT count(*) FROM '{parquet}'").fetchone()
print(f"wrote {parquet.name}; it holds {back:,} rows")
