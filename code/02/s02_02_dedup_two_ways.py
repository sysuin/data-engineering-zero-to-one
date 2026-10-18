# Exercise 2.2: one row per account with QUALIFY and with arg_max.
import duckdb

from groundwork.config import LANDING

nights = LANDING / "relay" / "*" / "accounts.csv"
con = duckdb.connect()
con.execute(f"""
    CREATE TABLE stacked AS
    SELECT parse_path(filename)[-2] AS night, account_id, status,
           segment
    FROM read_csv('{nights}', filename = true)""")

by_rank = con.execute("""
    SELECT account_id, status, segment FROM stacked
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY account_id ORDER BY night DESC) = 1
    ORDER BY account_id""").fetchall()
by_arg_max = con.execute("""
    SELECT account_id, arg_max(status, night), arg_max(segment, night)
    FROM stacked GROUP BY account_id
    ORDER BY account_id""").fetchall()
print(f"QUALIFY {len(by_rank):,} rows, "
      f"arg_max {len(by_arg_max):,} rows, "
      f"identical: {by_rank == by_arg_max}")
