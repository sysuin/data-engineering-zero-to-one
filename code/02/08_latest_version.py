# Fourteen nightly exports stacked, then one row per account, two ways.
import json
from pathlib import Path

import duckdb

from groundwork.config import LANDING

nights = LANDING / "relay" / "*" / "accounts.csv"
con = duckdb.connect()
con.execute(f"""
    CREATE TABLE stacked AS
    SELECT parse_path(filename)[-2] AS night,   -- the folder's name
           account_id, status, segment, updated_at
    FROM read_csv('{nights}', filename = true,
                  types = {{'updated_at': 'TIMESTAMP'}})
""")
(rows, keys, first, last) = con.execute("""
    SELECT count(*), count(DISTINCT account_id), min(night), max(night)
    FROM stacked""").fetchone()
(versions,) = con.execute("""
    SELECT count(*) FROM (SELECT DISTINCT * EXCLUDE (night)
                          FROM stacked)""").fetchone()
print(f"stacked {rows:,} rows from {first} to {last}")
print(f"{keys:,} accounts; {versions:,} distinct versions of them")

latest = con.execute("""
    WITH ranked AS (
        SELECT *, ROW_NUMBER() OVER (
                   PARTITION BY account_id ORDER BY night DESC) AS rn
        FROM stacked)
    SELECT account_id, status, segment FROM ranked WHERE rn = 1
    ORDER BY account_id""").fetchall()
qualify = con.execute("""
    SELECT account_id, status, segment FROM stacked
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY account_id ORDER BY night DESC) = 1
    ORDER BY account_id""").fetchall()
tonight = con.execute(f"""
    SELECT account_id, status, segment FROM stacked
    WHERE night = '{last}' ORDER BY account_id""").fetchall()
print(f"latest per account: CTE {len(latest):,}, QUALIFY "
      f"{len(qualify):,}; same rows: {latest == qualify}")
print(f"identical to the {last} export: {latest == tonight}")

changes = con.execute("""
    SELECT night, account_id, prev_status, status
    FROM (SELECT night, account_id, status,
                 LAG(status) OVER (PARTITION BY account_id
                                   ORDER BY night) AS prev_status
          FROM stacked)
    WHERE prev_status <> status
    ORDER BY night, account_id""").fetchall()
nights_seen = sorted({night for night, *_ in changes})
print(f"status changes between consecutive nights: {len(changes)}, "
      f"on {', '.join(nights_seen)}")
for night, account, before, after in changes[:3]:
    print(f"  account {account:>5}  {before} -> {after}")

(ties,) = con.execute("""
    SELECT count(*) FROM (
        SELECT account_id FROM stacked
        GROUP BY account_id, updated_at
        QUALIFY updated_at = max(updated_at) OVER (
                    PARTITION BY account_id)
            AND count(*) > 1)""").fetchone()
print(f"accounts whose newest updated_at is on several nights: "
      f"{ties:,}")

# One account's fourteen rows, ranked, for the chapter's figure.
example = con.execute("""
    SELECT night, status, strftime(updated_at, '%Y-%m-%d %H:%M'),
           ROW_NUMBER() OVER (ORDER BY night DESC) AS rn
    FROM stacked WHERE account_id = 438 ORDER BY night""").fetchall()
Path(__file__).with_suffix(".json").write_text(
    json.dumps({"accounts": keys, "example": example}))
