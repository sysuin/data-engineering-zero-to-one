# Three upserts of Relay's accounts: what each writes, and a late night.
import duckdb

from groundwork.config import LANDING

con = duckdb.connect()
for name in ("merged", "on_conflict", "replaced"):
    con.execute(f"""CREATE TABLE {name} (
        account_id BIGINT PRIMARY KEY, status VARCHAR,
        updated_at TIMESTAMP)""")
night = ("(SELECT account_id, status, updated_at FROM read_csv(?, "
         "types = {'updated_at': 'TIMESTAMP'}))")
UPSERTS = {
    "merged": f"""MERGE INTO merged AS t USING {night} AS s
        ON t.account_id = s.account_id
        WHEN MATCHED AND s.updated_at > t.updated_at THEN UPDATE
            SET status = s.status, updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT BY NAME
        RETURNING merge_action""",
    "on_conflict": f"""INSERT INTO on_conflict BY NAME FROM {night}
        ON CONFLICT (account_id) DO UPDATE
            SET status = excluded.status,
                updated_at = excluded.updated_at
            WHERE excluded.updated_at > on_conflict.updated_at
        RETURNING account_id""",
    "replaced": f"""INSERT OR REPLACE INTO replaced BY NAME
        FROM {night} RETURNING account_id""",
}

print(f"{'night':<16}{'MERGE':>6}{'ON CONFLICT':>13}{'OR REPLACE':>12}")
for day, label in [("2025-12-30", ""), ("2025-12-31", ""),
                   ("2025-12-31", " again"), ("2025-12-30", " late")]:
    path = str(LANDING / "relay" / day / "accounts.csv")
    written = [len(con.execute(sql, [path]).fetchall())
               for sql in UPSERTS.values()]
    print(f"{day + label:<16}{written[0]:>6,}{written[1]:>13,}"
          f"{written[2]:>12,}")

print("rows that differ from MERGE's table:")
for name in ("on_conflict", "replaced"):
    (n,) = con.execute(f"SELECT count(*) FROM "
                       f"(FROM merged EXCEPT FROM {name})").fetchone()
    print(f"  {name:<14}{n:>4}")
