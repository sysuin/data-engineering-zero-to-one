# Keep a current-accounts table up to date, night by night, with MERGE.
import duckdb

from groundwork.config import LANDING, SANDBOX

db = SANDBOX / "ch02" / "upsert.duckdb"
db.unlink(missing_ok=True)              # start from nothing, every run
con = duckdb.connect(str(db))
for table in ("accounts", "accounts_oc"):
    con.execute(f"""
        CREATE TABLE {table} (
            account_id BIGINT PRIMARY KEY,
            status     VARCHAR,
            segment    VARCHAR,
            updated_at TIMESTAMP)""")

EXPORT = """(SELECT account_id, status, segment, updated_at
            FROM read_csv(?, types = {'updated_at': 'TIMESTAMP'}))"""
MERGE = f"""
    MERGE INTO accounts AS t
    USING {EXPORT} AS s
    ON t.account_id = s.account_id
    WHEN MATCHED AND s.updated_at > t.updated_at THEN
        UPDATE SET status = s.status, segment = s.segment,
                   updated_at = s.updated_at
    WHEN NOT MATCHED THEN
        INSERT VALUES (s.account_id, s.status, s.segment, s.updated_at)
    RETURNING merge_action"""
ON_CONFLICT = f"""
    INSERT INTO accounts_oc SELECT * FROM {EXPORT}
    ON CONFLICT (account_id) DO UPDATE
        SET status = excluded.status, segment = excluded.segment,
            updated_at = excluded.updated_at
        WHERE excluded.updated_at > accounts_oc.updated_at"""


def load(night):
    """Merge one night's export; return (inserted, updated)."""
    path = str(LANDING / "relay" / night / "accounts.csv")
    actions = [a for (a,) in con.execute(MERGE, [path]).fetchall()]
    con.execute(ON_CONFLICT, [path])    # the same, the other way
    return actions.count("INSERT"), actions.count("UPDATE")


nights = sorted(p.name for p in (LANDING / "relay").glob("20*"))
print(f"{'night':10} {'inserted':>9} {'updated':>8} {'table':>6}")
for night in nights:
    inserted, updated = load(night)
    (size,) = con.execute("SELECT count(*) FROM accounts").fetchone()
    print(f"{night:10} {inserted:>9,} {updated:>8,} {size:>6,}")

print(f"{nights[-1]} again: inserted, updated = {load(nights[-1])}")
(differ,) = con.execute("""
    SELECT (SELECT count(*) FROM (FROM accounts
                                  EXCEPT FROM accounts_oc))
         + (SELECT count(*) FROM (FROM accounts_oc
                                  EXCEPT FROM accounts))
""").fetchone()
print(f"rows where MERGE and ON CONFLICT disagree: {differ}")
con.close()
