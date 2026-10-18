# Three uniqueness queries on the wrong key, and what each one says.
from groundwork.config import LANDING, TRADELINE_DB
from groundwork.profile import connect

con = connect()
con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline "
            "(TYPE sqlite, READ_ONLY)")
relay = LANDING / "relay" / "2026-01-04"
sup = LANDING / "suppliers"

CASES = [
    ("order_lines by order_id", "order_id", "tradeline.order_lines"),
    ("contracts by account_id", "account_id",
     f"read_csv('{relay / 'contracts.csv'}', all_varchar = true)"),
    ("13 alike lists by sku", "sku",
     f"read_csv('{sup / '*/*_????-??.csv'}', all_varchar = true)"),
]
print(f"{'key tried':<26}{'rows':>11}{'distinct':>10}{'most':>6}")
for name, key, source in CASES:
    rows, distinct, most = con.execute(f"""
        SELECT SUM(n), COUNT(*), MAX(n) FROM (
            SELECT {key}, COUNT(*) AS n FROM {source} GROUP BY {key})
    """).fetchone()
    print(f"{name:<26}{rows:>11,}{distinct:>10,}{most:>6,}")
