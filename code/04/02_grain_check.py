# Test each table's grain, then find what a line-grain sheet cannot say.
import json

import duckdb

from groundwork.config import LANDING, TRADELINE_DB

con = duckdb.connect()
con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline "
            "(TYPE sqlite, READ_ONLY)")
relay = LANDING / "relay" / "2026-01-04"
con.execute(f"CREATE VIEW accounts AS "
            f"SELECT * FROM read_csv('{relay}/accounts.csv')")
con.execute(f"CREATE VIEW contracts AS "
            f"SELECT * FROM read_csv('{relay}/contracts.csv')")

# A grain is a claim: "one row per <key>". Count rows and distinct keys.
claims = [("tradeline.orders", "id"),
          ("tradeline.order_lines", "order_id, line_no"),
          ("tradeline.order_lines", "order_id"),
          ("accounts", "account_id"),
          ("contracts", "contract_id"),
          ("contracts", "account_id")]
print(f"{'table':<22}{'one row per':<19}{'rows':>10}{'keys':>10}")
for table, key in claims:
    rows, keys = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT ({key})) FROM {table}
    """).fetchone()
    verdict = "" if rows == keys else "  no"
    print(f"{table:<22}{key:<19}{rows:>10,}{keys:>10,}{verdict}")

# The sheet: one row per 2025 order line, with the account beside it.
con.execute("""
    CREATE TABLE sheet AS
    SELECT o.order_number, l.line_no, o.placed_at, o.channel,
           l.sku, l.qty, l.unit_price_cents,
           l.qty * l.unit_price_cents AS revenue_cents,
           a.account_id, a.account_name, a.region, a.segment,
           a.status
    FROM tradeline.orders o
    JOIN tradeline.order_lines l ON l.order_id = o.id
    JOIN accounts a ON a.account_id = o.account_id
    WHERE o.status = 'placed'
      AND o.placed_at >= '2025-01-01' AND o.placed_at < '2026-01-01'
""")
rows, orders, accts = con.execute("""
    SELECT COUNT(*), COUNT(DISTINCT order_number),
           COUNT(DISTINCT account_id)
    FROM sheet
""").fetchone()
print(f"\nthe sheet: {rows:,} lines, {orders:,} orders, "
      f"{accts:,} accounts")

# Which active accounts ordered nothing in December? Two ways to ask.
(from_sheet,) = con.execute("""
    SELECT COUNT(DISTINCT account_id) FROM sheet
    WHERE status = 'active'
      AND account_id NOT IN (SELECT account_id FROM sheet
                               WHERE placed_at >= '2025-12-01')
""").fetchone()
(from_accounts,) = con.execute("""
    SELECT COUNT(*) FROM accounts
    WHERE status = 'active'
      AND account_id NOT IN (SELECT account_id FROM sheet
                             WHERE placed_at >= '2025-12-01')
""").fetchone()
print("active accounts with no order in December")
print(f"  asked of the sheet             {from_sheet:>6,}")
print(f"  asked of the accounts table    {from_accounts:>6,}")

# One order at three grains, saved for Figure 4.2.
(pick,) = con.execute("""
    SELECT order_number FROM sheet
    WHERE placed_at >= '2025-12-01' AND segment = 'Mid-market'
    GROUP BY order_number HAVING COUNT(*) = 3
    ORDER BY order_number LIMIT 1
""").fetchone()
lines = con.execute("""
    SELECT order_number, line_no, sku, qty, revenue_cents, account_id
    FROM sheet WHERE order_number = ? ORDER BY line_no
""", [pick]).fetchall()
acct = lines[0][-1]
month = con.execute("""
    SELECT order_number, COUNT(*), SUM(qty), SUM(revenue_cents),
           MIN(placed_at)
    FROM sheet WHERE account_id = ?
      AND placed_at >= '2025-12-01'
    GROUP BY order_number ORDER BY MIN(placed_at)
""", [acct]).fetchall()
name, segment = con.execute("SELECT account_name, segment FROM accounts "
                            "WHERE account_id = ?", [acct]).fetchone()
with open("code/04/02_grain_check.json", "w") as f:
    json.dump({"order": pick, "account_id": acct, "account": name,
               "segment": segment, "lines": lines, "orders": month},
              f, indent=1)
