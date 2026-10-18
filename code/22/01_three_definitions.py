# Three dashboards' "active accounts" at the end of 2025, computed as
# each one computes it, and how far apart they are.
# timeout: 300
from datetime import date

import duckdb

from _serve import warehouse

con = duckdb.connect(str(warehouse()), read_only=True)
DEFINITIONS = {
    "sales: ordered in the last 90 days": """
        SELECT DISTINCT a.account_id FROM marts.fct_orders AS o
        JOIN marts.dim_account AS a USING (account_key)
        WHERE o.order_date > $day - INTERVAL 90 DAY
          AND o.order_date <= $day""",
    "CRM: status is active": """
        SELECT account_id FROM marts.dim_account
        WHERE status = 'active' AND $day >= valid_from
          AND $day < coalesce(valid_to, DATE '9999-12-31')""",
    "account team: under contract": """
        SELECT DISTINCT a.account_id FROM marts.fct_contract_month
        JOIN marts.dim_account AS a USING (account_key)
        WHERE month_end = $day""",
}


def accounts(sql, day):
    return {a for (a,) in con.execute(sql, {"day": day}).fetchall()}


day = date(2025, 12, 31)
sets = {name: accounts(sql, day) for name, sql in DEFINITIONS.items()}
print(f"Active accounts on {day}, three ways")
for name, s in sets.items():
    print(f"  {name:<38}{len(s):>6,}")
sales, crm, team = sets.values()
print(f"  {'in all three':<38}{len(sales & crm & team):>6,}")
print(f"  {'in at least one':<38}{len(sales | crm | team):>6,}")
print("Where they part:")
for label, n in (
        ("under contract, no order in 90 days", len(team - sales)),
        ("ordered, no contract in force", len(sales - team)),
        ("under contract, CRM says closed", len(team - crm)),
        ("CRM active, no order in 90 days", len(crm - sales))):
    print(f"  {label:<38}{n:>6,}")

months = [m for (m,) in con.execute("""
    SELECT DISTINCT month_end FROM marts.fct_contract_month
    WHERE month_end >= DATE '2024-01-01' ORDER BY 1""").fetchall()]
series = {name: [len(accounts(sql, m)) for m in months]
          for name, sql in DEFINITIONS.items()}
spread = [max(v) - min(v) for v in zip(*series.values())]
print(f"Month ends from {months[0]} to {months[-1]}: the three "
      f"differ by\n  {min(spread)} to {max(spread)} accounts")
