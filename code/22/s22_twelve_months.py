# Exercise 1: accounts that ordered in the last twelve months, beside
# active_accounts, at every month end of 2025.
# timeout: 300
from datetime import date

import duckdb

from groundwork.serve import metrics

from _serve import tidy, warehouse

con = duckdb.connect(str(warehouse()), read_only=True)
active = {r["month"]: r["active_accounts"] for r in metrics.query(
    con, "active_accounts", grain="month", start=date(2025, 1, 1),
    end=date(2026, 1, 1))}
print(f"{'month end':<12}{'ordered 12 mo':>14}{'under contract':>16}")
for (m,) in con.execute("""SELECT DISTINCT month_end FROM
        marts.fct_contract_month WHERE year(month_end) = 2025
        ORDER BY 1""").fetchall():
    (n,) = con.execute("""
        SELECT count(DISTINCT a.account_id) FROM marts.fct_orders AS o
        JOIN marts.dim_account AS a USING (account_key)
        WHERE o.order_date > ? - INTERVAL 1 YEAR
          AND o.order_date <= ?""", [m, m]).fetchone()
    print(f"{m!s:<12}{n:>14,}{active[m.replace(day=1)]:>16,}")
con.execute("""CREATE TEMP TABLE o AS SELECT DISTINCT a.account_id
    FROM marts.fct_orders AS o JOIN marts.dim_account AS a
    USING (account_key) WHERE o.order_date > DATE '2024-12-31'""")
con.execute("""CREATE TEMP TABLE c AS SELECT DISTINCT a.account_id
    FROM marts.fct_contract_month JOIN marts.dim_account AS a
    USING (account_key) WHERE month_end = DATE '2025-12-31'""")
(only_o,) = con.execute("SELECT count(*) FROM o ANTI JOIN c "
                        "USING (account_id)").fetchone()
(only_c,) = con.execute("SELECT count(*) FROM c ANTI JOIN o "
                        "USING (account_id)").fetchone()
print(f"on 31 December: ordered, no contract {only_o:,}; "
      f"contract, no order {only_c:,}")
con.close()
tidy("warehouse.duckdb")
