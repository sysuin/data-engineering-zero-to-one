# A periodic snapshot: contracts in force at each month end, checked
# against Chapter 4's count, summed the right way and the wrong way.
# timeout: 600
from groundwork.config import BOOK2_WAREHOUSE

from _facts import built

con = built()
con.execute(f"ATTACH '{BOOK2_WAREHOUSE}' AS b2 (TYPE sqlite, "
            "READ_ONLY)")
(differ,) = con.execute("""
    SELECT count(*) FROM (
        SELECT contract_id, strftime(start_date, '%Y-%m-%d'),
               strftime(end_date, '%Y-%m-%d'), term_months,
               legacy_terms::INT, discount_pct, outcome,
               strftime(strptime(notice_date_key::VARCHAR, '%Y%m%d'),
                        '%Y-%m-%d'), cancellation_reason
        FROM marts.fct_contracts
        EXCEPT
        SELECT contract_id, start_date, end_date, term_months,
               legacy_terms, discount_pct, outcome, notice_date,
               cancellation_reason FROM b2.contracts)""").fetchone()
n, b2 = con.execute("SELECT (SELECT count(*) FROM marts.fct_contracts),"
                    " (SELECT count(*) FROM b2.contracts)").fetchone()
print(f"fct_contracts {n:,}, Book 2's contracts {b2:,}; "
      f"differ {differ}")
(early,) = con.execute("""
    SELECT count(*) FROM marts.fct_contracts
    WHERE start_date < DATE '2022-01-01'""").fetchone()
print(f"contracts that began before any account history: {early:,}")

n, first, last = con.execute("""
    SELECT count(*), min(month_end), max(month_end)
    FROM marts.fct_contract_month""").fetchone()
print(f"\nfct_contract_month {n:,} rows, {first} to {last}")
print(f"{'month end':<12}{'in force':>9}{'ending in 3 mo':>16}"
      f"{'avg discount':>14}")
for m, k, ending, disc in con.execute("""
        SELECT month_end, sum(contracts_in_force),
               count(*) FILTER (months_remaining <= 2),
               avg(discount_pct)
        FROM marts.fct_contract_month
        WHERE month(month_end) = 12
        GROUP BY 1 ORDER BY 1""").fetchall():
    print(f"{m!s:<12}{k:>9,}{ending:>16,}{disc:>13.2f}%")

summed, last, mean = con.execute("""
    WITH m AS (SELECT month_end, sum(contracts_in_force) AS k
               FROM marts.fct_contract_month
               WHERE year(month_end) = 2025 GROUP BY 1)
    SELECT sum(k), arg_max(k, month_end), avg(k) FROM m""").fetchone()
print(f"\n2025, contracts in force: summed over twelve month ends "
      f"{summed:,}")
print(f"  at the year end {last:,}, the average month end {mean:,.0f}")
