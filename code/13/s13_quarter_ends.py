# Exercise 4: contracts in force by segment at each 2025 quarter end,
# from the periodic snapshot, and the sum that means nothing.
# timeout: 600
from _facts import built

con = built()
rows = con.execute("""
    SELECT a.segment, f.month_end, sum(f.contracts_in_force)
    FROM marts.fct_contract_month AS f
    JOIN marts.dim_account AS a USING (account_key)
    WHERE f.month_end IN ('2025-03-31', '2025-06-30', '2025-09-30',
                          '2025-12-31')
    GROUP BY ALL ORDER BY 1, 2""").fetchall()
ends = sorted({r[1] for r in rows})
print(f"{'segment':<16}" + "".join(f"{e:%b %d}".rjust(9) for e in ends)
      + f"{'summed':>9}")
for seg in sorted({r[0] for r in rows}):
    k = {e: n for s, e, n in rows if s == seg}
    print(f"{seg:<16}" + "".join(f"{k.get(e, 0):>9,}" for e in ends)
          + f"{sum(k.values()):>9,}")
