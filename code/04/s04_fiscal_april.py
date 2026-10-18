# Exercise 3: dim_date with a financial year that starts in April.
from datetime import date

from groundwork import dates

cols = dates.COLUMNS
for start in (1, 4):
    rows = {r[1]: dict(zip(cols, r)) for r in dates.build(
        date(2025, 1, 1), date(2025, 12, 31), fiscal_start_month=start)}
    print(f"financial year starting in month {start}")
    for d in (date(2025, 3, 31), date(2025, 4, 1), date(2025, 12, 31)):
        r = rows[d]
        print(f"  {d}  year {r['year']}  FY{r['fiscal_year']} "
              f"Q{r['fiscal_quarter']}  period {r['fiscal_month']:>2}")

a = dates.build(fiscal_start_month=1)
b = dates.build(fiscal_start_month=4)
changed = [c for i, c in enumerate(cols)
           if any(x[i] != y[i] for x, y in zip(a, b))]
print(f"columns that change: {', '.join(changed)}")
