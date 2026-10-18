# Build dim_date, then join the orders to it on each of their two dates.
import duckdb

from groundwork import dates
from groundwork.config import SANDBOX
from groundwork.ddl import create

con = duckdb.connect(str(SANDBOX / "ch04" / "listings.duckdb"))
create(con, "marts")
n = dates.load(con)
first, last, gaps = con.execute("""
    SELECT MIN(date), MAX(date),
           COUNT(*) FILTER (WHERE date - prev <> 1)
    FROM (SELECT date, LAG(date) OVER (ORDER BY date) AS prev
          FROM marts.dim_date)
""").fetchone()
print(f"dim_date: {n:,} rows, {first} to {last}, {gaps} gaps")

print(f"\n{'date_key':<10}{'day':<11}{'work':>5}{'nth':>5}"
      f"{'iso week':>10}{'month end':>11}")
for key, day, work, nth, iy, iw, me in con.execute("""
    SELECT date_key, day_name, is_working_day, working_day_of_month,
           iso_year, iso_week, is_month_end
    FROM marts.dim_date
    WHERE date BETWEEN '2025-12-31' AND '2026-01-07'
    ORDER BY date_key
""").fetchall():
    line = (f"{key:<10}{day:<11}{'yes' if work else 'no':>5}"
            f"{nth if nth else '':>5}{f'{iy}-W{iw:02d}':>10}"
            f"{'yes' if me else '':>11}")
    print(line.rstrip())

# Every placed order, joined to its day twice: by UTC date and by the
# depot's date. Which calendar facts change?
rows = con.execute("""
    SELECT COUNT(*),
           COUNT(*) FILTER (WHERE u.day_name <> d.day_name),
           COUNT(*) FILTER (WHERE d.is_working_day
                            AND NOT u.is_working_day),
           COUNT(*) FILTER (WHERE NOT d.is_working_day
                            AND u.is_working_day),
           COUNT(*) FILTER (WHERE u.month <> d.month),
           COUNT(*) FILTER (WHERE u.year <> d.year)
    FROM orders_local o
    JOIN marts.dim_date u ON u.date = o.utc_date
    JOIN marts.dim_date d ON d.date = o.local_date
""").fetchone()
labels = ["orders joined on both dates",
          "a different day of the week",
          "a working day that UTC calls a weekend",
          "a weekend day that UTC calls a working day",
          "a different month",
          "a different year"]
print()
for label, value in zip(labels, rows):
    print(f"{label:<44}{value:>9,}")
