# Exercise 1: a margin percentage on each line looks like a measure,
# and averaging it gives a different answer from the true ratio.
# timeout: 600
from _facts import built

con = built()
print(f"{'2025 by channel':<16}{'ratio of sums':>15}"
      f"{'mean of lines':>15}{'mean, weighted':>16}")
for ch, ratio, mean, weighted in con.execute("""
        SELECT channel,
               100 * sum(gross_margin) / sum(revenue),
               avg(100 * gross_margin / revenue),
               sum(100 * gross_margin / revenue * revenue)
               / sum(revenue)
        FROM marts.fct_order_lines
        WHERE year(order_date) = 2025
        GROUP BY 1 ORDER BY 1""").fetchall():
    print(f"{ch:<16}{ratio:>14.2f}%{mean:>14.2f}%{weighted:>15.2f}%")
