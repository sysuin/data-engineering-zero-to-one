# Sales by account manager, credited to today's manager and to the
# manager each account had on the day it ordered.
import json
from pathlib import Path

from _dims import built, tidy

con = built("overwrite")
con.execute("""
CREATE TEMP TABLE sales AS
SELECT o.order_id, i.account_id, l.cents,
       CAST(timezone(r.timezone, timezone('UTC', o.placed_at))
            AS DATE) AS order_date
FROM staging.stg_tradeline__orders AS o
JOIN (SELECT order_id, sum(qty * unit_price_cents) AS cents
      FROM staging.stg_tradeline__order_lines GROUP BY ALL) AS l
    USING (order_id)
JOIN intermediate.int_account_identity AS i ON i.crm_id = o.account_id
JOIN marts.dim_account AS now
    ON now.account_id = i.account_id AND now.is_current
JOIN marts.dim_region AS r ON r.region_key = now.region_key
WHERE o.status = 'placed'""")
rows = con.execute("""
SELECT s.*, now.owner AS today, v.owner AS that_day
FROM sales AS s
JOIN marts.dim_account AS now
    ON now.account_id = s.account_id AND now.is_current
JOIN marts.dim_account AS v
    ON v.account_id = s.account_id AND s.order_date >= v.valid_from
   AND (s.order_date < v.valid_to OR v.valid_to IS NULL)
WHERE year(s.order_date) = 2025""").fetchall()

def by(owner_of):
    out = {}
    for r in rows:
        n, c = out.get(owner_of(r), (0, 0))
        out[owner_of(r)] = (n + 1, c + r[2])
    return out

today, that_day = by(lambda r: r[4]), by(lambda r: r[5])
print(f"2025: {len(rows):,} placed orders, "
      f"${sum(r[2] for r in rows) / 100:,.2f}")
print(f"{'account manager':<16}{'today (Type 1)':>26}"
      f"{'that day (Type 2)':>26}")
moved = sorted(today, key=lambda o: -abs(today[o][1]
                                         - that_day.get(o, (0, 0))[1]))
for owner in moved[:4]:
    (n1, c1), (n2, c2) = today[owner], that_day.get(owner, (0, 0))
    print(f"{owner:<16}{n1:>8,}{c1 / 100:>18,.2f}"
          f"{n2:>8,}{c2 / 100:>18,.2f}")
other = sum(r[4] != r[5] for r in rows)
print(f"orders whose manager then is not today's: {other:,} "
      f"({other / len(rows):.1%})")

# One account's managers, for Figure 12.1.
v = con.execute("SELECT valid_from, valid_to, owner, status FROM "
                "marts.dim_account WHERE account_id = 1130 "
                "ORDER BY valid_from").fetchall()
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"versions": [[str(a), str(b) if b else None, o, s]
                  for a, b, o, s in v]}, indent=1))
tidy("overwrite")
