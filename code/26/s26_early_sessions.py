# Exercise 4: the sessions before their account's first version, and
# what pointing them at that first version would change.
from _sandbox import connect

con = connect()
early = """
    FROM marts.fct_portal_sessions AS s
    JOIN marts.dim_account AS u ON u.account_key = s.account_key
    JOIN (SELECT account_id, account_key, segment, valid_from,
                 customer_since
          FROM marts.dim_account
          QUALIFY row_number() OVER (PARTITION BY account_id
                                     ORDER BY valid_from) = 1) AS f
      ON f.account_id = s.account_id
    WHERE u.account_id = -1"""
n, accounts, carts, ordered, first, last = con.execute(f"""
    SELECT count(*), count(DISTINCT s.account_id),
           count(*) FILTER (s.add_to_carts > 0),
           count(*) FILTER (s.orders_submitted > 0),
           min(s.started_at)::DATE, max(s.started_at)::DATE {early}
""").fetchone()
print(f"{n} sessions by {accounts} accounts, {first} to {last}")
print(f"  with a cart {carts}, with an order {ordered}")
print("their accounts' first version, by the day it starts:")
for since, k, m in con.execute(f"""
        SELECT f.valid_from, count(DISTINCT s.account_id), count(*)
        {early} GROUP BY 1 ORDER BY 1""").fetchall():
    print(f"  {since}  {k:>4} accounts {m:>5} sessions")
print("by the segment that first version gives them:")
for segment, m in con.execute(f"""
        SELECT f.segment, count(*) {early} GROUP BY 1
        ORDER BY 2 DESC, 1""").fetchall():
    print(f"  {segment:<18}{m:>5}")
(conv,) = con.execute("""
    SELECT round(100.0 * count(*) FILTER (orders_submitted > 0)
                 / count(*), 2)
    FROM marts.fct_portal_sessions WHERE NOT is_bot""").fetchone()
print(f"portal conversion, bots out: {conv}%, whichever key they carry")
con.close()
