# Exercise 2: two wrong versions of compat, measured against Book 2 by
# their row counts and row by row.
import sqlite3

from _sandbox import connect

from groundwork import reconcile_books as rb
from groundwork.config import BOOK2_WAREHOUSE

con = connect()
rb._attach(con, BOOK2_WAREHOUSE, "book2")
n_b2, n_gw, differ = rb.compare(con, "book2.orders", """(
    SELECT o.order_number AS order_id, a.account_id,
           strftime(o.order_date, '%Y-%m-%d') AS order_date, o.channel
    FROM marts.fct_orders AS o
    JOIN marts.dim_account AS a USING (account_key))""")
print("1. compat.orders with the marts' account ids")
print(f"  rows: Book 2 {n_b2:,}, Groundwork {n_gw:,}; "
      f"differing {differ:,}")

b2 = sqlite3.connect(f"file:{BOOK2_WAREHOUSE}?mode=ro", uri=True)
rows = b2.execute("SELECT * FROM account_history ORDER BY account_id, "
                  "valid_from, rowid").fetchall()
now = b2.execute("SELECT account_id, segment, account_manager "
                 "FROM accounts").fetchall()
b2.close()
expected, _ = rb._rules()(rows, now)
every = set(con.execute("""
    SELECT account_id, strftime(valid_from, '%Y-%m-%d'),
           strftime(lead(valid_from) OVER (PARTITION BY account_id
                    ORDER BY valid_from) - 1, '%Y-%m-%d'),
           segment, owner
    FROM marts.dim_account WHERE account_id > 0""").fetchall())
print("2. compat.account_history keeping status-only versions")
print(f"  rows: Book 2 under the rules {len(expected):,}, "
      f"Groundwork {len(every):,};")
print(f"  differing {len(every ^ expected):,}")
con.close()
