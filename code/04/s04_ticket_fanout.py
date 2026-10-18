# Exercise 2: revenue of accounts with a ticket, fanned out and not.
# timeout: 240
import os

import duckdb
import httpx

from groundwork.config import NOW, TRADELINE_DB
from meridian.sources import Clock, frontline

# Every ticket's requester, paged from Frontline's API as of today.
base = frontline.serve(Clock(NOW))
token = os.getenv("FRONTLINE_TOKEN", "frontline-demo-token")
headers = {"Authorization": f"Bearer {token}"}
tickets, params = [], {"limit": 100}
with httpx.Client(base_url=base, headers=headers) as http:
    while True:
        page = http.get("/api/v2/tickets", params=params).json()
        tickets += [(t["id"], t["requester"]["account_id"],
                     t["created_at"]) for t in page["tickets"]]
        if not page["meta"]["has_more"]:
            break
        params["cursor"] = page["meta"]["after_cursor"]

con = duckdb.connect()
con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline "
            "(TYPE sqlite, READ_ONLY)")
con.execute("CREATE TABLE tickets (id VARCHAR, account_id BIGINT, "
            "created_at VARCHAR)")
con.executemany("INSERT INTO tickets VALUES (?, ?, ?)", tickets)
con.execute("""
    CREATE TABLE lines AS
    SELECT o.account_id, l.qty * l.unit_price_cents AS cents
    FROM tradeline.orders o
    JOIN tradeline.order_lines l ON l.order_id = o.id
    WHERE o.status = 'placed'
      AND o.placed_at >= '2025-01-01' AND o.placed_at < '2026-01-01'
""")
con.execute("""
    CREATE VIEW t25 AS SELECT * FROM tickets
    WHERE account_id IS NOT NULL
      AND created_at >= '2025-01-01' AND created_at < '2026-01-01'
""")
fanned = con.execute("""SELECT SUM(l.cents) FROM lines l
    JOIN t25 t ON t.account_id = l.account_id""").fetchone()[0]
exists = con.execute("""SELECT SUM(cents) FROM lines l
    WHERE EXISTS (SELECT 1 FROM t25 t
                  WHERE t.account_id = l.account_id)""").fetchone()[0]
first = con.execute("""
    SELECT SUM(r.cents) FROM
      (SELECT account_id, SUM(cents) AS cents FROM lines
       GROUP BY account_id) r
    JOIN (SELECT DISTINCT account_id FROM t25) t USING (account_id)
""").fetchone()[0]
(n,) = con.execute("SELECT COUNT(*) FROM t25").fetchone()
print(f"tickets paged from Frontline   {len(tickets):>16,}")
print(f"2025 tickets from customers    {n:>16,}")
print(f"joined row to row              {fanned / 100:>16,.2f}")
print(f"with EXISTS                    {exists / 100:>16,.2f}")
print(f"aggregated first               {first / 100:>16,.2f}")
print(f"the join overstated it {fanned / exists:.2f} times")
