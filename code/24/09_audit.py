# A week of reading through audited sessions, then the questions the
# log answers: who read customers' personal data, and who was refused.
import textwrap
from datetime import datetime

from groundwork.govern import audit, classify, masking, roles

from _govern import tidy, work

copy = work("audit")
con = copy.connect()
masking.build(con)
roles.create(con)

WEEK = [
    ("Mon 09:40", "tomas", "analyst", """
        SELECT date_trunc('month', order_date) AS month, sum(revenue)
        FROM role_analyst.fct_orders GROUP BY 1 ORDER BY 1"""),
    ("Mon 10:05", "ada.k", "support", """
        SELECT ticket_id, status
        FROM role_support.stg_frontline__tickets
        WHERE requester_email = 'kevin.anderson@bexham-estates.example'
        """),
    ("Tue 14:20", "tomas", "analyst", """
        SELECT count(*) FROM raw.frontline_ticket_comments"""),
    ("Tue 14:31", "tomas", "analyst", """
        SELECT count(*) FROM role_analyst.stg_frontline__ticket_comments
        WHERE body_redacted LIKE '%[phone]%'"""),
    ("Wed 08:55", "jo.f", "finance", """
        SELECT sum(amount_cents)
        FROM role_finance.stg_ledger__gl_revenue"""),
    ("Thu 02:47", "sam.eng", "engineer", """
        SELECT ticket_id, body FROM raw.frontline_ticket_comments
        WHERE body LIKE '%555-01%' LIMIT 20"""),
    ("Thu 11:02", "ada.k", "support", """
        SELECT c.body FROM role_support.stg_frontline__ticket_comments c
        WHERE c.ticket_id = 'FT-012456'"""),
]
day = {"Mon": 5, "Tue": 6, "Wed": 7, "Thu": 8}
for when, user, role, query in WEEK:
    at = datetime(2026, 1, day[when[:3]], int(when[4:6]), int(when[7:]))
    try:
        audit.Session(con, user, role).sql(query, at)
    except audit.Denied:
        pass

print(f"{'no':>2} {'at':<10}{'user':<8}{'role':<9}{'rows':>6}  read")
for no, at, user, role, rels, ok, _, rows, _ in con.execute(
        "SELECT * FROM audit.query_log ORDER BY query_no").fetchall():
    shown = rows if ok else "denied"
    rel = rels[0].replace(f"role_{role}.", "")
    print(f"{no:>2} {at:%a %H:%M} {user:<8}{role:<9}{shown:>6}  {rel}")

wanted = {c.relation for c in classify.classify(con, ("raw", "staging"))
          if c.cls == classify.PERSONAL and "colleague" not in c.reason}
print("\nwho read customers' personal data, 5 to 9 January:")
for user, role, src, n, glass in audit.who_read(
        con, wanted, datetime(2026, 1, 5), datetime(2026, 1, 10)):
    flag = "  break-glass" if glass else ""
    print(f"  {user:<8}{src:<39}{n}{flag}")
(text,) = con.execute(
    "SELECT query FROM audit.query_log WHERE query_no = 2").fetchone()
print("\nwhat the log kept of query 2:")
for line in textwrap.wrap(text, 64):
    print(f"  {line}")
con.close()
tidy("audit")
