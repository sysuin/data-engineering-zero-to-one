# An accumulating snapshot: what Frontline can say about a ticket's
# milestones, one ticket's row filling in night by night, a quarter of
# nightly pulls, and what Groundwork's first pull could know.
# timeout: 900
import json
from datetime import datetime, timedelta
from pathlib import Path

from meridian.sources import Clock, frontline

from groundwork.ingest.frontline import Client, pull

from _facts import built

con = built()
n, by_requester, public = con.execute("""
    SELECT count(*), count(*) FILTER (c.author = t.requester_name),
           count(*) FILTER (c.is_public)
    FROM staging.stg_frontline__ticket_comments AS c
    JOIN staging.stg_frontline__tickets AS t USING (ticket_id)
    """).fetchone()
print(f"comments {n:,}: by the ticket's requester {by_requester:,}, "
      f"public {public:,}")

MILESTONES = {"open": "first_reply_at", "solved": "solved_at",
              "closed": "closed_at"}


def fill(row, t):
    """One night's version: fill each milestone the first time seen."""
    col = MILESTONES.get(t["status"])
    if col and row.get(col) is None:
        row[col] = t["updated_at"][:16].replace("T", " ")


# One ticket, asked for at every nightly run from the night it opened.
clock = Clock("2025-11-04 06:00")
client = Client(frontline.serve(clock))
TICKET = "FT-020844"
row, nights = {}, []
print(f"\n{TICKET}, at the nightly runs (06:00 UTC) that "
      "changed its row")
print(f"{'run':<11}{'status':<8}{'updated_at':<18}{'row now holds'}")
while "closed_at" not in row:
    t = client.get(f"/api/v2/tickets/{TICKET}", {}).json()["ticket"]
    before = set(row)
    row.setdefault("opened_at", t["created_at"][:16].replace("T", " "))
    fill(row, t)
    new = ", ".join(k for k in row if k not in before) or "-"
    nights.append([f"{clock.now:%Y-%m-%d}", t["status"],
                   t["updated_at"], dict(row)])
    if new != "-":
        print(f"{clock.now:%a %d %b} {t['status']:<8}"
              f"{t['updated_at'][5:16].replace('T', ' '):<18}{new}")
    clock.advance(days=1)
print(f"{len(nights)} runs; on the other {len(nights) - 4} nothing new")

# The same ticket asked for every ten minutes of its first day.
clock.set(nights[0][3]["opened_at"])
last, steps = None, []
print(f"\n{TICKET} every ten minutes from its opening")
while clock.now.hour < 6 or clock.now.day == 3:
    t = client.get(f"/api/v2/tickets/{TICKET}", {}).json()["ticket"]
    now = (t["status"], t["priority"], t["assignee"])
    if now != last:
        print(f"  {clock.now:%d %b %H:%M}  {t['status']:<7}"
              f"{t['priority']:<8}{t['assignee'] or '-':<10}"
              f"updated_at {t['updated_at'][11:16]}")
        last = now
        steps.append([t["updated_at"][11:16], *now])
    clock.advance(minutes=10)

# Every ticket opened in October and November 2025, pulled nightly.
clock.set("2025-10-01 06:00")
rows = {}
since = "2025-10-01T00:00:00Z"
while clock.now < datetime(2026, 1, 5, 6, 0, tzinfo=clock.now.tzinfo):
    for body in pull(client, since):
        for t in json.loads(body)["tickets"]:
            if "2025-10-01" <= t["created_at"] < "2025-12-01":
                fill(rows.setdefault(t["id"], {}), t)
    since = (clock.now - timedelta(hours=1)).strftime(
        "%Y-%m-%dT%H:%M:%SZ")
    clock.advance(days=1)
client.close()
seen = {c: sum(c in r for r in rows.values())
        for c in MILESTONES.values()}
print(f"\nopened Oct-Nov 2025: {len(rows):,} tickets, nightly pulls")
for c, k in seen.items():
    print(f"  {c:<16}seen {k:>5,}   never seen {len(rows) - k:>4,}")

print("\nGroundwork's first pull (4 January 2026): fct_tickets")
print(f"{'status':<8}{'tickets':>8}{'first_reply':>13}{'solved':>8}"
      f"{'closed':>8}")
for r in con.execute("""
        SELECT status, count(*), count(first_reply_at),
               count(solved_at), count(closed_at)
        FROM marts.fct_tickets
        GROUP BY 1 ORDER BY 2 DESC""").fetchall():
    print(f"{r[0]:<8}{r[1]:>8,}{r[2]:>13,}{r[3]:>8,}{r[4]:>8,}")
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"ticket": TICKET, "nights": nights, "steps": steps, "seen": seen,
     "opened": len(rows)}, indent=1))
