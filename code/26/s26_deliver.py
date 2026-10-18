# Exercise 3: delivering audit.notifications, once each, to a file per
# recipient; run twice, the second run sends nothing.
import shutil
from datetime import datetime

import duckdb

from groundwork.config import SANDBOX

OUTBOX = SANDBOX / "ch26" / "outbox"
DDL = """
CREATE SCHEMA audit;
CREATE TABLE audit.notifications (sent_at TIMESTAMP, key VARCHAR,
    kind VARCHAR, recipient VARCHAR, text VARCHAR);
CREATE TABLE IF NOT EXISTS audit.deliveries (
    sent_at TIMESTAMP, key VARCHAR, kind VARCHAR, recipient VARCHAR,
    delivered_at TIMESTAMP NOT NULL,
    PRIMARY KEY (sent_at, key, kind, recipient));
"""


def deliver(con, now: datetime) -> int:
    """Every notification due by `now` and not yet delivered, appended
    to its recipient's file, then recorded. A crash between the two
    sends one message twice, never none: at least once."""
    due = con.execute("""
        SELECT n.* FROM audit.notifications AS n
        ANTI JOIN audit.deliveries AS d
          USING (sent_at, key, kind, recipient)
        WHERE n.sent_at <= ? ORDER BY n.sent_at, n.recipient, n.key""",
                      [now]).fetchall()
    OUTBOX.mkdir(parents=True, exist_ok=True)
    for sent_at, key, kind, to, text in due:
        with open(OUTBOX / f"{to.replace(' ', '-')}.txt", "a") as f:
            f.write(f"{sent_at:%Y-%m-%d %H:%M} [{kind}] {text}\n")
        con.execute("INSERT INTO audit.deliveries "
                    "VALUES (?, ?, ?, ?, ?)",
                    [sent_at, key, kind, to, now])
    return len(due)


shutil.rmtree(OUTBOX, ignore_errors=True)
con = duckdb.connect()
con.execute(DDL)
con.execute("""INSERT INTO audit.notifications VALUES
    ('2026-01-05 07:00', 'held:suppliers', 'ticket', 'data engineering',
     'suppliers.price_lists: 1 row parked'),
    ('2026-01-05 07:00', 'held:suppliers', 'copy', 'procurement',
     'suppliers.price_lists: 1 row parked'),
    ('2026-01-06 12:00', 'night:2026-01-05', 'page', 'on call',
     'nightly.2026-01-05.1 failed: ingest_relay')""")
for at in ("2026-01-05 07:05", "2026-01-05 07:10", "2026-01-06 12:05"):
    n = deliver(con, datetime.fromisoformat(at))
    print(f"delivery at {at[11:]} on {at[8:10]} Jan: {n} sent")
for path in sorted(OUTBOX.iterdir()):
    lines = path.read_text().splitlines()
    print(f"  {path.name}: {len(lines)} line(s)")
shutil.rmtree(OUTBOX)
