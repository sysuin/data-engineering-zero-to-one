# An international pattern added to the scanner, measured on all of
# Frontline's free text: what it finds, and what it finds wrongly.
import re

from groundwork.govern.classify import PHONE

from _govern import base

INTL = r"\+[1-9][0-9 ().-]{7,}[0-9]"
con = base().connect(read_only=True)
texts = [t for (t,) in con.execute("""
    SELECT body FROM raw.frontline_ticket_comments
    UNION ALL SELECT description FROM raw.frontline_tickets
    """).fetchall() if t]
for label, pattern in (("phone", PHONE), ("international", INTL),
                       ("either", f"({PHONE})|({INTL})")):
    rx = re.compile(pattern)
    print(f"{label:<14}{sum(1 for t in texts if rx.search(t)):>6,} "
          f"of {len(texts):,} texts")
