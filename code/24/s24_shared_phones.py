# How many contacts share each phone number in today's Relay export,
# and how many people a deletion keyed on a phone would touch.
from _govern import base

con = base().connect(read_only=True)
numbers, alone, median, most, contacts = con.execute("""
    SELECT count(*), count(*) FILTER (WHERE n = 1), median(n), max(n),
           sum(n)
    FROM (SELECT contact_phone, count(*) AS n FROM raw.relay_accounts
          WHERE load_date = DATE '2026-01-04' GROUP BY 1)""").fetchone()
print(f"{contacts:,} contacts, {numbers} phone numbers")
print(f"numbers held by one contact only: {alone}")
print(f"contacts sharing a number: median {median:.0f}, most {most}")
(emails,) = con.execute("""
    SELECT count(DISTINCT lower(contact_email)) FROM raw.relay_accounts
    WHERE load_date = DATE '2026-01-04'""").fetchone()
print(f"distinct emails: {emails:,}")
