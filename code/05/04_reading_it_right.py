# The pre-migration export read right, and checked against today's.
from pathlib import Path

import duckdb

from groundwork.config import LANDING, SANDBOX
from meridian.sources import relay

path = Path(relay.export("2024-02-29", SANDBOX / "ch05" / "relay")[0])
lines = path.read_bytes().count(b"\r\n") - 1      # less the header
con = duckdb.connect()

# Say everything; guess nothing. Every column arrives as text.
con.execute(f"""CREATE TABLE old AS SELECT * FROM read_csv('{path}',
    delim = ';', quote = '"', encoding = 'cp1252', header = true,
    new_line = '\\r\\n', all_varchar = true)""")

# Then type and repair each column on purpose, in SQL you can read.
con.execute("""CREATE TABLE fixed AS SELECT
    "Acct ID"::INTEGER                             AS account_id,
    strptime("Customer Since", '%m/%d/%Y')::DATE   AS customer_since,
    lpad("Zip", 5, '0')                            AS postcode,
    split_part("Primary Contact", ', ', 2) || ' ' ||
        split_part("Primary Contact", ', ', 1)     AS contact_name
    FROM old""")
n = con.execute("SELECT count(*) FROM fixed").fetchone()[0]
print(f"rows read {n:,}; data lines in the file {lines:,}")
print(con.execute("SELECT * FROM fixed WHERE account_id = 2485")
      .fetchone())

# The same accounts as the new CRM exported them on 4 January 2026.
new = LANDING / "relay" / "2026-01-04" / "accounts.csv"
con.execute(f"""CREATE TABLE new AS SELECT * FROM read_csv('{new}',
    all_varchar = true)""")
both, pc, since, contact = con.execute("""SELECT count(*),
    count(*) FILTER (WHERE f.postcode = n.postcode),
    count(*) FILTER (WHERE f.customer_since = n.customer_since::DATE),
    count(*) FILTER (WHERE f.contact_name = n.contact_name)
    FROM fixed f JOIN new n ON f.account_id = n.account_id::INTEGER
    """).fetchone()
print(f"in both exports {both:,}: postcode agrees {pc:,}, "
      f"customer since {since:,},")
print(f"  contact name {contact:,}; ids only in the old export "
      f"{n - both:,}")
