# Exercise 5.2: the pre-migration contracts file, read as text, typed on
# purpose, and reconciled with the new CRM's export of 4 January 2026.
from pathlib import Path

import duckdb

from groundwork.config import LANDING, SANDBOX
from meridian.sources import relay

path = Path(relay.export("2024-02-29", SANDBOX / "ch05" / "relay")[1])
lines = path.read_bytes().count(b"\r\n") - 1
con = duckdb.connect()
con.execute(f"""CREATE TABLE old AS SELECT * FROM read_csv('{path}',
    delim = ';', quote = '"', encoding = 'cp1252', header = true,
    new_line = '\\r\\n', all_varchar = true)""")
con.execute("""CREATE TABLE typed AS SELECT
    "Contract ID"::INTEGER                            AS contract_id,
    "Acct ID"::INTEGER                                AS account_id,
    strptime("Start", '%m/%d/%Y')::DATE               AS start_date,
    strptime("End", '%m/%d/%Y')::DATE                 AS end_date,
    "Term (months)"::INTEGER                          AS term_months,
    "Legacy Terms" = 'Y'                              AS legacy_terms,
    "Discount %"::INTEGER                             AS discount_pct,
    CASE "Outcome" WHEN 'Renewed' THEN 'renewed'
                   WHEN 'Not renewed' THEN 'not_renewed' END AS outcome,
    strptime("Notice Date", '%m/%d/%Y')::DATE         AS notice_date,
    "Cancellation Reason"                             AS reason
    FROM old""")
n = con.execute("SELECT count(*) FROM typed").fetchone()[0]
print(f"rows read {n:,}; data lines in the file {lines:,}")

new = LANDING / "relay" / "2026-01-04" / "contracts.csv"
con.execute(f"CREATE TABLE new AS FROM read_csv('{new}', "
            "all_varchar = true)")
cols = ["account_id", "start_date", "end_date", "term_months",
        "legacy_terms", "discount_pct", "outcome", "notice_date",
        "reason"]
both = con.execute("""SELECT count(*) FROM typed t JOIN new n
    ON t.contract_id = n.contract_id::INTEGER""").fetchone()[0]
print(f"in both exports {both:,}; rows that differ, and of those, how")
print("many were empty on 29 February 2024:")
for c in cols:
    theirs = {"reason": "cancellation_reason"}.get(c, c)
    differ, empty = con.execute(f"""SELECT count(*),
        count(*) FILTER (WHERE t.{c} IS NULL) FROM typed t JOIN new n
        ON t.contract_id = n.contract_id::INTEGER
        WHERE t.{c}::VARCHAR IS DISTINCT FROM
              nullif(n.{theirs}, '')""").fetchone()
    print(f"  {c:<13} {differ:>6,} {empty:>6,}")
