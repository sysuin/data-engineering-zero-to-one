# Three type choices that change an answer: zones, money and casts.
import duckdb

from groundwork.config import LANDING, THREADS, TRADELINE_DB

relay = LANDING / "relay" / "2026-01-04"
con = duckdb.connect(config={"threads": THREADS})
con.execute(f"ATTACH '{TRADELINE_DB}' AS tl (TYPE sqlite, READ_ONLY)")

# A string ending in Z is read as TIMESTAMPTZ, whose date follows the
# session; read as TIMESTAMP, it is UTC whatever the session says.
guess = f"read_csv('{relay}/accounts.csv')"
typed = (f"read_csv('{relay}/accounts.csv', "
         "types = {'updated_at': 'TIMESTAMP'})")
print("accounts updated on 31 December 2025, by session time zone")
for zone in ["UTC", "America/Los_Angeles"]:
    con.execute(f"SET TimeZone = '{zone}'")
    counts = [con.execute(
        f"SELECT count(*) FROM {src} "
        "WHERE CAST(updated_at AS DATE) = '2025-12-31'").fetchone()[0]
        for src in (guess, typed)]
    print(f"  {zone:<20} TIMESTAMPTZ {counts[0]:>4}"
          f"   TIMESTAMP {counts[1]:>4}")
con.execute("SET TimeZone = 'UTC'")
(same,) = con.execute(f"""SELECT count(*) FROM {typed} AS t
    JOIN read_csv('{relay}/accounts.csv', all_varchar = true) AS v
    USING (account_id) WHERE t.updated_at =
    strptime(rtrim(v.updated_at, 'Z'), '%Y-%m-%dT%H:%M:%S')
""").fetchone()
print(f"  types= and strptime agree on {same:,} rows")

# Money: integer cents are exact; a DECIMAL divided becomes a DOUBLE.
cents, decimal, double = con.execute("""
    SELECT sum(qty * unit_price_cents),
           sum(qty * unit_price_cents * 0.01),
           sum(qty * unit_price_cents / 100)
    FROM tl.order_lines""").fetchone()
print(f"revenue as cents {cents:,}  DECIMAL {decimal}")
print(f"        as DOUBLE {double!r}")
for expr in ["CAST(9999 AS DECIMAL(18, 2)) / 100",
             "CAST(9999 AS DECIMAL(18, 2)) * 0.01",
             "9999 // 100", "9999 / 100"]:
    kind, value = con.execute(
        f"SELECT typeof({expr}), {expr}").fetchone()
    print(f"  {expr:<36}{kind:<15}{value}")

# CAST stops at the first bad value; TRY_CAST makes it NULL instead.
catalogue = (f"read_csv('{LANDING}/catalogue/catalogue.csv', "
             "all_varchar = true)")
try:
    con.execute(f'SELECT CAST("List Price" AS DECIMAL(12, 2)) '
                f"FROM {catalogue}").fetchall()
except duckdb.ConversionException as e:
    print("CAST:", str(e).split(": ", 1)[1].split(" when ")[0])
total, failed, fixed = con.execute(f"""
    SELECT count(*),
           count(*) FILTER (TRY_CAST("List Price" AS DECIMAL(12, 2))
                            IS NULL),
           count(*) FILTER (TRY_CAST(replace("List Price", '$', '')
                                     AS DECIMAL(12, 2)) IS NULL)
    FROM {catalogue}""").fetchone()
print(f"TRY_CAST: NULL for {failed:,} of {total:,} list prices; "
      f"{fixed} once '$' is gone")
