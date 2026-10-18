# Where the old CRM's numbers-that-are-not-numbers went wrong.
from pathlib import Path

import duckdb

from groundwork.config import SANDBOX
from meridian.sources import relay

path = Path(relay.export("2024-02-29", SANDBOX / "ch05" / "relay")[0])
text = f"""read_csv('{path}', delim = ';', encoding = 'cp1252',
                    all_varchar = true)"""
con = duckdb.connect()

print("region      zips  4 digits  lowest  highest")
for row in con.execute(f"""
        SELECT "Region", count(*),
               count(*) FILTER (WHERE length("Zip") = 4),
               min("Zip"), max("Zip")
        FROM {text} GROUP BY 1 ORDER BY 1""").fetchall():
    print("{:<10} {:>5,} {:>9,}  {:>6}  {:>7}".format(*row))

# Dates: which could be read either way round?
print("\ncolumn          filled  both parts <= 12")
for col in ("Customer Since", "Closed Date"):
    n, both = con.execute(f"""
        SELECT count("{col}"), count(*) FILTER (WHERE
            split_part("{col}", '/', 1)::int <= 12 AND
            split_part("{col}", '/', 2)::int <= 12)
        FROM {text}""").fetchone()
    print(f"{col:<15} {n:>6,}  {both:>16,}")
types = dict(con.execute(f"""SELECT column_name, column_type FROM
    (DESCRIBE SELECT * FROM read_csv('{path}', encoding = 'cp1252'))
    """).fetchall())
print(f"guessed types:  Customer Since {types['Customer Since']}, "
      f"Closed Date {types['Closed Date']}")

# Long numbers: a spreadsheet keeps every number as a double.
for digits in ("9007199254740993", "40000000000000123"):
    print(f"\n{digits} as a double: {int(float(digits))}", end="")
print()
