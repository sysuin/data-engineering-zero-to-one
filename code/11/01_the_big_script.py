# The reporting job's sales pack: one SQL file that reads every source
# where it lives, cleans it, joins it and sums it, four times over.
import re
from pathlib import Path

import duckdb

from groundwork.config import LANDING, REFERENCE, TRADELINE_DB

script = Path(__file__).with_name("reporting_job.sql").read_text()
lines = script.splitlines()
sections = [ln for ln in lines if re.match(r"-- \d\. ", ln)]
print(f"{len(lines)} lines, {len(sections)} sections, "
      f"{script.count('WITH lines AS')} copies of the same CTE")
print(f"Tradeline's order_lines read "
      f"{script.count('FROM tradeline.order_lines')} times, "
      f"the catalogue {script.count('$CATALOGUE')} times")
skus = re.findall(r"^\s+SELECT (.*\"SKU\".*) AS sku,", script, re.M)
prices = re.findall(r"CAST\(replace\(\"List Price\", '\$', ''\) "
                    r"AS (\w+(?:\(\d+, \d+\))?)\)", script)
print("the catalogue's SKU, as each copy reads it:")
print("  " + ", ".join(skus))
print("its list price, as each copy types it:")
print("  " + ", ".join(prices))

con = duckdb.connect()
con.execute(f"ATTACH '{TRADELINE_DB}' AS tradeline (TYPE sqlite, "
            "READ_ONLY)")
con.execute(script
            .replace("$CATALOGUE", str(LANDING / "catalogue" /
                                       "catalogue.csv"))
            .replace("$SUPPLIERS", str(REFERENCE / "suppliers.csv")))

print("\nThe pack's grand totals, every month added up:")
print(f"  {'section':<14}{'lines':>12}{'sales':>20}")
for name in ("channel", "category", "supplier", "realisation"):
    count = "sum(lines)" if name != "realisation" else "NULL"
    n, cents = con.execute(
        f"SELECT {count}, sum(value_cents) FROM pack_{name}").fetchone()
    shown = f"{n:,}" if n is not None else "-"
    usd = f"${cents // 100:,}.{cents % 100:02d}"
    print(f"  {name:<14}{shown:>12}{usd:>20}")
