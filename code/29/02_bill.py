# Chapter 25's finance design, priced: its units times list prices.
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "cost", "code/25/02_cost_model.py")
cost = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cost)

# List prices in USD, US East, read from each vendor's own pricing
# page or published price list on 4 October 2026. They will change.
S3_GB_MO = 0.023          # AWS S3 Standard, N. Virginia, first 50 TB
GCS_GIB_MO = 0.023        # Cloud Storage Standard, us-east4
BLOB_GB_MO = 0.0208       # Azure Blob, hot, LRS, East US, first 50 TB
BQ_TIB = 6.25             # BigQuery on-demand, US, per TiB scanned
SNOW_TB_MO = 23.00        # Snowflake on-demand storage, AWS US East
CREDIT = {"Standard": 2.00, "Enterprise": 3.00}   # Snowflake, a credit
XS_CREDITS_H = 1          # an X-Small warehouse, credits an hour
MIN_S, SUSPEND_S = 60, 600    # Snowflake: least billed a start, and
                              # idle seconds before it suspends, default
DBU = 0.70                # Databricks SQL Serverless, AWS US East

XS_CORES = 8              # assumed, not published: the cost model's
                          # cores an X-Small warehouse does the work of
TB_PER_TIB = 2**40 / 1e12     # Google prices in binary units;
GB_PER_GIB = 2**30 / 1e9      # the cost model counts in decimal ones

PRICES = {  # dollars per unit of Chapter 25's cost model
    "BigQuery": {"stored GB-mo": GCS_GIB_MO / GB_PER_GIB,
                 "queries TB": BQ_TIB / TB_PER_TIB,
                 "runs TB": BQ_TIB / TB_PER_TIB},
    **{f"Snowflake {edition}": {
        "stored GB-mo": SNOW_TB_MO / 1e3,
        "core-h": price * XS_CREDITS_H / XS_CORES}
       for edition, price in CREDIT.items()}}


def dollars(x: float) -> str:
    """As a price list prints it: cents, or the digits it gives."""
    return f"{x:,.2f}" if x >= 0.1 else f"{x:.3g}"


print("list prices, US East, 4 October 2026")
for label, price, unit in (
        ("S3 Standard", S3_GB_MO, "a GB-month"),
        ("Cloud Storage Standard", GCS_GIB_MO, "a GiB-month"),
        ("Azure Blob, hot", BLOB_GB_MO, "a GB-month"),
        ("BigQuery on-demand", BQ_TIB, "a TiB scanned"),
        ("Snowflake storage", SNOW_TB_MO, "a TB-month"),
        ("Snowflake, Standard", CREDIT["Standard"], "a credit"),
        ("Snowflake, Enterprise", CREDIT["Enterprise"], "a credit"),
        ("Databricks SQL Serverless", DBU, "a DBU")):
    print(f"  {label:<28}{dollars(price):>8}  {unit}")
print(f"  Snowflake bills by the second, at least {MIN_S} s a start,")
print(f"  and suspends after {SUSPEND_S} s idle unless told otherwise")

UNITS = {"stored GB-mo": "GB-month", "queries TB": "TB scanned",
         "core-h": "core-hour"}     # runs TB is priced as queries TB
print(f"\n{'dollars a unit':<22}"
      + "".join(f"{name:>15}" for name in UNITS.values()))
for vendor, prices in PRICES.items():
    print(f"{vendor:<22}" + "".join(
        f"{prices[u]:>15.3g}" if u in prices else f"{'-':>15}"
        for u in UNITS))

data = json.loads(Path("code/25/01_capacity_estimate.json")
                  .read_text())
keep = {s["name"]: s["keep_days"] for s in data["systems"]}
est, use = data["estimates"]["Finance"], cost.USAGE["Finance"]
print(f"\n{'finance, a month':<30}{'storage':>12}{'reading':>12}"
      f"{'total':>12}")
for vendor, prices in PRICES.items():
    for label, designed in (("naive", False), ("designed", True)):
        month = cost.units(est, keep["Finance"], use, designed)
        store = month["stored GB-mo"] * prices["stored GB-mo"]
        total = cost.bill(month, prices)
        print(f"{vendor + ', ' + label:<30}{store:>12,.2f}"
              f"{total - store:>12,.2f}{total:>12,.2f}")

# What busy hours leave out: a warehouse woken once an hour all month
# is billed the minimum and the idle wait every time, at the least.
woken_h = 24 * 30 * (MIN_S + SUSPEND_S) / 3600
idle = woken_h * XS_CREDITS_H * CREDIT["Standard"]
print(f"\nan X-Small woken hourly for a month: at least "
      f"{woken_h:.0f} hours,\n  or {idle:,.2f}"
      f" dollars at Standard, when its work takes under a minute")
