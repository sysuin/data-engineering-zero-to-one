# An hourly query for a month, in the units two kinds of warehouse bill.
from _plans import connect

from groundwork.config import RAW

FILE = (RAW / "tradeline" / "order_lines" / "load_date=2026-01-04"
        / "part-0.parquet")
RUNS = 24 * 30          # hourly, for a thirty-day month
GB_PER_CORE_H = 30      # assumed, as Chapter 25: GB per core-hour
MINIMUM_S = 60          # assumed: least time billed each start
SUSPEND_S = 300         # assumed: idle time before it stops

con = connect()
COLUMNS = {"SELECT *": None, "two columns": ("qty", "unit_price_cents")}
print(f"{'query':<12}{'on disk':>9}{'uncompr.':>10}"
      f"{'scanned/month':>15}{'busy core-h':>12}{'billed':>8}")
for label, cols in COLUMNS.items():
    disk, unpacked = con.execute(f"""
        SELECT sum(total_compressed_size), sum(total_uncompressed_size)
        FROM parquet_metadata('{FILE}')
        WHERE {'true' if cols is None else
               f"path_in_schema IN {cols}"}""").fetchone()
    month_gb = RUNS * unpacked / 1e9          # scan-priced: bytes read
    busy_s = unpacked / 1e9 / GB_PER_CORE_H * 3600
    billed_s = max(busy_s, MINIMUM_S) + SUSPEND_S
    print(f"{label:<12}{disk / 1e6:>6.1f} MB{unpacked / 1e6:>7.1f} MB"
          f"{month_gb:>12.1f} GB{RUNS * busy_s / 3600:>12.2f}"
          f"{RUNS * billed_s / 3600:>8.1f}")
print(f"runs a month: {RUNS}. Billed: one core, at least "
      f"{MINIMUM_S} s a start,")
print(f"plus {SUSPEND_S} s idle before it suspends itself")
