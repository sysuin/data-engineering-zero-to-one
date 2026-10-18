# Groundwork's full-refresh loader, run on three nights, rerun, and
# then run on a full disk; the load log records every attempt.
import shutil
from datetime import date, datetime

import duckdb
from _first_pipeline import dollars

from groundwork import loadlog
from groundwork.config import SANDBOX
from groundwork.ingest import tradeline_full
from meridian.sources import tradeline

HERE = SANDBOX / "ch06"
ROOT, WH, DB = HERE / "raw", HERE / "warehouse.duckdb", HERE / "tl.db"
shutil.rmtree(ROOT, ignore_errors=True)
WH.unlink(missing_ok=True)

runs = [("2025-12-01", "2025-12-02 02:05"),
        ("2025-12-02", "2025-12-03 02:05"),
        ("2025-12-03", "2025-12-04 02:05"),
        ("2025-12-03", "2025-12-04 02:40")]      # a rerun
for day, moment in runs:
    tradeline.at(moment, DB)                # Tradeline at that moment
    tradeline_full.load(date.fromisoformat(day), source=DB, root=ROOT,
                        warehouse=WH,
                        run_at=datetime.fromisoformat(moment))


def full_disk(*args, **kwargs):
    raise OSError(28, "No space left on device")


tradeline_full.write_partition = full_disk
try:
    tradeline_full.load(date(2025, 12, 3), source=DB, root=ROOT,
                        warehouse=WH,
                        run_at=datetime(2025, 12, 4, 3, 10))
except OSError:
    pass
DB.unlink()

con = duckdb.connect(str(WH))
print(f"{'load_id':<34} {'status':<9} {'written':>9} {'source':>9}")
for x in loadlog.history(con):
    written = "" if x.rows_written is None else f"{x.rows_written:,}"
    print(f"{x.load_id:<34} {x.status:<9} {written:>9} "
          f"{x.source_count:>9,}")
    if "error" in x.extra:
        print(f"  {x.extra['error']}")

rows, ids = con.execute("SELECT count(*), count(DISTINCT id) "
                        "FROM raw.tradeline_orders").fetchone()
print(f"raw.tradeline_orders: {rows:,} rows, {ids:,} order ids")

NOVEMBER = """SELECT count(*), sum(l.qty * l.unit_price_cents)
    FROM raw.tradeline_orders o JOIN raw.tradeline_order_lines l
      ON l.order_id = o.id {}
    WHERE o.status = 'placed' AND o.placed_at >= '2025-11-01'
      AND o.placed_at < '2025-12-01'"""
good = {t: loadlog.last(con, "tradeline", t).load_date
        for t in ("orders", "order_lines")}
assert good["orders"] == good["order_lines"]
one = ("AND o.load_date = l.load_date "
       f"AND o.load_date = DATE '{good['orders']}'")
lines, cents = con.execute(NOVEMBER.format(one)).fetchone()
_, naive = con.execute(NOVEMBER.format("")).fetchone()
print(f"November, joined across every night: {naive / cents:.1f} times")
print(f"November, last good load ({good['orders']}): {lines:,} lines, "
      f"{dollars(cents)}")
con.close()
shutil.rmtree(ROOT)                          # keep the sandbox small
WH.unlink()
