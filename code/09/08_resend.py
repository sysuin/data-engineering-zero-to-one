# Aldridge's March 2024 list, sent twice: both versions land in raw,
# and the file ledger's rule says which one is in force.
import json
import os
import shutil
from datetime import date, datetime, time, timedelta
from pathlib import Path

import duckdb

from groundwork.config import SANDBOX, TRADELINE_DB
from groundwork.ingest import files, suppliers
from meridian.sources import landing

here = SANDBOX / "ch09" / "resend"
shutil.rmtree(here, ignore_errors=True)
where = dict(landing=here / "landing", root=here / "raw",
             warehouse=here / "warehouse.duckdb",
             archive=here / "landed")
runs = []


def short(name):
    return name.split("/")[-1].removeprefix("Aldridge Trade Prices ")


def usd(cents):
    return f"{'-' if cents < 0 else ''}${abs(cents) / 100:,.2f}"


for i in range(6):                       # load dates 29 Feb to 5 Mar
    night = date(2024, 2, 29) + timedelta(days=i)
    at = datetime.combine(night + timedelta(days=1), time(6))
    landing.stage(at, where["landing"], under="suppliers")
    suppliers.load(night, run_at=at, **where)
    runs.append(at)

con = duckdb.connect(str(where["warehouse"]))
cost = ("CAST(round(CAST(cost AS DECIMAL(12, 2)) * 100) AS BIGINT)")
got = con.execute(f"""
    SELECT load_date, {cost}, split_part(file, '/', 3)
    FROM raw.suppliers_price_lists
    WHERE sku = 'MRD-FAC-030' AND effective_from = '1 March 2024'
    ORDER BY load_date""").fetchall()
print("raw holds two March costs for MRD-FAC-030:")
for day, cents, name in got:
    print(f"  loaded {day}  {cents / 100:>7.2f}  {short(name)}")

# March's sales of that product, and a margin at each cost.
tl = duckdb.connect()
tl.execute(f"ATTACH '{TRADELINE_DB}' AS t (TYPE sqlite, READ_ONLY)")
qty, sales = tl.execute("""
    SELECT sum(l.qty), sum(l.qty * l.unit_price_cents)
    FROM t.order_lines l JOIN t.orders o ON o.id = l.order_id
    WHERE l.sku = 'MRD-FAC-030' AND o.status <> 'cancelled'
      AND o.placed_at >= '2024-03-01' AND o.placed_at < '2024-04-01'
    """).fetchone()
print(f"\nMarch sales of MRD-FAC-030: {qty:,} units, {usd(sales)}")
for day, cents, name in got:
    print(f"  at {cents / 100:>6.2f} a unit, margin "
          f"{usd(sales - qty * cents):>13}")
both = sum(c for _, c, _ in got)
print(f"  joined to both: sales {usd(2 * sales)}, margin "
      f"{usd(2 * sales - qty * both)}")

print("\nIn force, by the ledger's rule (latest arrival wins):")
for s in files.current(con, "suppliers"):
    if s.logical_key == "aldridge-co/2024-03":
        print(f"  {short(s.name)}, arrived {s.arrived_at}")
first_seen = {n: min(s.seen_at for s in files.ledger(con, "suppliers")
                     if s.name.endswith(n)) for _, _, n in got}
for n, t in first_seen.items():
    print(f"  {short(n):<26} first loaded at {t:%d %b %H:%M}")

# Suppose the first version is sent again, unchanged, on 6 March.
con.close()
old = where["landing"] / "suppliers/aldridge-co" / got[0][2]
again = old.with_name("Aldridge Trade Prices Mar 2024 (2).xlsx")
shutil.copyfile(old, again)
t = datetime(2024, 3, 6, 11, 0).timestamp()
os.utime(again, (t, t))
x = suppliers.load(date(2024, 3, 6), run_at=datetime(2024, 3, 7, 6),
                   **where)
con = duckdb.connect(str(where["warehouse"]))
s = [s for s in files.ledger(con, "suppliers") if s.name == again
     .relative_to(where["landing"]).as_posix()][0]
print(f"\nresent unchanged on 6 Mar: {s.status}")
print(f"  same bytes as {short(s.note)}")
now = [c.name for c in files.current(con, "suppliers")
       if c.logical_key == "aldridge-co/2024-03"]
print(f"in force after it: {short(now[0])}")

Path(__file__).with_suffix(".json").write_text(json.dumps({
    "versions": [{"loaded": str(d), "cents": c, "name": n}
                 for d, c, n in got],
    "arrived": {n: str(s.arrived_at) for s in files.ledger(
        con, "suppliers") for n in [s.name.split("/")[-1]]
        if n.startswith("Aldridge Trade Prices Mar 2024")},
    "runs": [str(r) for r in runs], "qty": qty, "sales": sales},
    indent=1))
shutil.rmtree(here)
