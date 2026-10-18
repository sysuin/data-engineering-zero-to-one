# A seventeenth supplier's first list, in a layout nobody has seen:
# refused as it stands, then loaded by a reader that knows it.
import csv
import io
import os
import shutil
from datetime import date, datetime

import duckdb

from groundwork.config import SANDBOX
from groundwork.ingest import files, suppliers
from groundwork.storage import raw_views

here = SANDBOX / "ch09" / "s_layout"
shutil.rmtree(here, ignore_errors=True)
here.mkdir(parents=True)
where = dict(root=here / "raw", archive=here / "landed")
folder = here / "landing" / "suppliers" / "harrow-fixings"
folder.mkdir(parents=True)
path = folder / "HARROW 2024-04.csv"
path.write_text("Part No;Net Price;Valid;Curr\n"
                "MRD-FAC-031;12,50;01.04.2024;EUR\n"
                "MRD-FAC-032;7,05;01.04.2024;EUR\n", encoding="utf-8")
t = datetime(2024, 4, 1, 9).timestamp()
os.utime(path, (t, t))
con = duckdb.connect(str(here / "w.duckdb"))
at = datetime(2024, 4, 2, 6)


def run(read):
    try:
        x = files.load(con, "suppliers", "price_lists",
                       files.discover(here / "landing", "suppliers",
                                      by=at), date(2024, 4, 1),
                       run_at=at, read=read, folder="suppliers",
                       **where)
        return f"succeeded, {x.rows_written} rows, reconciled " \
               f"{x.reconciled}"
    except Exception as e:
        return f"failed: {str(e)[:40]}"


print(f"as it is: {run(suppliers.read)}")

HARROW = suppliers.Layout(
    "harrow", "Part No",
    {"sku": "Part No", "cost": "Net Price", "currency": "Curr",
     "effective_from": "Valid"}, "euros", "%d.%m.%Y")


def read(data, arrival):
    """Harrow's semicolon layout; everything else as before."""
    if not data.startswith(b"Part No;"):
        return suppliers.read(data, arrival)
    rows = []
    text = data.decode("utf-8-sig")
    for r in csv.DictReader(io.StringIO(text, newline=""),
                            delimiter=";"):
        out = {"supplier": arrival.name.split("/")[1]}
        out.update({f: r.get(HARROW.columns[f])
                    if f in HARROW.columns else None
                    for f in suppliers.FIELDS})
        out.update(cost_unit=HARROW.cost_unit, layout=HARROW.name)
        rows.append(out)
    month = datetime.strptime(rows[0]["effective_from"],
                              HARROW.date_format)
    key = f"{rows[0]['supplier']}/{month:%Y-%m}"
    return files.Parsed(rows, files.lines(data), key, note="harrow")


print(f"with the layout: {run(read)}")
raw_views(con, where["root"])
for r in con.sql("SELECT r.sku, r.cost, r.currency, r.cost_unit, "
                 "c.logical_key FROM raw.suppliers_price_lists r "
                 "JOIN audit.file_ledger c ON c.sha256 = r.file_sha256 "
                 "ORDER BY r.sku").fetchall():
    print("  " + "  ".join(map(str, r)))
con.close()
shutil.rmtree(here)
