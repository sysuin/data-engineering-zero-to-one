# Exercise 10.3: Kestrel adds a description column. What version 2
# does with it, and version 3, which lands the description.
import csv
import io
from dataclasses import replace

from groundwork import contracts
from groundwork.config import LANDING
from groundwork.contracts import check
from groundwork.ingest import files, price_lists, suppliers
from groundwork.ingest.suppliers import FIELDS, Layout

name = "kestrel-supply/kestrel_20251201.csv"
a = files.Arrival(LANDING / "suppliers" / name, f"suppliers/{name}",
                  0, None)
old = a.path.read_text().splitlines()
new = ["sku,description,cost_cents,effective"] + [
    f"{r.split(',')[0]},Kestrel item {i},{','.join(r.split(',')[1:])}"
    for i, r in enumerate(old[1:], 1)]
data = ("\n".join(new) + "\n").encode()

v2 = contracts.load("suppliers.price_lists")
p = check.Guard(price_lists.read, v2)
got = p(data, a)
print(f"version 2: {p.seen[-1][2].status}, {len(got.rows)} rows; "
      f"{p.seen[-1][2].notes[0]}")
print(f"  description in raw: {got.rows[0]['description']}")

KESTREL_2026 = Layout("kestrel-2026", "cost_cents",
                      {"sku": "sku", "description": "description",
                       "cost": "cost_cents",
                       "effective_from": "effective"},
                      "cents", "%Y-%m-%d")
LAYOUTS = [KESTREL_2026, *price_lists.LAYOUTS]
FAMILY = dict(price_lists.FAMILY, **{"kestrel-2026": "kestrel"})


def read(data, arrival):
    """price_lists.read, choosing the layout whose every column is in
    the header, the fullest first: two layouts share a marker now."""
    if data[:2] == b"PK":
        return suppliers.read_spreadsheet(data, arrival)
    reader = csv.DictReader(io.StringIO(data.decode("utf-8-sig")))
    head = reader.fieldnames or []
    fits = [x for x in LAYOUTS if x.marker in head
            and set(x.columns.values()) <= set(head)]
    layout = max(fits, key=lambda x: len(x.columns))
    rows = [dict({f: row[layout.columns[f]] if f in layout.columns
                  else None for f in FIELDS},
                 supplier=arrival.name.split("/")[1],
                 cost_unit=layout.cost_unit,
                 layout=FAMILY.get(layout.name, layout.name))
            for row in reader]
    key = suppliers._key(rows[0]["supplier"], rows[0]["effective_from"],
                         layout)
    return files.Parsed(rows, files.lines(data), key, note=layout.name)


v3 = replace(v2, version=3, formats=dict(
    v2.formats, **{"kestrel-2026": ("sku", "description", "cost_cents",
                                    "effective")}))
print(f"version 2 to 3: {contracts.compatible(v2, v3)}")
for label, d in (("with the column", data),
                 ("as sent today", a.path.read_bytes())):
    g = check.Guard(read, v3)
    got = g(d, a)
    print(f"version 3, {label}: {g.seen[-1][2].status}, "
          f"{len(got.rows)} rows, format {g.seen[-1][2].format},\n"
          f"  description {got.rows[0]['description']!r}")
