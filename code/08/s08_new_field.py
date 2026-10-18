# Exercise 3: a custom field the loader has never seen, kept unchanged.
import json
import shutil
from datetime import datetime

import duckdb

from meridian.sources import Clock, frontline
from groundwork.config import SANDBOX
from groundwork.ingest.frontline import land, load, parse
from groundwork.storage import raw_views, write_partition

work = SANDBOX / "ch08" / "s08_new_field"
shutil.rmtree(work, ignore_errors=True)
root, wh = work / "raw", work / "wh.duckdb"
RUN = datetime(2025, 12, 3, 6, 0)
x = load("2025-12-02", run_at=RUN, base=frontline.serve(Clock(RUN)),
         root=root, warehouse=wh, start="2025-12-01T00:00:00Z")

# Support adds `region` on 2 December: tickets changed from then on
# carry it. Take the landed pages and add it, as a new attempt.
pages = sorted((root / "frontline" / "pages").rglob("page-*.json"))
new = pages[0].parent.parent / "attempt-2"
for p in pages:
    body = json.loads(p.read_bytes())
    for t in body["tickets"]:
        if t["updated_at"] >= "2025-12-02":
            t["custom_fields"].append({"id": 360003, "name": "region",
                                       "value": "Northeast"})
    land(new, int(p.stem.split("-")[1]),
         json.dumps(body, ensure_ascii=False).encode())

tables, read = parse(sorted(new.glob("page-*.json")))
for name, tbl in tables.items():
    write_partition(tbl, "frontline", name, "2025-12-02", loaded_at=RUN,
                    load_id=x.load_id + "-reparse", root=root,
                    source_file=new.relative_to(root).as_posix())
con = duckdb.connect(str(wh))
raw_views(con, root)
print(con.execute("""
    SELECT name, count(*) FROM raw.frontline_ticket_fields
    GROUP BY 1 ORDER BY 1""").fetchall())
rows = con.execute("""
    SELECT ticket_id, updated_at,
           max(value) FILTER (name = 'desk') AS desk,
           max(value) FILTER (name = 'category') AS category,
           max(value) FILTER (name = 'region') AS region
    FROM raw.frontline_ticket_fields
    GROUP BY ALL ORDER BY updated_at, ticket_id""").fetchall()
print(f"{len(rows)} tickets, one row each; the first and last two:")
for r in rows[:2] + rows[-2:]:
    print("  ", r[0], r[1][5:16], r[2], r[3], r[4])
con.close()
shutil.rmtree(work)
