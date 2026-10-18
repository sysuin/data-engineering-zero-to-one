# Two nights of Groundwork's Frontline loader, and a reparse from disk.
import shutil
from datetime import datetime

import duckdb
import httpx
import pyarrow.parquet as pq

from meridian.sources import Clock, frontline
from groundwork.config import SANDBOX
from groundwork.ingest.frontline import load, parse, token
from groundwork.storage import raw_path

work = SANDBOX / "ch08"
root, wh = work / "raw", work / "warehouse.duckdb"
shutil.rmtree(root, ignore_errors=True)
wh.unlink(missing_ok=True)

# Two nightly runs against a desk that refuses every 4th request
# and fails every 5th. The first load starts at 1 December.
for day, at in (("2025-12-02", "2025-12-03 06:00"),
                ("2025-12-03", "2025-12-04 06:00")):
    run_at = datetime.fromisoformat(at)
    base = frontline.serve(Clock(run_at), rate_limit=4, fail_every=5)
    x = load(day, run_at=run_at, base=base, root=root, warehouse=wh,
             start="2025-12-01T00:00:00Z")
    e = x.extra
    print(f"{x.load_id}  {x.status}")
    print(f"  since {e['since']}  watermark {e['watermark']}")
    print(f"  {e['requests']} requests, {e['retries']} retried: "
          + " ".join(str(s) for _, s in frontline.requests_made()))
    print(f"  tickets {x.rows_written} (API total {x.source_count}), "
          f"fields {e['ticket_fields']}, tags {e['ticket_tags']}")

landed = root / "frontline" / "pages"
pages = sorted(landed.rglob("*.json"))
print(f"landed under {landed.relative_to(work).as_posix()}/:")
for p in pages:
    name = p.relative_to(landed).as_posix()
    print(f"  {name}  {p.stat().st_size:,} B")

con = duckdb.connect(str(wh), read_only=True)
print("raw.frontline_tickets by load date:")
for d, n, first in con.execute("""
        SELECT load_date, count(*), min(_source_file)
        FROM raw.frontline_tickets GROUP BY ALL ORDER BY 1
        """).fetchall():
    print(f"  {d}  {n:>4} rows from {first.split('/')[-1]}")
(both,) = con.execute("""
    SELECT count(*) FROM (SELECT ticket_id FROM raw.frontline_tickets
    GROUP BY 1 HAVING count(DISTINCT load_date) = 2)""").fetchone()
print(f"  {both} tickets in both nights' partitions")

# A month on, the API no longer has the first night's tickets.
night1 = pq.read_table(raw_path("frontline", "tickets", "2025-12-02",
                                root)).to_pylist()
base = frontline.serve(Clock("2026-01-05 06:00"))
with httpx.Client(base_url=base, headers={
        "Authorization": f"Bearer {token()}"}) as http:
    moved = sum(http.get(f"/api/v2/tickets/{t['ticket_id']}").json()
                ["ticket"]["updated_at"] != t["updated_at"]
                for t in night1)
print(f"asked again on 5 January: {moved} of {len(night1)} tickets "
      "have changed since")

# The first night's pages, parsed again: no request to anyone.
first = [p for p in pages if "2025-12-02" in str(p)]
asked = len(frontline.requests_made())
tables, read = parse(first)
again = tables["tickets"].to_pylist()
keep = [{k: v for k, v in t.items() if not k.startswith("_")}
        for t in night1]
same = [{k: v for k, v in t.items() if k != "_page"}
        for t in again] == keep
print(f"reparsed {len(first)} pages: {read} tickets, identical to the "
      f"partition: {same}")
print(f"requests made while reparsing: "
      f"{len(frontline.requests_made()) - asked}")
