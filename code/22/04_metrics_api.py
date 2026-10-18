# The metrics API, called in-process: one definition of November's
# revenue reaching a dashboard, a program and Clarity alike.
# timeout: 300
import asyncio
import json
import textwrap
from pathlib import Path

import duckdb
import httpx

from groundwork.config import THREADS, TRADELINE_DB
from groundwork.serve import metrics
from groundwork.serve.api import create_app

from _serve import tidy, warehouse

path = warehouse()
with duckdb.connect(str(path)) as con:
    metrics.publish(con)
app = create_app(path)


def get(url, **params):
    """One request to the app, in this process: no server, no port."""
    async def call():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://serve") as c:
            return await c.get(url, params=params)
    r = asyncio.run(call())
    return r.status_code, r.json()


status, index = get("/metrics")
print(f"GET /metrics -> {status}, {len(index)} metrics")
for m in index:
    print(f"  {m['name']:<18} {m['kind']:<9} {m['owner']}")

nov = {"start": "2025-11-01", "end": "2025-12-01"}
status, body = get("/metrics/revenue", **nov, by="region",
                   filter="region:Northeast")
print(f"GET /metrics/revenue?...&filter=region:Northeast -> {status}")
print(" ", json.dumps(body["rows"]))
f = body["freshness"]
print(f"  loaded through {f['loaded_through']}, expected "
      f"{f['expected']}, stale: {f['stale']}")

# Three readers of November's revenue, and the dashboard of Chapter 1.
with duckdb.connect(str(path), read_only=True) as con:
    (view,) = con.execute("SELECT revenue FROM metrics.revenue_monthly"
                          " WHERE month = DATE '2025-11-01'").fetchone()
    (spec,) = metrics.query(con, "revenue", **nov)   # Clarity's tool
_, api = get("/metrics/revenue", **nov)
with duckdb.connect(config={"threads": THREADS}) as con:
    con.execute(f"ATTACH '{TRADELINE_DB}' AS tl (TYPE sqlite, "
                "READ_ONLY)")
    (old,) = con.execute("""
        SELECT SUM(l.qty * l.unit_price_cents / 100.0)
        FROM tl.orders o JOIN tl.order_lines l ON l.order_id = o.id
        WHERE o.placed_at >= '2025-11-01'
          AND o.placed_at < '2025-12-01'""").fetchone()
print("November 2025 revenue")
for reader, value in (
        ("dashboard, metrics.revenue_monthly", f"{view:,}"),
        ("a program, GET /metrics/revenue", api["rows"][0]["revenue"]),
        ("Clarity, a metric spec", f"{spec['revenue']:,}"),
        ("Chapter 1's dashboard query", repr(old))):
    print(f"  {reader:<36}{value:>20}")

for url, params in (("/metrics/revenue", {"by": "owner"}),
                    ("/metrics/active", {})):
    status, body = get(url, **params)
    said = f"GET {url} {params or ''} -> {status}: {body['detail']}"
    print(textwrap.fill(said, 68, subsequent_indent="  "))
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"view": str(view), "api": api["rows"][0]["revenue"],
     "spec": str(spec["revenue"]), "old": old,
     "sql": api["sql"]}))
tidy("warehouse.duckdb")
