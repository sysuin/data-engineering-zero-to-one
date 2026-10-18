# The API's cache and its freshness flag, across a night: a question
# asked twice, a run that is late, and the run that arrives.
# timeout: 300
import asyncio
from datetime import date, datetime

import duckdb
import httpx

from groundwork.config import NOW
from groundwork.serve.api import create_app

from _serve import tidy, warehouse

path = warehouse()


def ask(app, name, **params):
    async def call():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://serve") as c:
            return await c.get(f"/metrics/{name}", params=params)
    return asyncio.run(call()).json()


def show(when, body):
    f = body["freshness"]
    rows = body["rows"]
    value = rows[-1][body["metric"]] if rows else None
    print(f"  {when:<20}{body['cache']:<6}"
          f"{str(f['loaded_through']):<11}{f['expected']:<11}"
          f"{str(f['stale']):<6}{value}")


q = {"grain": "month", "start": "2025-12-01", "end": "2026-01-01"}
print(f"  {'asked at':<20}{'cache':<6}{'loaded':<11}{'expected':<11}"
      f"{'stale':<6}value")
morning = create_app(path, now=NOW)
show("Mon 06:00", ask(morning, "revenue", **q))
show("Mon 06:00, again", ask(morning, "revenue", **q))
print(f"  queries run for two answers: {morning.state.queries}")

tuesday = create_app(path, now=datetime(2026, 1, 6, 7))
show("Tue 07:00, run late", ask(tuesday, "revenue", **q))
with duckdb.connect(str(path)) as con:       # the run for 5 January
    for table in ("orders", "order_lines"):
        con.execute("""
            INSERT INTO audit.load_log (load_id, source, table_name,
                load_date, started_at, finished_at, status)
            VALUES (?, 'tradeline', ?, ?, ?, ?, 'succeeded')""",
            [f"tradeline.{table}.2026-01-05.1", table,
             date(2026, 1, 5), datetime(2026, 1, 6, 7, 10),
             datetime(2026, 1, 6, 7, 12)])
show("Tue 07:15, run done", ask(tuesday, "revenue", **q))
show("Tue 07:15, portal", ask(tuesday, "portal_conversion",
                                 start="2025-07-01", end="2026-01-01"))
tidy("warehouse.duckdb")
