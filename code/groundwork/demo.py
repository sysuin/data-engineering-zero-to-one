"""
Groundwork in a minute: what ran, what it proves, one number every
reader agrees on, and what it refuses. It reads the warehouse and
writes nothing.

    python -m groundwork.demo [--warehouse W]          (make demo)

  1. The night: the newest published night, task by task, from
     audit.task_runs.
  2. The proof: Books 1 and 2 reconciled again, by reconcile_books.
  3. One November: the dashboard's old query on Tradeline, the metric
     layer, the API, Book 2's own v_sales over compat, and the ledger,
     with the journal that separates the last two named.
  4. What it refuses: a metric that does not exist, a breakdown the
     metric does not offer, and a build over a warehouse that exists.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date
from pathlib import Path

import duckdb

from groundwork.config import NOW, TRADELINE_DB, WAREHOUSE

MONTH = date(2025, 11, 1)          # the month Chapter 1 opened with


def night(con) -> list[str]:
    from groundwork.nightly import CHECKS
    run_id, day = con.execute("""
        SELECT run_id, logical_date FROM audit.published
        ORDER BY logical_date DESC, published_at DESC LIMIT 1"""
                                  ).fetchone()
    rows = con.execute("""
        SELECT task, state, try_number, detail FROM audit.task_runs
        WHERE run_id = ?
        QUALIFY row_number() OVER (PARTITION BY task
                                   ORDER BY try_number DESC) = 1
        ORDER BY started_at, task""", [run_id]).fetchall()
    ok = sum(s == "succeeded" and n == 1 for _, s, n, _ in rows)
    out = [f"1. The night: {run_id}",
           f"   {ok} of {len(rows)} tasks succeeded at the first try; "
           f"{day.day} {day:%B} published",
           "   the tasks v1.0 added:"]
    added = {"restore_old_crm"} | {t.name for t in CHECKS}
    for task, _, _, detail in rows:
        if task in added:
            d = detail if len(detail) <= 44 else detail[:41] + "..."
            out.append(f"   {task:<17}{d}")
    return out


def proof(con) -> list[str]:
    from groundwork import reconcile_books as rb
    b2, (h, _) = rb.book2(con), rb.history(con)
    b1, qs = rb.book1(con), rb.quarters(con)
    rows2 = sum(x.theirs for x in b2)
    bad2 = sum(x.differing for x in [*b2, h])
    bad1 = sum(x.differing for x in b1) + sum(len(q.differing)
                                              for q in qs)
    return ["2. The proof",
            f"   Book 2: {len(b2)} tables, {rows2:,} rows, and the "
            f"history: {bad2} differ",
            f"   Book 1: {len(b1)} tables and {len(qs)} quarters: "
            f"{bad1} differ"]


def _get(app, url, **params):
    import httpx

    async def call():
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport,
                                     base_url="http://serve") as c:
            return await c.get(url, params=params)
    r = asyncio.run(call())
    return r.status_code, r.json()


def november(con, warehouse: Path, tradeline: Path) -> list[str]:
    from groundwork import reconcile
    from groundwork.serve import metrics
    from groundwork.serve.api import create_app
    end = date(MONTH.year + MONTH.month // 12, MONTH.month % 12 + 1, 1)
    period = dict(start=MONTH, end=end)
    dash = reconcile.dashboard(con, MONTH, tradeline=tradeline)[0]
    (layer,) = [r["revenue"] for r in metrics.query(con, "revenue",
                                                    **period)]
    status, body = _get(create_app(warehouse, NOW), "/metrics/revenue",
                        start=str(MONTH), end=str(end))
    (book2,) = con.execute("""
        SELECT round(sum(revenue), 2) FROM compat.v_sales
        WHERE order_date >= ? AND order_date < ?""",
                           [str(MONTH), str(end)]).fetchone()
    month = MONTH.strftime("%Y-%m")
    (ledger,) = con.execute("""
        SELECT sum(amount_cents) FROM staging.stg_ledger__gl_revenue
        WHERE strftime(period_month, '%Y-%m') = ?""", [month]
                            ).fetchone()
    journals = con.execute("""
        SELECT region, amount, description FROM seeds.ledger_journals
        WHERE period = ? ORDER BY region""", [month]).fetchall()
    out = [f"3. {MONTH:%B %Y}, every reader",
           f"   {'the dashboard, as Chapter 1 found it':<40}"
           f"{dash!r:>20}",
           f"   {'the metric layer':<40}{layer:>20,.2f}",
           f"   {'the API, GET /metrics/revenue':<40}"
           f"{body['rows'][0]['revenue']:>20}",
           f"   {"Book 2's v_sales, over compat":<40}{book2:>20,.2f}",
           f"   {'the ledger, the number of record':<40}"
           f"{ledger / 100:>20,.2f}"]
    for region, amount, text in journals:
        out.append(f"   the gap is finance's own journal: {region} "
                   f"{amount:,.2f},")
        out.append(f"     {text.split(':', 1)[1].strip()}")
    return out


def refusals(warehouse: Path) -> list[str]:
    from groundwork import nightly
    from groundwork.serve.api import create_app
    app = create_app(warehouse, NOW)
    out = ["4. What it refuses"]
    for url, params in (("/metrics/revnue", {}),
                        ("/metrics/revenue", {"by": "colour"})):
        status, body = _get(app, url, **params)
        q = "?" + "&".join(f"{k}={v}" for k, v in params.items()) \
            if params else ""
        said = body["detail"].split(";")[0]
        out.append(f"   GET {url}{q} -> {status}")
        out.append(f"     {said}")
    try:
        nightly.build(Path(warehouse).parent)
    except FileExistsError:
        out.append("   build over a warehouse that exists -> refused")
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="python -m groundwork.demo")
    ap.add_argument("--warehouse", type=Path, default=WAREHOUSE)
    ap.add_argument("--tradeline", type=Path, default=TRADELINE_DB)
    a = ap.parse_args(argv)
    if not a.warehouse.exists():
        print(f"{a.warehouse} is not built: run make warehouse first")
        return 2
    con = duckdb.connect(str(a.warehouse), read_only=True)
    try:
        con.execute("SET enable_progress_bar = false")
        parts = [night(con), proof(con),
                 november(con, a.warehouse, a.tradeline)]
    finally:
        con.close()
    parts.append(refusals(a.warehouse))
    print("\n\n".join("\n".join(p) for p in parts))
    return 0


if __name__ == "__main__":
    sys.exit(main())
