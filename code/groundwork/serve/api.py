"""
The metrics API: the semantic layer over HTTP, for anything that can
make a web request and cannot be given a database connection.

    GET /metrics                  every metric: what it means, its
                                  owner, the dimensions it allows
    GET /metrics/{name}?by=region&grain=month&start=2025-01-01
                       &end=2026-01-01&filter=region:Northeast
                                  the rows, the SQL that made them, and
                                  how fresh the data behind them is

Every answer says how fresh it is: the logical date of the last
successful load of each source the metric depends on, and whether that
is the date a nightly run should have reached. Answers are cached
until the warehouse changes, so a dashboard that asks the same
question all morning costs one query.

    create_app(warehouse, now=NOW)       the FastAPI application
    uvicorn "groundwork.serve.api:app"   to serve the real warehouse
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import duckdb
from fastapi import FastAPI, HTTPException, Query

from groundwork.config import NOW, WAREHOUSE
from groundwork.serve import metrics

VERSION = """SELECT (SELECT count(*) FROM audit.model_runs),
                    (SELECT count(*) FROM audit.load_log)"""
LOADED = """SELECT max(load_date) FROM audit.load_log
            WHERE source = ? AND table_name = ?
              AND status = 'succeeded'"""


def _json(v):
    if isinstance(v, Decimal):          # money stays exact: a string
        return str(v)
    if isinstance(v, (date, datetime)):
        return v.isoformat()
    return v


def freshness(con, m: metrics.Metric, now: datetime) -> dict:
    """The newest logical date every load behind `m` has reached, and
    whether it is the one the last nightly run should have loaded."""
    expected = (now - timedelta(hours=6)).date() - timedelta(days=1)
    if not m.loads:
        return {"loaded_through": None, "expected": str(expected),
                "stale": None}
    dates = [con.execute(LOADED, load.split(".")).fetchone()[0]
             for load in m.loads]
    through = None if None in dates else min(dates)
    return {"loaded_through": through and str(through),
            "expected": str(expected),
            "stale": through is None or through < expected}


def create_app(warehouse: Path = WAREHOUSE, now: datetime = NOW,
               cat: metrics.Catalogue | None = None) -> FastAPI:
    """The API over one warehouse file, at a moment `now` (a run's
    moment, never the wall clock)."""
    cat = cat or metrics.catalogue()
    app = FastAPI(title="Groundwork metrics")
    app.state.cache, app.state.queries = {}, 0

    def connect():
        return duckdb.connect(str(warehouse), read_only=True)

    @app.get("/metrics")
    def index():
        return [{"name": m.name, "label": m.label, "owner": m.owner,
                 "kind": m.kind, "description": m.description,
                 "dimensions": list(m.dimensions)}
                for m in cat.metrics.values()]

    @app.get("/metrics/{name}")
    def ask(name: str, by: list[str] = Query(default=[]),
            grain: str | None = None, start: date | None = None,
            end: date | None = None,
            filter: list[str] = Query(default=[])):
        if name not in cat.metrics:
            raise HTTPException(404, f"no metric called {name!r}")
        where: dict[str, list[str]] = {}
        for f in filter:
            dim, _, value = f.partition(":")
            where.setdefault(dim, []).append(value)
        question = dict(by=tuple(by), where=where, grain=grain,
                        start=start, end=end, cat=cat)
        try:
            sql, params = metrics.compile(name, **question)
        except metrics.MetricError as e:
            raise HTTPException(400, str(e)) from e
        with connect() as con:
            version = con.execute(VERSION).fetchone()
            key = (name, sql, tuple(map(str, params)), version)
            hit = key in app.state.cache
            if not hit:
                cur = con.execute(sql, params)
                cols = [c[0] for c in cur.description]
                rows = [{c: _json(v) for c, v in zip(cols, r)}
                        for r in cur.fetchall()]
                app.state.queries += 1
                app.state.cache[key] = (
                    rows, freshness(con, cat[name], now))
            rows, fresh = app.state.cache[key]
        return {"metric": name, "label": cat[name].label,
                "rows": rows, "freshness": fresh,
                "cache": "hit" if hit else "miss", "sql": sql}

    return app


app = create_app()
