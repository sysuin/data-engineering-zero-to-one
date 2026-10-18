"""
Groundwork's transformation runner: SQL files in, views and tables out,
in an order worked out from the SQL itself.

A model is one SQL file under models/<layer>/, and makes one relation
named after the file in the schema named after the folder:

    models/staging/stg_relay__accounts.sql
                                  -> staging.stg_relay__accounts
    models/seeds/supplier_names.csv  -> seeds.supplier_names

A model never names another relation directly. It writes

    {{ ref('stg_relay__accounts') }}     another model (or a seed)
    {{ source('raw', 'relay_accounts') }}  something a loader made

and the runner replaces each with a schema-qualified name. The refs
are also the dependency graph: a model is built after everything it
refs. A comment in the file's header says how to materialise it:

    -- materialised: view     (the default) or table

A seed is a CSV that a person keeps in the repository; it loads as a
table of text, and the models that use it cast its columns.

A run builds only what changed. Each model has a fingerprint: a hash
of its SQL, its materialisation and its upstream models' fingerprints,
and, for a table, of the loads behind the raw data it reads. A model
whose fingerprint matches its last successful build is skipped. Every
model in every run is a row of audit.model_runs.

    run(warehouse, run_at=..., select=None, full=False)
    python -m groundwork.transform.runner [--select M ...] [--full]
"""
from __future__ import annotations

import argparse
import hashlib
import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import duckdb

from groundwork.config import NOW, WAREHOUSE

MODELS = Path(__file__).resolve().parents[1] / "models"
REF = re.compile(r"\{\{\s*ref\(\s*'(\w+)'\s*\)\s*\}\}")
SOURCE = re.compile(
    r"\{\{\s*source\(\s*'(\w+)'\s*,\s*'(\w+)'\s*\)\s*\}\}")
HEADER = re.compile(r"^--\s*materialised:\s*(\w+)\s*$", re.M)
KINDS = ("view", "table")


@dataclass(frozen=True)
class Model:
    name: str
    schema: str
    path: Path
    sql: str                         # the file's text; '' for a seed
    materialised: str                # view, table or seed
    refs: tuple[str, ...]
    sources: tuple[tuple[str, str], ...]

    @property
    def relation(self) -> str:
        return f"{self.schema}.{self.name}"


@dataclass(frozen=True)
class Result:
    model: str
    relation: str
    materialised: str
    status: str                      # built, skipped, failed, blocked
    rows: int | None
    error: str | None = None


def discover(folder=MODELS) -> dict[str, Model]:
    """Every model and seed under folder, by name."""
    models = {}
    for path in sorted(Path(folder).rglob("*")):
        if path.suffix == ".csv":
            m = Model(path.stem, "seeds", path, "", "seed", (), ())
        elif path.suffix == ".sql":
            text = path.read_text()
            how = HEADER.search(text)
            kind = how.group(1) if how else "view"
            if kind not in KINDS:
                raise ValueError(f"{path.name}: materialised: {kind}?")
            m = Model(path.stem, path.parent.name, path, text, kind,
                      tuple(sorted(set(REF.findall(text)))),
                      tuple(sorted(set(SOURCE.findall(text)))))
        else:
            continue
        if m.name in models:
            raise ValueError(f"two models are called {m.name}")
        models[m.name] = m
    return models


def compile_sql(model: Model, models: dict[str, Model]) -> str:
    """The model's SQL with every ref() and source() resolved."""
    def ref(match):
        name = match.group(1)
        if name not in models:
            raise KeyError(f"{model.name}: ref('{name}') "
                           "is not a model")
        return models[name].relation
    sql = REF.sub(ref, model.sql)
    return SOURCE.sub(lambda m: f"{m.group(1)}.{m.group(2)}", sql)


def order(models: dict[str, Model]) -> list[str]:
    """Every model after all it refs; ties broken by name."""
    waiting = {n: set(m.refs) for n, m in models.items()}
    for n, refs in waiting.items():
        if refs - set(models):
            raise KeyError(f"{n}: ref('{min(refs - set(models))}') "
                           "is not a model")
    done: list[str] = []
    while waiting:
        ready = sorted(n for n, refs in waiting.items()
                       if not refs - set(done))
        if not ready:                 # follow refs until one repeats
            path = [min(waiting)]
            while path.count(path[-1]) < 2:
                path.append(min(waiting[path[-1]] - set(done)))
            cycle = path[path.index(path[-1]):]
            raise ValueError("a cycle: " + " refs ".join(cycle))
        for n in ready:
            done.append(n)
            del waiting[n]
    return done


def downstream(models: dict[str, Model], names) -> set[str]:
    """names, and every model that refs one of them, at any distance."""
    keep = set(names)
    while True:
        more = {n for n, m in models.items() if set(m.refs) & keep}
        if more <= keep:
            return keep
        keep |= more


def _hash(*parts) -> str:
    text = "\x1f".join(map(str, parts))
    return hashlib.sha256(text.encode()).hexdigest()


def _loads(con, schema: str, name: str) -> str:
    """What the data behind a source looks like: its loads so far."""
    if schema != "raw":
        return ""
    try:
        (ids,) = con.execute(
            "SELECT string_agg(load_id, ',' ORDER BY load_id) "
            "FROM audit.load_log WHERE status = 'succeeded' "
            "AND source = ?", [name.split("_")[0]]).fetchone()
    except duckdb.CatalogException:
        return ""
    return ids or ""


def fingerprints(con, models: dict[str, Model]) -> dict[str, str]:
    """For each model: the hash that changes when it must be rebuilt."""
    prints: dict[str, str] = {}
    data: dict[str, str] = {}
    for n in order(models):
        m = models[n]
        if m.materialised == "seed":
            prints[n] = data[n] = _hash(m.path.read_bytes())
            continue
        defined = _hash(m.materialised, compile_sql(m, models),
                        *(prints[r] for r in m.refs))
        data[n] = _hash(defined, *(data[r] for r in m.refs),
                        *(_loads(con, *s) for s in m.sources))
        prints[n] = defined if m.materialised == "view" else data[n]
    return prints


DDL = """
CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS audit.model_runs (
    run_id       INTEGER NOT NULL,
    model        VARCHAR NOT NULL,
    relation     VARCHAR NOT NULL,
    materialised VARCHAR NOT NULL,
    status       VARCHAR NOT NULL CHECK (status IN
                 ('built', 'skipped', 'failed', 'blocked')),
    fingerprint  VARCHAR NOT NULL,
    rows         BIGINT,
    started_at   TIMESTAMP NOT NULL,
    finished_at  TIMESTAMP NOT NULL,
    error        VARCHAR,
    PRIMARY KEY (run_id, model)
);
"""


def _kind_now(con, m: Model) -> str | None:
    """'view' or 'table' if the model's relation exists, else None."""
    row = con.execute(
        "SELECT 'view' FROM duckdb_views() WHERE schema_name = ? "
        "AND view_name = ? UNION ALL SELECT 'table' "
        "FROM duckdb_tables() WHERE schema_name = ? AND table_name = ?",
        [m.schema, m.name, m.schema, m.name]).fetchone()
    return row[0] if row else None


def unchanged(con, m: Model, fingerprint: str) -> bool:
    """True when the last good build had this fingerprint, and is
    still there."""
    last = con.execute(
        "SELECT fingerprint FROM audit.model_runs WHERE model = ? "
        "AND status = 'built' ORDER BY run_id DESC LIMIT 1",
        [m.name]).fetchone()
    return (last is not None and last[0] == fingerprint
            and _kind_now(con, m) is not None)


def materialise(con, m: Model, models: dict[str, Model]) -> int:
    """Build one model, all or nothing, and count its rows."""
    if m.materialised == "seed":
        path = m.path.as_posix().replace("'", "''")
        kind, body = "TABLE", (f"SELECT * FROM read_csv('{path}', "
                               "header = true, all_varchar = true)")
    else:
        kind, body = m.materialised.upper(), compile_sql(m, models)
    con.execute("BEGIN")
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {m.schema}")
        was = _kind_now(con, m)
        if was and was != kind.lower():
            con.execute(f"DROP {was.upper()} {m.relation}")
        con.execute(f"CREATE OR REPLACE {kind} {m.relation} AS {body}")
        (rows,) = con.execute(
            f"SELECT count(*) FROM {m.relation}").fetchone()
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return rows


def run(warehouse=WAREHOUSE, *, run_at: datetime = NOW, select=None,
        full: bool = False, folder=MODELS) -> list[Result]:
    """Build every model (or select and what depends on it) in order."""
    models = discover(folder)
    names = order(models)
    if select:
        missing = set(select) - set(models)
        if missing:
            raise KeyError("no such model: "
                           + ", ".join(sorted(missing)))
        wanted = downstream(models, select)
        names = [n for n in names if n in wanted]
    run_at = run_at.replace(tzinfo=None)
    con = duckdb.connect(str(warehouse))
    try:
        con.execute(DDL)
        (run_id,) = con.execute("SELECT coalesce(max(run_id), 0) + 1 "
                                "FROM audit.model_runs").fetchone()
        prints = fingerprints(con, models)
        results, broken, clock = [], set(), time.perf_counter()
        for n in names:
            m = models[n]
            started = time.perf_counter() - clock
            rows, error = None, None
            if set(m.refs) & broken:
                status, error = "blocked", "upstream failed"
                broken.add(n)
            elif not full and unchanged(con, m, prints[n]):
                status = "skipped"
            else:
                try:
                    rows, status = materialise(con, m, models), "built"
                except duckdb.Error as e:
                    status, error = "failed", str(e).splitlines()[0]
                    broken.add(n)
            finished = time.perf_counter() - clock
            con.execute(
                "INSERT INTO audit.model_runs VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [run_id, n, m.relation, m.materialised, status,
                 prints[n], rows, run_at + timedelta(seconds=started),
                 run_at + timedelta(seconds=finished), error])
            results.append(Result(n, m.relation, m.materialised, status,
                                  rows, error))
        return results
    finally:
        con.close()


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--select", nargs="+", metavar="MODEL",
                   help="build these and what depends on them")
    p.add_argument("--full", action="store_true",
                   help="rebuild even what has not changed")
    p.add_argument("--at", default=NOW.strftime("%Y-%m-%d %H:%M"),
                   help="the run's moment, UTC (default: config.NOW)")
    a = p.parse_args(argv)
    results = run(run_at=datetime.fromisoformat(a.at), select=a.select,
                  full=a.full)
    for r in results:
        rows = f"{r.rows:>12,}" if r.rows is not None else " " * 12
        print(f"{r.status:<8} {r.relation:<40}{rows}"
              + (f"  {r.error}" if r.error else ""))
    bad = [r for r in results if r.status in ("failed", "blocked")]
    counts = [f"{sum(r.status == s for r in results)} {s}"
              for s in ("built", "skipped", "failed", "blocked")]
    print(f"{len(results)} models: " + ", ".join(counts))
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
