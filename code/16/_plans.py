"""
Chapter 16's helpers: a copy of the warehouse to measure, query plans
read as data, and dbt models rendered the way a full build renders
them. Not printed in the chapter.

Every listing measures data/sandbox/ch16/warehouse.duckdb, a copy of
Groundwork v0.6 taken once. The real warehouse is never opened for
writing, and is read only to make the copy.

The copy is written table by table rather than copied byte for byte.
How a table lies on disk (its row groups, and the statistics the
optimiser estimates from) depends on the order its rows were written,
and every rebuild of the real warehouse writes them in a new order.
One thread and a fixed ORDER BY give the same copy every time.
"""
import json
import shutil
from pathlib import Path

import duckdb
import jinja2

from groundwork.config import ROOT, SANDBOX, WAREHOUSE

HERE = SANDBOX / "ch16"
COPY = HERE / "warehouse.duckdb"
PROJECT = ROOT / "dbt" / "groundwork"
MODELS16 = Path(__file__).resolve().parent / "models"


# The fact's lines in an order that has nothing to do with the day, as
# a parallel build leaves them; every other table in ORDER BY ALL.
ORDER = {"marts.fct_order_lines": "account_key, order_id, line_no"}


def warehouse() -> Path:
    """The chapter's copy of the warehouse, made the first time."""
    if COPY.exists():
        return COPY
    # Built under its final name, so views that name the catalogue
    # 'warehouse' bind, then moved into place.
    part = HERE / "copying" / COPY.name
    shutil.rmtree(part.parent, ignore_errors=True)
    part.parent.mkdir(parents=True)
    con = duckdb.connect(str(part), config={"threads": 1})
    con.execute(f"ATTACH '{WAREHOUSE}' AS w (READ_ONLY)")
    for (schema,) in con.execute(
            "SELECT schema_name FROM duckdb_schemas() "
            "WHERE database_name = 'w' AND NOT internal "
            "ORDER BY 1").fetchall():
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    tables = con.execute(
        "SELECT schema_name || '.' || table_name, sql "
        "FROM duckdb_tables() WHERE database_name = 'w' "
        "ORDER BY 1").fetchall()
    for table, ddl in tables:
        con.execute(ddl)
        con.execute(f"INSERT INTO {table} FROM w.{table} "
                    f"ORDER BY {ORDER.get(table, 'ALL')}")
    views = con.execute(
        "SELECT sql FROM duckdb_views() WHERE database_name = 'w' "
        "AND NOT internal ORDER BY schema_name, view_name").fetchall()
    while views:                  # a view waits for the views it reads
        waiting = []
        for (ddl,) in views:
            try:
                con.execute(ddl)
            except (duckdb.CatalogException, duckdb.BinderException):
                waiting.append((ddl,))
        if len(waiting) == len(views):
            raise RuntimeError(f"cannot create {waiting[0][0][:60]}")
        views = waiting
    con.execute("DETACH w")
    con.execute("CHECKPOINT")
    con.close()
    part.rename(COPY)
    shutil.rmtree(part.parent)
    return COPY


def connect(read_only=True, **config):
    """A connection to the copy. Its catalogue is named 'warehouse'."""
    return duckdb.connect(str(warehouse()), read_only=read_only,
                          config=config)


def analyze(con, sql: str) -> dict:
    """Run a query under EXPLAIN ANALYZE; the profile as a dict."""
    (_, text), = con.execute(
        "EXPLAIN (ANALYZE, FORMAT json) " + sql).fetchall()
    return json.loads(text)


def explain(con, sql: str) -> dict:
    """The plans DuckDB makes for a query, without running it."""
    con.execute("SET explain_output = 'all'")
    plans = {kind: json.loads(text)[0] for kind, text in
             con.execute("EXPLAIN (FORMAT json) " + sql).fetchall()}
    con.execute("SET explain_output = 'physical_only'")
    return plans


def operators(node: dict, depth: int = 0):
    """(depth, operator) for every operator in a plan, top down."""
    if "operator_name" in node or "name" in node:
        yield depth, node
        depth += 1
    for child in node.get("children", []):
        yield from operators(child, depth)


def name(node: dict) -> str:
    return (node.get("operator_name") or node.get("name")).strip()


def scans(profile: dict) -> list[dict]:
    """The operators that read a table or files."""
    return [n for _, n in operators(profile)
            if "SCAN" in name(n) or "PARQUET" in name(n)]


# --------------------------------------------------- dbt, rendered
def _schemas() -> dict:
    where = {}
    for path in (PROJECT / "models").rglob("*.sql"):
        where[path.stem] = path.parent.name
    for path in (PROJECT / "seeds").glob("*.csv"):
        where[path.stem] = "seeds"
    return where


def render(path, **variables) -> str:
    """A model's SQL as a full build compiles it (no is_incremental)."""
    where = _schemas()
    env = jinja2.Environment()
    defaults = {"since": None, "lookback_hours": 1, "night": None}
    defaults.update(variables)
    context = {
        "ref": lambda m: f"{where[m]}.{m}",
        "source": lambda s, t: f"{s}.{t}",
        "config": lambda **_: "",
        "is_incremental": lambda: False,
        "var": lambda k, default=None: defaults.get(k, default),
        "this": "this",
    }
    macros = "".join(p.read_text() for p in
                     sorted((PROJECT / "macros").glob("*.sql")))
    text = Path(path).read_text()
    return env.from_string(macros + text).render(**context).strip()


def model(name: str, **variables) -> str:
    """Groundwork's model, as Chapter 14 left it, rendered."""
    (path,) = (PROJECT / "models").rglob(f"{name}.sql")
    return render(path, **variables)


def faster(name: str, **variables) -> str:
    """This chapter's version of a model, rendered."""
    return render(MODELS16 / f"{name}.sql", **variables)


def tidy(*names: str) -> None:
    """Delete scratch files and folders in the chapter's sandbox."""
    for n in names:
        p = HERE / n
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        elif p.exists():
            p.unlink()
