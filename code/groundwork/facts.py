"""
Groundwork's facts: held to marts.sql, and kept up to date a night at a
time when a full rebuild is too slow.

The runner builds the five fact tables from models/marts/ in full on
every run, as tables, with the same CREATE OR REPLACE TABLE ... AS
SELECT that drops every constraint marts.sql declares. check() tests
them as dimensions.check() tests the dimensions, and adds what a fact
promises: every key finds its dimension, every key is the version
valid on the fact's day, and an order's revenue is its lines'.

incremental() is the other way to bring fct_order_lines and
fct_orders up to date. It restates, with the models' own SQL, only the
orders that need it: those that changed since the last run (less a
lookback), those whose keys point at the unknown member or at a
version that no longer covers their day, and none that the source no
longer holds.

    check(con)                       what is wrong, as sentences
    incremental(con, since=...)      restate what changed; counts
"""
from __future__ import annotations

from datetime import datetime, timedelta

from groundwork.dimensions import declared
from groundwork.transform import runner

FACTS = ("fct_order_lines", "fct_orders", "fct_tickets",
         "fct_contracts", "fct_contract_month")
DIMS = {"account_key": "dim_account", "product_key": "dim_product",
        "supplier_key": "dim_supplier", "region_key": "dim_region"}
# A date key must be its date column as yyyymmdd.
DATES = {"fct_order_lines": [("date_key", "order_date")],
         "fct_orders": [("date_key", "order_date")],
         "fct_contracts": [("start_date_key", "start_date"),
                           ("end_date_key", "end_date")],
         "fct_contract_month": [("month_end_date_key", "month_end")]}
# Versioned keys: the version must be valid on the fact's own day.
AS_OF = {"fct_order_lines": ("order_date", ["account_key",
                                            "product_key"]),
         "fct_contract_month": ("month_end", ["account_key"])}
LOOKBACK = timedelta(hours=1)


def check(con) -> list[str]:
    """Every way the built facts break their promises."""
    problems = []
    for table in FACTS:
        cols, not_null, keys = declared(table)
        built = con.execute(
            "SELECT column_name, data_type FROM information_schema."
            "columns WHERE table_schema = 'marts' AND table_name = ? "
            "ORDER BY ordinal_position", [table]).fetchall()
        if not built:
            problems.append(f"{table}: not built")
            continue
        if built != cols:
            problems.append(f"{table}: columns differ from marts.sql")
        n = lambda sql: con.execute(sql).fetchone()[0]   # noqa: E731
        for c in sorted(not_null):
            k = n(f"SELECT count(*) FROM marts.{table} "
                  f"WHERE {c} IS NULL")
            if k:
                problems.append(f"{table}.{c}: {k:,} NULLs")
        for key in keys:
            k = n(f"SELECT count(*) FROM (SELECT {', '.join(key)} FROM "
                  f"marts.{table} GROUP BY ALL HAVING count(*) > 1)")
            if k:
                problems.append(f"{table} ({', '.join(key)}): "
                                f"{k:,} repeated")
        for col, dim in DIMS.items():
            if col in {c for c, _ in built}:
                k = n(f"SELECT count(*) FROM marts.{table} AS f "
                      f"ANTI JOIN marts.{dim} AS d USING ({col}) "
                      f"WHERE f.{col} IS NOT NULL")
                if k:
                    problems.append(f"{table}.{col}: {k:,} not in "
                                    f"{dim}")
        for key, day in DATES.get(table, []):
            k = n(f"SELECT count(*) FROM marts.{table} WHERE {key} "
                  f"<> CAST(strftime({day}, '%Y%m%d') AS INTEGER)")
            if k:
                problems.append(f"{table}.{key}: {k:,} not {day}")
        if table in AS_OF:
            day, cols_ = AS_OF[table]
            for col in cols_:
                k = n(stale_sql(table, day, col, count=True))
                if k:
                    problems.append(f"{table}.{col}: {k:,} not the "
                                    f"version of {day}")
    if not problems:
        lines, orders = con.execute(
            "SELECT (SELECT sum(revenue) FROM marts.fct_order_lines), "
            "(SELECT sum(revenue) FROM marts.fct_orders)").fetchone()
        if lines != orders:
            problems.append(f"fct_orders revenue {orders} is not the "
                            f"lines' {lines}")
    return problems


def stale_sql(table: str, day: str, col: str, count=False) -> str:
    """Rows whose versioned key is gone, or no longer covers their
    own day: a dimension changed under them."""
    dim = DIMS[col]
    what = "count(*)" if count else "DISTINCT f.order_id"
    return (f"SELECT {what} FROM marts.{table} AS f "
            f"LEFT JOIN marts.{dim} AS d USING ({col}) "
            f"WHERE d.valid_from IS NULL "
            f"OR NOT (f.{day} >= d.valid_from "
            f"AND f.{day} < coalesce(d.valid_to, DATE '9999-12-31'))")


def incremental(con, *, since: datetime, lookback=LOOKBACK,
                folder=runner.MODELS) -> dict:
    """
    Restate fct_order_lines and fct_orders for the orders that need it.

    `since` is when the last run read the source; rows changed after
    since - lookback are taken again. Returns how many orders each
    reason picked, and how many lines were written.
    """
    models = runner.discover(folder)
    start = (since - lookback).replace(tzinfo=None)
    con.execute("""
        CREATE OR REPLACE TEMP TABLE gone AS
        SELECT DISTINCT order_id FROM marts.fct_order_lines
        ANTI JOIN intermediate.int_order_lines USING (order_id)""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE changed AS
        SELECT order_id FROM staging.stg_tradeline__orders
        WHERE updated_at >= ?
        UNION
        SELECT order_id FROM staging.stg_tradeline__order_lines
        WHERE updated_at >= ?""", [start, start])
    unknown = """
        SELECT DISTINCT order_id FROM marts.fct_order_lines
        WHERE account_key = (SELECT account_key FROM marts.dim_account
                             WHERE account_id = -1)
           OR product_key = (SELECT product_key FROM marts.dim_product
                             WHERE sku = 'UNKNOWN')"""
    stale = " UNION ".join(stale_sql("fct_order_lines", "order_date", c)
                           for c in ("account_key", "product_key"))
    con.execute(f"CREATE OR REPLACE TEMP TABLE repoint AS {unknown}")
    con.execute(f"CREATE OR REPLACE TEMP TABLE stale AS {stale}")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE redo AS
        SELECT order_id FROM changed UNION SELECT order_id FROM repoint
        UNION SELECT order_id FROM stale""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE fresh_lines AS
        SELECT * FROM intermediate.int_order_lines
        SEMI JOIN redo USING (order_id)""")
    lines_sql = runner.compile_sql(models["fct_order_lines"], models) \
        .replace("intermediate.int_order_lines", "fresh_lines")
    con.execute("BEGIN")
    try:
        for table in ("fct_order_lines", "fct_orders"):
            con.execute(f"DELETE FROM marts.{table} WHERE order_id IN "
                        "(SELECT order_id FROM redo UNION "
                        "SELECT order_id FROM gone)")
        con.execute(f"INSERT INTO marts.fct_order_lines {lines_sql}")
        orders_sql = runner.compile_sql(models["fct_orders"], models) \
            .replace("marts.fct_order_lines",
                     "(SELECT * FROM marts.fct_order_lines SEMI JOIN "
                     "fresh_lines USING (order_id))")
        con.execute(f"INSERT INTO marts.fct_orders {orders_sql}")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    n = lambda t: con.execute(                         # noqa: E731
        f"SELECT count(*) FROM {t}").fetchone()[0]
    return {"changed": n("changed"), "unknown": n("repoint"),
            "stale": n("stale"), "gone": n("gone"),
            "orders": n("(SELECT DISTINCT order_id FROM fresh_lines)"),
            "lines": n("fresh_lines")}
