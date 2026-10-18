"""
Groundwork's dimensions, built and held to their promises.

The runner builds dim_region, dim_supplier, dim_product and dim_account
from models/marts/, with CREATE OR REPLACE TABLE ... AS SELECT. A table
made that way has the columns and types its SELECT gives it, and none
of the constraints ddl/marts.sql declares: no primary key, no NOT NULL,
no UNIQUE. So check() reads those constraints from marts.sql and tests
the built tables against them, and against the rules a versioned
dimension must keep. dim_date is the exception to the runner: no source
sends it, and dates.load() fills it from the calendar.

    build(warehouse)        the runner's models, then dim_date
    check(con)              what is wrong, as sentences; [] if nothing
    python -m groundwork.dimensions [--check]
"""
from __future__ import annotations

import argparse
from datetime import datetime

import duckdb

from groundwork import dates, ddl
from groundwork.config import NOW, WAREHOUSE
from groundwork.transform import runner

DIMENSIONS = ("dim_date", "dim_region", "dim_supplier", "dim_product",
              "dim_account")
VERSIONED = {"dim_account": "account_id", "dim_product": "sku"}
UNKNOWN = {"dim_region": "region_id = -1",
           "dim_supplier": "supplier_id = -1",
           "dim_product": "sku = 'UNKNOWN'",
           "dim_account": "account_id = -1"}


def build(warehouse=WAREHOUSE, run_at: datetime = NOW) -> list:
    """The runner, then dim_date. Returns the runner's results."""
    results = runner.run(warehouse, run_at=run_at)
    con = duckdb.connect(str(warehouse))
    try:
        ddl.create(con, "marts")       # IF NOT EXISTS: dim_date only
        dates.load(con)
    finally:
        con.close()
    return results


def declared(table: str) -> tuple[list, set, list]:
    """(columns and types, NOT NULL columns, keys) from marts.sql."""
    ref = duckdb.connect()
    ddl.create(ref, "marts")
    cols = ref.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'marts' AND table_name = ? "
        "ORDER BY ordinal_position", [table]).fetchall()
    rows = ref.execute(
        "SELECT constraint_type, constraint_column_names "
        "FROM duckdb_constraints() WHERE schema_name = 'marts' "
        "AND table_name = ?", [table]).fetchall()
    ref.close()
    not_null = {c[0] for t, c in rows if t == "NOT NULL"}
    keys = sorted({tuple(c) for t, c in rows
                   if t in ("PRIMARY KEY", "UNIQUE")})
    return cols, not_null, keys


def check(con) -> list[str]:
    """Every way the built dimensions break their promises."""
    problems, present = [], set()
    for table in DIMENSIONS:
        cols, not_null, keys = declared(table)
        built = con.execute(
            "SELECT column_name, data_type "
            "FROM information_schema.columns WHERE table_schema = "
            "'marts' AND table_name = ? ORDER BY ordinal_position",
            [table]).fetchall()
        if not built:
            problems.append(f"{table}: not built")
            continue
        present.add(table)
        if built != cols:
            problems.append(f"{table}: columns differ from marts.sql")
        for c in sorted(not_null):
            (n,) = con.execute(f"SELECT count(*) FROM marts.{table} "
                               f"WHERE {c} IS NULL").fetchone()
            if n:
                problems.append(f"{table}.{c}: {n:,} NULLs")
        for key in keys:
            k = ", ".join(key)
            (n,) = con.execute(
                f"SELECT count(*) FROM (SELECT {k} FROM marts.{table} "
                f"GROUP BY ALL HAVING count(*) > 1)").fetchone()
            if n:
                problems.append(f"{table} ({k}): {n:,} repeated")
        if table in UNKNOWN:
            (n,) = con.execute(f"SELECT count(*) FROM marts.{table} "
                               f"WHERE {UNKNOWN[table]}").fetchone()
            if n != 1:
                problems.append(f"{table}: {n} unknown members, not 1")
    for table, nat in VERSIONED.items():
        if table in present:
            problems += versions(con, table, nat)
    (n,) = con.execute("SELECT count(*) FROM intermediate."
                       "int_account_identity WHERE account_id IS NULL"
                       ).fetchone()
    if n:
        problems.append(f"int_account_identity: {n:,} ids unresolved")
    return problems


def versions(con, table: str, nat: str) -> list[str]:
    """A versioned dimension's ranges: touching, one current."""
    (bad,) = con.execute(f"""
        SELECT count(*) FROM (
            SELECT valid_from, valid_to, is_current,
                   lead(valid_from) OVER w AS next_from
            FROM marts.{table} WINDOW w AS (PARTITION BY {nat}
                                            ORDER BY valid_from))
        WHERE valid_to IS DISTINCT FROM next_from
           OR valid_from >= coalesce(valid_to, DATE '9999-12-31')
           OR is_current <> (valid_to IS NULL)""").fetchone()
    return [f"{table}: {bad:,} versions overlap, gap or end early"] \
        if bad else []


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("--check", action="store_true",
                   help="check what is built; build nothing")
    a = p.parse_args(argv)
    if not a.check:
        for r in build():
            if r.status != "skipped":
                rows = f"{r.rows:>10,}" if r.rows is not None else ""
                print(f"{r.status:<8} {r.relation:<38}{rows}")
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    try:
        problems = check(con)
        for t in DIMENSIONS:
            (n,) = con.execute(
                f"SELECT count(*) FROM marts.{t}").fetchone()
            print(f"marts.{t:<14}{n:>8,} rows")
    finally:
        con.close()
    for line in problems:
        print("PROBLEM", line)
    print("every promise kept" if not problems
          else f"{len(problems)} problems")
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
