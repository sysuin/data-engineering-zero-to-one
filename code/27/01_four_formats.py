# One day's rows in four formats, counted by Python and by DuckDB.
import argparse
import csv
import gzip
import json
import sqlite3
import sys
from datetime import date, timedelta

import duckdb
import pyarrow.parquet as pq

from groundwork.config import LANDING, SANDBOX, TRADELINE_DB

PARQUET = SANDBOX / "ch27" / "accounts.parquet"


def paths(night):
    """The Relay export and the portal's hourly files for one day."""
    accounts = LANDING / "relay" / night / "accounts.csv"
    hours = LANDING / "portal" / night.replace("-", "/")
    return accounts, hours


def python_counts(night, day_after):
    """Rows in each source, counted without DuckDB."""
    accounts, hours = paths(night)
    with open(accounts, newline="", encoding="utf-8-sig") as f:
        n_csv = sum(1 for _ in csv.DictReader(f))
    n_json = 0
    for path in sorted(hours.glob("*.jsonl.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            n_json += sum(1 for line in f if json.loads(line))
    uri = f"file:{TRADELINE_DB}?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    (n_sql,) = con.execute(
        "SELECT count(*) FROM orders WHERE placed_at >= ? "
        "AND placed_at < ?", (night, day_after)).fetchone()
    con.close()
    n_parquet = pq.read_metadata(PARQUET).num_rows     # the footer
    return [n_csv, n_json, n_sql, n_parquet]


def duckdb_counts(night, day_after):
    """The same rows, each counted by one line of SQL."""
    accounts, hours = paths(night)
    con = duckdb.connect()
    con.execute(f"ATTACH '{TRADELINE_DB}' AS tl "
                "(TYPE sqlite, READ_ONLY)")
    con.execute(f"COPY (FROM read_csv('{accounts}')) TO '{PARQUET}' "
                "(FORMAT parquet)")
    sources = [f"read_csv('{accounts}')",
               f"read_json('{hours}/*.jsonl.gz')",
               "tl.orders WHERE placed_at >= ? AND placed_at < ?",
               f"read_parquet('{PARQUET}')"]
    return [con.execute(f"SELECT count(*) FROM {s}",
                        [night, day_after] if "?" in s else None
                        ).fetchone()[0] for s in sources]


def main(argv=None):
    parser = argparse.ArgumentParser(description="Count a day twice.")
    parser.add_argument("--night", default="2025-12-31",
                        help="YYYY-MM-DD: a night with a Relay export")
    args = parser.parse_args(argv)
    day_after = str(date.fromisoformat(args.night) + timedelta(days=1))
    PARQUET.parent.mkdir(parents=True, exist_ok=True)
    db = duckdb_counts(args.night, day_after)       # writes the Parquet
    py = python_counts(args.night, day_after)
    print(f"{args.night}  {'source':<22}{'Python':>9}{'DuckDB':>9}")
    for name, a, b in zip(["Relay accounts.csv", "portal *.jsonl.gz",
                           "Tradeline orders", "accounts.parquet"],
                          py, db):
        print(f"{'':12}{name:<22}{a:>9,}{b:>9,}")
    print("agree" if py == db else "DISAGREE")
    return 0 if py == db else 1


if __name__ == "__main__":          # a command, not an import
    sys.exit(main())
