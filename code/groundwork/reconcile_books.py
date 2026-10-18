"""
Groundwork against the warehouses Books 1 and 2 were handed.

    python -m groundwork.reconcile_books [--warehouse W] [--at T]
                                         [--no-record]

The compat schema (compat.sql) presents Book 2's tables under their
names and columns, as views over Groundwork. This module installs it
and then proves it, three ways:

  1. Book 2, table by table. Each compat view against the table of the
     same name in meridian_ml.db, every row and every value, money to
     the cent, both ways round.
  2. Book 2's account history, which no CRM could have recorded
     exactly. Appendix F's four rules turn it into what one could have
     (code/meridian/verify_sources.py keeps them), and compat's history
     must equal that, row for row.
  3. Book 1. Its twelve quarters of key-account revenue, cost and
     orders, from data/meridian/manifest.json; and its own tables,
     which are the key accounts' slice of Book 2's from 2023.

Every line is printed and recorded in audit.book_reconciliations.
Exit 1 if anything differs.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import duckdb

from groundwork.config import (BOOK2_WAREHOUSE, MERIDIAN, NOW, ROOT,
                               WAREHOUSE)

COMPAT = Path(__file__).with_name("compat.sql")
BOOK1_WAREHOUSE = MERIDIAN / "warehouse" / "meridian.db"
BOOK1_MANIFEST = MERIDIAN / "manifest.json"

BOOK2_TABLES = ("regions", "suppliers", "products", "accounts",
                "contracts", "orders", "order_lines", "tickets",
                "price_events", "v_sales")

# Book 1's tables, as the slice of compat they are: the key accounts,
# and their orders from 1 January 2023, Book 1's first day.
KEY = ("SELECT account_id FROM compat.accounts "
       "WHERE is_key_account = 1")
BOOK1_SLICE = {
    "regions": "SELECT * FROM compat.regions",
    "suppliers": "SELECT * FROM compat.suppliers",
    "products": "SELECT * EXCLUDE (launched_on) FROM compat.products",
    "customers": f"""
        SELECT account_id, name, region_id, segment, since
        FROM compat.accounts WHERE account_id IN ({KEY})""",
    "orders": f"""
        SELECT * FROM compat.orders
        WHERE account_id IN ({KEY}) AND order_date >= '2023-01-01'""",
    "order_lines": f"""
        SELECT l.* FROM compat.order_lines AS l
        JOIN compat.orders AS o USING (order_id)
        WHERE o.account_id IN ({KEY})
          AND o.order_date >= '2023-01-01'""",
}

QUARTERS = f"""
SELECT year(CAST(o.order_date AS DATE)) || ' Q'
           || quarter(CAST(o.order_date AS DATE)) AS quarter,
       sum(round(l.qty * l.unit_price * 100))::BIGINT AS revenue,
       sum(round(l.qty * l.unit_cost * 100))::BIGINT AS cost,
       count(DISTINCT o.order_id) AS orders,
       sum(l.qty) AS units
FROM compat.orders AS o JOIN compat.order_lines AS l USING (order_id)
WHERE o.account_id IN ({KEY}) AND o.order_date >= '2023-01-01'
GROUP BY 1 ORDER BY 1
"""

DDL = """
CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS audit.book_reconciliations (
    run_at     TIMESTAMP NOT NULL,
    book       VARCHAR NOT NULL,
    item       VARCHAR NOT NULL,
    theirs     BIGINT,
    ours       BIGINT,
    differing  BIGINT NOT NULL,
    status     VARCHAR NOT NULL CHECK (status IN ('ties', 'differs')),
    PRIMARY KEY (run_at, book, item)
);
"""


@dataclass(frozen=True)
class Line:
    book: str            # "Book 1" or "Book 2"
    item: str            # a table, or a quarter
    theirs: int          # rows (or cents) in the earlier book
    ours: int            # rows (or cents) in Groundwork
    differing: int       # rows on one side only, or measures differing

    @property
    def ties(self) -> bool:
        return self.differing == 0


@dataclass(frozen=True)
class Quarter:
    quarter: str
    book1: dict          # revenue, cost (dollars), orders, units
    ours: dict

    @property
    def differing(self) -> list[str]:
        return [k for k in ("revenue", "cost", "orders", "units")
                if self.book1[k] != self.ours[k]]


# ------------------------------------------------------------- compat
def install(con) -> list[str]:
    """Create or replace the compat schema; its views, in order."""
    con.execute(COMPAT.read_text())
    return [t for (t,) in con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'compat' ORDER BY table_name").fetchall()]


def _attach(con, path: Path, alias: str) -> None:
    attached = {r[0] for r in con.execute(
        "SELECT database_name FROM duckdb_databases()").fetchall()}
    if alias not in attached:
        con.execute(f"ATTACH '{path}' AS {alias} (TYPE sqlite, "
                    "READ_ONLY)")


def _columns(con, relation: str) -> str:
    """The relation's columns, doubles rounded to the cent."""
    return ", ".join(
        f"round({name}, 2)" if kind in ("DOUBLE", "FLOAT") else name
        for name, kind, *_ in con.execute(
            f"DESCRIBE SELECT * FROM {relation}").fetchall())


def compare(con, theirs: str, ours: str) -> tuple[int, int, int]:
    """Rows in each, and rows on one side only (both ways, counting
    duplicates), for two queries with the same columns."""
    cols_t, cols_o = _columns(con, theirs), _columns(con, ours)
    a = f"SELECT {cols_t} FROM {theirs}"
    b = f"SELECT {cols_o} FROM {ours}"
    (n_t,), (n_o,) = (con.execute(f"SELECT count(*) FROM {x}")
                      .fetchone() for x in (theirs, ours))
    (only,) = con.execute(f"""
        SELECT (SELECT count(*) FROM ({a} EXCEPT ALL {b}))
             + (SELECT count(*) FROM ({b} EXCEPT ALL {a}))"""
                          ).fetchone()
    return n_t, n_o, only


# ------------------------------------------------------------- Book 2
def book2(con, path: Path = BOOK2_WAREHOUSE) -> list[Line]:
    """Every compat view against Book 2's table of the same name."""
    _attach(con, path, "book2")
    return [Line("Book 2", t,
                 *compare(con, f"book2.{t}", f"compat.{t}"))
            for t in BOOK2_TABLES]


def _rules():
    sys.path.insert(0, str(ROOT / "code" / "meridian"))
    import verify_sources
    return verify_sources.book2_history_as_a_crm_saw_it


def history(con, path: Path = BOOK2_WAREHOUSE) -> tuple[Line, dict]:
    """compat.account_history against Book 2's, under the four rules.
    Returns the line and how many rows each rule changed."""
    b2 = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        rows = b2.execute("SELECT * FROM account_history ORDER BY "
                          "account_id, valid_from, rowid").fetchall()
        now = b2.execute("SELECT account_id, segment, account_manager "
                         "FROM accounts").fetchall()
    finally:
        b2.close()
    expected, notes = _rules()(rows, now)
    ours = set(con.execute(
        "SELECT * FROM compat.account_history").fetchall())
    line = Line("Book 2", "account_history", len(rows), len(ours),
                len(ours ^ expected))
    return line, {"as a CRM could have recorded it": len(expected),
                  **notes}


# ------------------------------------------------------------- Book 1
def book1(con, path: Path = BOOK1_WAREHOUSE) -> list[Line]:
    """Book 1's tables against the slice of compat they are."""
    _attach(con, path, "book1")
    return [Line("Book 1", table,
                 *compare(con, f"book1.{table}", f"({sql})"))
            for table, sql in BOOK1_SLICE.items()]


def quarters(con, manifest: Path = BOOK1_MANIFEST) -> list[Quarter]:
    """The twelve quarters Book 1 printed, and Groundwork's."""
    printed = json.loads(manifest.read_text())["quarterly_totals"]
    ours = {q: dict(revenue=round(r / 100, 2), cost=round(c / 100, 2),
                    orders=o, units=int(u))
            for q, r, c, o, u in con.execute(QUARTERS).fetchall()}
    return [Quarter(q, {k: v[k] for k in ("revenue", "cost", "orders",
                                          "units")},
                    ours.get(q, dict.fromkeys(("revenue", "cost",
                                               "orders", "units"))))
            for q, v in sorted(printed.items())]


# ----------------------------------------------------------- the record
def record(con, lines: list[Line], at: datetime) -> int:
    con.execute(DDL)
    con.execute("DELETE FROM audit.book_reconciliations "
                "WHERE run_at = ?", [at])
    con.executemany(
        "INSERT INTO audit.book_reconciliations VALUES "
        "(?, ?, ?, ?, ?, ?, ?)",
        [[at, x.book, x.item, x.theirs, x.ours, x.differing,
          "ties" if x.ties else "differs"] for x in lines])
    return len(lines)


def quarter_lines(qs: list[Quarter]) -> list[Line]:
    return [Line("Book 1", q.quarter,
                 round(q.book1["revenue"] * 100),
                 round((q.ours["revenue"] or 0) * 100),
                 len(q.differing))
            for q in qs]


def report(b2: list[Line], hist: tuple[Line, dict], b1: list[Line],
           qs: list[Quarter]) -> str:
    def table(title, who, lines):
        out = [f"{title:<28}{who:>10}{'Groundwork':>12}{'differ':>8}"]
        for x, name in lines:
            out.append(f"  {name:<26}{x.theirs:>10,}{x.ours:>12,}"
                       f"{x.differing:>8,}")
        return out
    h, _ = hist
    out = table("Book 2, table by table", "Book 2",
                [(x, x.item) for x in b2]
                + [(h, "account_history, 4 rules")])
    out += table("Book 1, its own tables", "Book 1",
                 [(x, x.item) for x in b1])
    out.append(f"{'Book 1, quarters':<16}{'revenue':>12}{'cost':>14}"
               f"{'orders':>8}{'units':>9}  differ")
    for q in qs:
        g = q.ours
        out.append(f"  {q.quarter:<12}{g['revenue'] or 0:>14,.2f}"
                   f"{g['cost'] or 0:>14,.2f}{g['orders'] or 0:>8,}"
                   f"{g['units'] or 0:>9,}  {len(q.differing)}")
    bad = sum(not x.ties for x in [*b2, h, *b1]) + sum(
        bool(q.differing) for q in qs)
    out.append("everything reconciles" if not bad
               else f"{bad} line(s) differ")
    return "\n".join(out)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(
        prog="python -m groundwork.reconcile_books")
    ap.add_argument("--warehouse", type=Path, default=WAREHOUSE)
    ap.add_argument("--at", type=datetime.fromisoformat,
                    default=NOW.replace(tzinfo=None),
                    help="the moment to record the run as (UTC)")
    ap.add_argument("--no-record", action="store_true")
    a = ap.parse_args(argv)
    con = duckdb.connect(str(a.warehouse))
    try:
        con.execute("SET enable_progress_bar = false")
        install(con)
        b2, h = book2(con), history(con)
        b1, qs = book1(con), quarters(con)
        print(report(b2, h, b1, qs))
        lines = [*b2, h[0], *b1, *quarter_lines(qs)]
        if not a.no_record:
            record(con, lines, a.at)
    finally:
        con.close()
    return 0 if all(x.ties for x in lines) else 1


if __name__ == "__main__":
    sys.exit(main())
