"""
Reconciliation: the warehouse tied to systems outside it (Chapter 15).

A test asks whether the warehouse agrees with itself. A reconciliation
asks whether it agrees with something it did not make: the ledger that
finance closes, or a source's own count of its rows. Three of them,
each a function, all three behind one command:

    revenue(con)          every month the ledger has closed, by
                          region: the warehouse's revenue against the
                          ledger's sales journal, to the cent, and each
                          journal finance posted by hand against
                          finance's register of them
    bridge(con, month)    the dashboard's number for a month, walked to
                          the ledger's one named cause at a time; it
                          must close exactly
    counts(con)           the rows each source holds against the rows
                          the warehouse holds, every difference named

    python -m groundwork.reconcile [--month 2025-11] [--warehouse W]

prints all three, records them in audit.reconciliations, and exits 1 if
anything does not tie. Money is integer cents throughout; the one
floating-point number is the dashboard's, which is where the bridge
starts.
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import duckdb

from groundwork.config import (LANDING, NOW, ROOT, TRADELINE_DB,
                               WAREHOUSE)

JOURNALS = (ROOT / "dbt" / "groundwork" / "seeds" / "finance"
            / "ledger_journals.csv")
"""Finance's register of the journals it posts by hand (a dbt seed)."""

SALES = "Sales journal"
"""The ledger's description of the lines the order system feeds."""

CENT = Decimal("0.01")


def _month(value) -> date:
    """A month as the date of its first day, from '2025-11' or a date."""
    if isinstance(value, date):
        return value.replace(day=1)
    year, month = str(value)[:7].split("-")
    return date(int(year), int(month), 1)


def _next(month: date) -> date:
    return date(month.year + month.month // 12, month.month % 12 + 1, 1)


def dollars(cents: int) -> Decimal:
    return (Decimal(cents) * CENT).quantize(CENT)


def money(d: Decimal) -> str:
    """Dollars with thousands separators; every digit a float had."""
    if d == d.quantize(CENT):
        return f"{d.quantize(CENT):,}"
    return f"{d:,f}"


# --------------------------------------------------------------- journals
@dataclass(frozen=True)
class Journal:
    """One line of finance's register: a journal no source holds."""
    period: date
    region: str
    cents: int
    description: str
    reason: str
    confirmed_by: str


def journals(path: Path = JOURNALS) -> list[Journal]:
    """Finance's register, as the file in the dbt project holds it."""
    with open(path, newline="", encoding="utf-8") as f:
        return [Journal(_month(r["period"]), r["region"],
                        int(Decimal(r["amount"]) * 100),
                        r["description"], r["reason"], r["confirmed_by"])
                for r in csv.DictReader(f)]


# ---------------------------------------------------------------- revenue
@dataclass
class Cell:
    """One month and region: what each side says, in cents."""
    period: date
    region: str
    warehouse: int                  # sum of fct_order_lines.revenue
    sales: int                      # the ledger's sales journal
    manual: list[tuple[str, int]]   # lines posted by hand
    documented: list[tuple[str, int]]   # the register's, for the cell

    @property
    def ledger(self) -> int:
        return self.sales + sum(c for _, c in self.manual)

    @property
    def ties(self) -> bool:
        """The warehouse equals the sales journal, to the cent."""
        return self.warehouse == self.sales

    @property
    def documented_ok(self) -> bool:
        """Every hand-posted line is in the register, and no other."""
        return sorted(self.manual) == sorted(self.documented)

    @property
    def closes(self) -> bool:
        """Warehouse plus documented journals is the ledger's total."""
        return self.warehouse + sum(c for _, c in self.documented) \
            == self.ledger


REVENUE_SQL = f"""
WITH ours AS (
    SELECT date_trunc('month', f.order_date)::DATE AS period,
           r.region_code AS region,
           CAST(sum(f.revenue) * 100 AS BIGINT) AS cents
    FROM marts.fct_order_lines AS f
    JOIN marts.dim_region AS r USING (region_key)
    GROUP BY ALL
),
ledger AS (
    SELECT period_month AS period, region_code AS region,
           coalesce(sum(amount_cents) FILTER (
               WHERE description = '{SALES}'), 0) AS sales,
           list((description, amount_cents) ORDER BY description)
               FILTER (WHERE description <> '{SALES}') AS manual
    FROM staging.stg_ledger__gl_revenue
    GROUP BY ALL
)
SELECT period, region, coalesce(o.cents, 0), coalesce(l.sales, 0),
       coalesce(l.manual, [])
FROM ledger AS l
FULL JOIN ours AS o USING (period, region)
WHERE period IN (SELECT period FROM ledger)
ORDER BY period, region
"""


def revenue(con, register: list[Journal] | None = None) -> list[Cell]:
    """Every closed month x region, the warehouse against the ledger."""
    register = journals() if register is None else register
    cells = []
    for period, region, ours, sales, manual in \
            con.execute(REVENUE_SQL).fetchall():
        mine = [(j.description, j.cents) for j in register
                if j.period == period and j.region == region]
        cells.append(Cell(period, region, ours, sales,
                          [(d, c) for d, c in manual], mine))
    return cells


# ----------------------------------------------------------------- bridge
@dataclass(frozen=True)
class Step:
    """One cause, and how much it moves the number, in dollars."""
    name: str
    amount: Decimal
    orders: int | None = None
    note: str = ""


@dataclass
class Bridge:
    """From the dashboard's number to the ledger's, cause by cause."""
    month: date
    start: Decimal                  # the dashboard, as it printed
    steps: list[Step] = field(default_factory=list)
    ledger: Decimal = Decimal(0)

    @property
    def end(self) -> Decimal:
        return self.start + sum((s.amount for s in self.steps),
                                Decimal(0))

    @property
    def remaining(self) -> Decimal:
        """What no step explains. The bridge closes when it is zero."""
        return self.ledger - self.end

    @property
    def closes(self) -> bool:
        return self.remaining == 0


DASHBOARD_SQL = """
SELECT SUM(l.qty * l.unit_price_cents / 100.0),
       SUM(l.qty * l.unit_price_cents),
       count(DISTINCT o.id)
FROM {orders} o
JOIN {lines} l ON l.order_id = o.id
WHERE o.placed_at >= ? AND o.placed_at < ?
"""


def dashboard(con, month, tradeline: Path | None = None):
    """
    The dashboard's query for a month: every order Tradeline holds,
    by the UTC month of placed_at, summed as floating point. Returns
    (the float, the same rows in exact cents, orders). Run against
    Tradeline itself when `tradeline` is given, else against raw's
    copy of it. One thread, so the float repeats run to run.
    """
    m = _month(month)
    if tradeline is not None:
        con = duckdb.connect(config={"threads": 1})
        con.execute(f"ATTACH '{tradeline}' AS t (TYPE sqlite, "
                    "READ_ONLY)")
        names = {"orders": "t.orders", "lines": "t.order_lines"}
    else:
        con.execute("SET threads = 1")
        names = {"orders": "raw.tradeline_orders_current",
                 "lines": "raw.tradeline_order_lines_current"}
    row = con.execute(DASHBOARD_SQL.format(**names),
                      [m.isoformat(), _next(m).isoformat()]).fetchone()
    if tradeline is not None:
        con.close()
    return row


BRIDGE_SQL = """
WITH cancelled AS (
    SELECT count(DISTINCT o.order_id) AS orders,
           coalesce(sum(l.qty * l.unit_price_cents), 0) AS cents
    FROM staging.stg_tradeline__orders AS o
    JOIN staging.stg_tradeline__order_lines AS l USING (order_id)
    WHERE o.status = 'cancelled'
      AND o.placed_at >= $first AND o.placed_at < $next
),
lines AS (
    SELECT order_id, revenue,
           placed_at >= $first AND placed_at < $next AS utc_in,
           order_date >= $first AND order_date < $next AS local_in
    FROM marts.fct_order_lines
)
SELECT
    (SELECT orders FROM cancelled), (SELECT cents FROM cancelled),
    count(DISTINCT order_id) FILTER (WHERE utc_in AND NOT local_in),
    coalesce(CAST(sum(revenue) FILTER (WHERE utc_in AND NOT local_in)
                  * 100 AS BIGINT), 0),
    count(DISTINCT order_id) FILTER (WHERE local_in AND NOT utc_in),
    coalesce(CAST(sum(revenue) FILTER (WHERE local_in AND NOT utc_in)
                  * 100 AS BIGINT), 0),
    coalesce(CAST(sum(revenue) FILTER (WHERE local_in)
                  * 100 AS BIGINT), 0)
FROM lines
"""


def bridge(con, month, *, start: float | None = None,
           exact: int | None = None, orders: int | None = None,
           register: list[Journal] | None = None) -> Bridge:
    """
    Walk the dashboard's revenue for `month` to the ledger's. `start`
    is the dashboard's number as it printed, and `exact` the same rows
    in cents; both are computed over raw unless given. The steps, in
    this order: the floating-point dust; the cancelled orders; the
    orders the depot dated the month before (placed in the first hours
    of the month, UTC); those it dated this month (the first hours of
    the next); each journal finance posted by hand. Each step is
    measured, never taken as the difference left over; whatever is
    left is `remaining`.
    """
    m = _month(month)
    if start is None or exact is None:
        start, exact, orders = dashboard(con, m)
    register = journals() if register is None else register
    first = datetime(m.year, m.month, 1)
    nxt = datetime(_next(m).year, _next(m).month, 1)
    (c_orders, c_cents, out_orders, out_cents, in_orders, in_cents,
     ours) = con.execute(BRIDGE_SQL, {"first": first, "next": nxt}
                         ).fetchone()
    cells = [c for c in revenue(con, register) if c.period == m]

    b = Bridge(m, Decimal(repr(start)))
    b.steps.append(Step("floating-point dust",
                        dollars(exact) - b.start, None,
                        "the same rows, summed in cents"))
    b.steps.append(Step("cancelled orders", -dollars(c_cents), c_orders,
                        "never happened; finance never booked them"))
    before = date(m.year - (m.month == 1), (m.month - 2) % 12 + 1, 1)
    b.steps.append(Step(f"UTC {m:%b}, the depot's {before:%b}",
                        -dollars(out_cents), out_orders,
                        "placed in the first hours of the month"))
    b.steps.append(Step(f"UTC {_next(m):%b}, the depot's {m:%b}",
                        dollars(in_cents), in_orders,
                        "placed in the first hours of the next"))
    sales = sum(c.sales for c in cells)
    if sales != ours:
        b.steps.append(Step("sales journal against the warehouse",
                            dollars(sales - ours), None,
                            "UNEXPLAINED: a cell does not tie"))
    for c in cells:
        documented = dict(c.documented)
        for description, cents in c.manual:
            known = documented.get(description) == cents
            b.steps.append(Step(f"manual journal, {c.region}",
                                dollars(cents), None,
                                description if known else
                                f"NOT in the register: {description}"))
    b.ledger = dollars(sum(c.ledger for c in cells))
    return b


# ----------------------------------------------------------------- counts
@dataclass(frozen=True)
class Count:
    """A source's count against the warehouse's, with named reasons."""
    name: str
    source: int
    warehouse: int
    explained: tuple[tuple[str, int], ...] = ()

    @property
    def ties(self) -> bool:
        return self.source == self.warehouse + sum(
            n for _, n in self.explained)


def _rows(path: Path) -> int:
    with open(path, newline="", encoding="utf-8-sig") as f:
        return sum(1 for _ in csv.DictReader(f))


def counts(con, tradeline: Path = TRADELINE_DB,
           landing: Path = LANDING) -> list[Count]:
    """
    Each source's own count, asked separately, against the warehouse.
    Tradeline is asked directly; Relay and the ledger by their files in
    the landing zone (Relay's newest export); Frontline by the total
    its API gave the last load, which the load log kept.
    """
    src = sqlite3.connect(f"file:{tradeline}?mode=ro", uri=True)
    orders, cancelled = src.execute(
        "SELECT count(*), sum(status = 'cancelled') FROM orders"
    ).fetchone()
    lines, cancelled_lines = src.execute(
        "SELECT count(*), sum(o.status = 'cancelled') FROM order_lines "
        "JOIN orders AS o ON o.id = order_lines.order_id").fetchone()
    src.close()
    one = lambda sql: con.execute(sql).fetchone()[0]       # noqa: E731
    night = max(p for p in (landing / "relay").iterdir()
                if p.name[:2] == "20" and p.is_dir())
    tickets = one("""SELECT source_count FROM audit.load_log
                     WHERE source = 'frontline' AND table_name =
                     'tickets' AND status = 'succeeded'
                     ORDER BY load_date DESC, started_at DESC LIMIT 1""")
    ledger = sum(_rows(p) for p in (landing / "ledger").glob("*.csv"))
    return [
        Count("Tradeline orders", orders,
              one("SELECT count(*) FROM marts.fct_orders"),
              (("cancelled", cancelled),)),
        Count("Tradeline lines", lines,
              one("SELECT count(*) FROM marts.fct_order_lines"),
              (("cancelled", cancelled_lines),)),
        Count("Relay accounts",
              _rows(night / "accounts.csv"),
              one("SELECT count(*) FROM marts.dim_account "
                  "WHERE is_current AND account_id > 0")),
        Count("Relay contracts",
              _rows(night / "contracts.csv"),
              one("SELECT count(*) FROM marts.fct_contracts")),
        Count("Frontline tickets", tickets,
              one("SELECT count(*) FROM marts.fct_tickets")),
        Count("ledger lines", ledger,
              one("SELECT count(*) FROM staging.stg_ledger__gl_revenue")),
    ]


# ----------------------------------------------------------------- record
DDL = """
CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS audit.reconciliations (
    run_at     TIMESTAMP NOT NULL,
    kind       VARCHAR NOT NULL,     -- revenue, bridge or count
    item       VARCHAR NOT NULL,
    source     DECIMAL(20, 2),       -- the other system's number
    warehouse  DECIMAL(20, 2),       -- the warehouse's
    status     VARCHAR NOT NULL,     -- ties, explained or differs
    note       VARCHAR,
    PRIMARY KEY (run_at, kind, item)
);
"""


def record(con, run_at: datetime, cells: list[Cell], b: Bridge | None,
           cs: list[Count]) -> int:
    """Write one run's results to audit.reconciliations; rows written."""
    con.execute(DDL)
    con.execute("DELETE FROM audit.reconciliations WHERE run_at = ?",
                [run_at])
    rows = []
    for c in cells:
        status = ("ties" if c.ties and not c.manual else
                  "explained" if c.ties and c.closes else "differs")
        rows.append((run_at, "revenue", f"{c.period:%Y-%m} {c.region}",
                     dollars(c.ledger), dollars(c.warehouse), status,
                     "; ".join(d for d, _ in c.manual) or None))
    if b is not None:
        for s in b.steps:
            rows.append((run_at, "bridge", f"{b.month:%Y-%m} {s.name}",
                         None, s.amount, "differs" if s.note.startswith(
                             ("NOT", "UNEXPLAINED")) else "explained",
                         s.note))
        rows.append((run_at, "bridge", f"{b.month:%Y-%m} remaining",
                     b.ledger, b.end, "ties" if b.closes else "differs",
                     None))
    for c in cs:
        rows.append((run_at, "count", c.name, c.source, c.warehouse,
                     "ties" if c.ties and not c.explained else
                     "explained" if c.ties else "differs",
                     "; ".join(f"{k} {n:,}" for k, n in c.explained)
                     or None))
    con.executemany("INSERT INTO audit.reconciliations VALUES "
                    "(?, ?, ?, ?, ?, ?, ?)", rows)
    return len(rows)


# -------------------------------------------------------------------- CLI
def report(cells: list[Cell], b: Bridge | None,
           cs: list[Count]) -> list[str]:
    """What the command prints: three short sections, 68 columns."""
    out = []
    manual = [(c, d, n) for c in cells for d, n in c.manual]
    out.append(f"revenue  {len(cells)} months x regions, "
               f"{sum(c.ties for c in cells)} tie to the sales journal")
    out.append(f"         journals posted by hand {len(manual)}, "
               f"in the register "
               f"{sum(c.documented_ok for c in cells if c.manual)}")
    out.append(f"         warehouse + register = ledger in "
               f"{sum(c.closes for c in cells)} of {len(cells)}")
    for c in cells:
        if not (c.ties and c.closes):
            out.append(f"  DIFFERS {c.period:%Y-%m} {c.region}: "
                       f"warehouse {dollars(c.warehouse):,} "
                       f"ledger {dollars(c.ledger):,}")
    if b is not None:
        out.append(f"bridge   {b.month:%B %Y}, the dashboard's number "
                   "to the ledger's")
        out.append(f"  {'the dashboard, as it printed':<40}"
                   f"{b.start!s:>24}")
        for s in b.steps:
            n = f"{s.orders:,}" if s.orders is not None else ""
            out.append(f"  {s.name:<34}{n:>6}{money(s.amount):>24}")
        out.append(f"  {'the ledger':<40}{money(b.ledger):>24}")
        out.append(f"  {'remaining':<40}{money(b.remaining):>24}")
    out.append(f"counts   {sum(c.ties for c in cs)} of {len(cs)} tie, "
               "source = warehouse + what it leaves out")
    for c in cs:
        why = "".join(f" + {n:,} {k}" for k, n in c.explained)
        mark = "=" if c.ties else "DIFFERS"
        out.append(f"  {c.name:<18}{c.source:>10,} {mark} "
                   f"{c.warehouse:>9,}{why}")
    return out


def main(argv=None) -> int:
    p = argparse.ArgumentParser(prog="python -m groundwork.reconcile",
                                description=__doc__.split("\n\n")[0])
    p.add_argument("--warehouse", type=Path, default=WAREHOUSE)
    p.add_argument("--month", help="the month to bridge (YYYY-MM); "
                   "default the ledger's last closed month")
    p.add_argument("--at", default=None,
                   help="the run's moment, for the record (UTC)")
    p.add_argument("--no-record", action="store_true",
                   help="print only; write nothing to the warehouse")
    a = p.parse_args(argv)
    con = duckdb.connect(str(a.warehouse), read_only=a.no_record)
    con.execute("SET enable_progress_bar = false")
    cells = revenue(con)
    month = _month(a.month) if a.month else max(c.period for c in cells)
    b = bridge(con, month)
    cs = counts(con)
    print("\n".join(report(cells, b, cs)))
    ok = (all(c.ties and c.closes and (c.documented_ok or not c.manual)
              for c in cells) and b.closes and all(c.ties for c in cs))
    if not a.no_record:
        run_at = (datetime.fromisoformat(a.at) if a.at
                  else NOW.replace(tzinfo=None))
        n = record(con, run_at, cells, b, cs)
        print(f"recorded {n} rows in audit.reconciliations at {run_at}")
    con.close()
    print("everything ties" if ok else "SOMETHING DOES NOT TIE")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
