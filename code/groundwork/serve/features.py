"""
Foresight's renewal features, served point in time.

features.renewal_snapshot has one row per contract, described as it
stood on the morning of its 90-day mark, the day the account team
decides whom to call: Book 2's framing. Every feature reads only what
the warehouse knew before that morning. Orders and tickets dated on
or after the mark are not counted; the account's segment and manager
are the dim_account version in force the day before; nothing that is
recorded later (the outcome, the notice date, the cancellation
reason) is a feature at all. The label, not_renewed, is the one
column written afterwards, and it is NULL until the contract has
ended.

    build(con, first_end=..., today=...)   the table; its row count
    check(con, cohorts=3)                  what is wrong, as sentences

check() tests the property directly: rebuild a cohort from a
warehouse cut off at its mark, with every later fact and every later
version of an account removed, and the cohort's rows must not change.
"""
from __future__ import annotations

from datetime import date

from groundwork.config import TODAY

MARK_DAYS = 90
FIRST_END = date(2023, 1, 1)        # Book 2's first training contracts
FEATURES = ("days_since_order", "orders_90d", "orders_prev_90d",
            "spend_365", "tickets_90d", "tenure_days", "segment",
            "owner", "region", "term_months", "legacy_terms",
            "discount_pct")

# The facts and the account versions as the warehouse holds them, or
# as it held them on the morning of {cut}.
SOURCES = {
    "now": """
    accounts AS (SELECT * FROM marts.dim_account),
    orders AS (SELECT order_date, account_key, revenue
               FROM marts.fct_orders),
    tickets AS (SELECT strptime(CAST(opened_date_key AS VARCHAR),
                                '%Y%m%d')::DATE AS opened_date,
                       account_key
                FROM marts.fct_tickets)""",
    "cut": """
    accounts AS (SELECT * REPLACE (CASE WHEN valid_to >= DATE '{cut}'
                                        THEN NULL ELSE valid_to END
                                   AS valid_to)
                 FROM marts.dim_account
                 WHERE valid_from < DATE '{cut}'),
    orders AS (SELECT order_date, account_key, revenue
               FROM marts.fct_orders
               WHERE order_date < DATE '{cut}'),
    tickets AS (SELECT * FROM (
                    SELECT strptime(CAST(opened_date_key AS VARCHAR),
                                    '%Y%m%d')::DATE AS opened_date,
                           account_key
                    FROM marts.fct_tickets)
                WHERE opened_date < DATE '{cut}')""",
}

SNAPSHOT = """
WITH {sources},
ids AS (SELECT DISTINCT account_key, account_id FROM accounts),
rows AS (
    SELECT c.contract_id, i.account_id, c.end_date,
           CAST(c.end_date - INTERVAL {days} DAY AS DATE) AS mark,
           c.term_months, c.legacy_terms, c.discount_pct, c.outcome
    FROM marts.fct_contracts AS c
    JOIN marts.dim_account AS i USING (account_key)
    WHERE c.end_date >= ? AND c.end_date - INTERVAL {days} DAY <= ?
),
o AS (                         -- orders strictly before the mark
    SELECT r.contract_id,
           min(date_diff('day', x.order_date, r.mark)) AS since,
           count(*) FILTER (date_diff('day', x.order_date, r.mark)
                            <= 90) AS n90,
           count(*) FILTER (date_diff('day', x.order_date, r.mark)
                            > 90 AND date_diff('day', x.order_date,
                                               r.mark) <= 180) AS p90,
           coalesce(sum(x.revenue) FILTER (date_diff(
               'day', x.order_date, r.mark) <= 365), 0) AS spend
    FROM rows AS r
    JOIN (SELECT o.order_date, o.revenue, i.account_id
          FROM orders AS o JOIN ids AS i USING (account_key)) AS x
      ON x.account_id = r.account_id AND x.order_date < r.mark
    GROUP BY r.contract_id
),
t AS (                         -- tickets in the 90 days before it
    SELECT r.contract_id, count(*) AS n90
    FROM rows AS r
    JOIN (SELECT t.opened_date, i.account_id
          FROM tickets AS t JOIN ids AS i USING (account_key)) AS x
      ON x.account_id = r.account_id AND x.opened_date < r.mark
     AND x.opened_date >= r.mark - INTERVAL 90 DAY
    GROUP BY r.contract_id
)
SELECT r.contract_id, r.account_id, r.mark AS as_of, r.end_date,
       o.since AS days_since_order,
       coalesce(o.n90, 0) AS orders_90d,
       coalesce(o.p90, 0) AS orders_prev_90d,
       CAST(coalesce(o.spend, 0) AS DECIMAL(18,2)) AS spend_365,
       coalesce(t.n90, 0) AS tickets_90d,
       date_diff('day', cur.customer_since, r.mark) AS tenure_days,
       a.segment, a.owner, g.region_name AS region,
       r.term_months, r.legacy_terms, r.discount_pct,
       CASE WHEN r.end_date < ?           -- the label, written later
            THEN r.outcome = 'not_renewed' END AS not_renewed
FROM rows AS r
LEFT JOIN o USING (contract_id)
LEFT JOIN t USING (contract_id)
LEFT JOIN accounts AS a           -- the version of the day before
  ON a.account_id = r.account_id
 AND r.mark - INTERVAL 1 DAY >= a.valid_from
 AND r.mark - INTERVAL 1 DAY < coalesce(a.valid_to, DATE '9999-12-31')
LEFT JOIN accounts AS cur          -- Type 1 columns: the latest row
  ON cur.account_id = r.account_id AND cur.valid_to IS NULL
LEFT JOIN marts.dim_region AS g ON g.region_key = cur.region_key
ORDER BY r.contract_id
"""


def snapshot_sql(cut: date | None = None) -> str:
    """The query, over today's marts or over the marts cut at `cut`."""
    sources = (SOURCES["now"] if cut is None
               else SOURCES["cut"].format(cut=cut.isoformat()))
    return SNAPSHOT.format(sources=sources.strip(), days=MARK_DAYS)


def build(con, first_end: date = FIRST_END, today: date = TODAY) -> int:
    """features.renewal_snapshot: every contract ending on or after
    first_end whose mark has arrived by today. Returns its rows."""
    con.execute("CREATE SCHEMA IF NOT EXISTS features")
    con.execute("CREATE OR REPLACE TABLE features.renewal_snapshot AS "
                + snapshot_sql(), [first_end, today, today])
    con.execute("COMMENT ON TABLE features.renewal_snapshot IS 'One "
                "row per contract, as it stood on the morning of its "
                "90-day mark.'")
    return con.execute("SELECT count(*) FROM features.renewal_snapshot"
                       ).fetchone()[0]


def cohort(con, mark: date, cut: date | None = None) -> list[tuple]:
    """The rows of the contracts whose mark is `mark`, built from the
    marts as they are, or as they were on the morning of `cut`."""
    end = mark.fromordinal(mark.toordinal() + MARK_DAYS)
    sql = f"SELECT * FROM ({snapshot_sql(cut)}) WHERE as_of = ?"
    return con.execute(sql, [end, mark, TODAY, mark]).fetchall()


def check(con, cohorts: int = 3) -> list[str]:
    """Everything that must be true of features.renewal_snapshot."""
    problems = []
    n, ids, gap, early, label = con.execute(f"""
        SELECT count(*), count(DISTINCT contract_id),
               count(*) FILTER (date_diff('day', as_of, end_date)
                                <> {MARK_DAYS}),
               count(*) FILTER (days_since_order < 1),
               count(*) FILTER ((not_renewed IS NULL)
                                <> (end_date >= ?))
        FROM features.renewal_snapshot""", [TODAY]).fetchone()
    if n == 0:
        problems.append("the table is empty")
    if ids != n:
        problems.append(f"{n - ids:,} contracts appear more than once")
    if gap:
        problems.append(f"{gap:,} rows not {MARK_DAYS} days before "
                        "their end")
    if early:
        problems.append(f"{early:,} rows count an order on the mark")
    if label:
        problems.append(f"{label:,} labels known before the end")
    marks = [m for (m,) in con.execute("""
        SELECT DISTINCT as_of FROM features.renewal_snapshot
        ORDER BY as_of DESC LIMIT ?""", [cohorts]).fetchall()]
    for mark in marks:
        if cohort(con, mark) != cohort(con, mark, cut=mark):
            problems.append(f"the cohort of {mark} changes when the "
                            "warehouse is cut at its mark")
    return problems
