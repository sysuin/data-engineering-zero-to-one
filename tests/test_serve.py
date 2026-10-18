"""Chapter 22's serving layer over a small hand-made warehouse: the
semantic layer, the metrics API, the point-in-time feature table, the
description Clarity reads, and the reverse-ETL export."""
from __future__ import annotations

import asyncio
from datetime import date, datetime
from pathlib import Path

import duckdb
import httpx
import pytest

from groundwork import ddl
from groundwork.serve import clarity, features, metrics, reverse_etl
from groundwork.serve.api import create_app
from groundwork.stream import sessions

NOW = datetime(2026, 1, 5, 6, 0)
U = "9999-12-31"


def build(path) -> None:
    """Two accounts, a handful of orders, two contracts, three
    sessions (one a bot) and the loads behind them."""
    con = duckdb.connect(str(path))
    ddl.create(con, "marts")
    con.execute(Path(sessions.__file__).with_suffix(".sql").read_text())
    con.executemany("INSERT INTO marts.dim_region VALUES (?,?,?,?,?,?)",
                    [("r1", 1, "Northeast", "NE", "Newark",
                      "America/New_York"),
                     ("r-1", -1, "Unknown", "--", "Unknown", "UTC")])
    con.execute("INSERT INTO marts.dim_supplier VALUES "
                "('s1', 1, 'Voss Industrial', 'US', 'A')")
    con.execute("INSERT INTO marts.dim_product VALUES ('p1', 'SKU-1', "
                "'Mop', 'Cleaning', 's1', 10, 6, '2021-01-01', NULL, "
                "true)")
    acct = """INSERT INTO marts.dim_account VALUES
        (?, ?, ?, 'r1', ?, ?, 'active', false, '2020-01-01', NULL,
         '07001', ?, ?, ?)"""
    con.executemany(acct, [
        ("a10v1", 10, "Ash Ltd", "Small business", "Ann",
         date(2024, 1, 1), date(2025, 6, 1), False),
        ("a10v2", 10, "Ash Ltd", "Small business", "Retention desk",
         date(2025, 6, 1), None, True),
        ("a20v1", 20, "Birch plc", "Enterprise", "Ben",
         date(2024, 1, 1), None, True)])
    orders = [  # id, account_key, day, revenue
        (1, "a10v1", date(2025, 3, 1), 100),
        (2, "a10v1", date(2025, 4, 1), 50),
        (3, "a20v1", date(2025, 4, 2), 200),
        (4, "a10v1", date(2025, 5, 2), 999)]   # on contract 1's mark
    for oid, key, day, rev in orders:
        con.execute("INSERT INTO marts.fct_order_lines VALUES (?, 1, ?,"
                    " ?, ?, 'p1', 's1', 'r1', 'Web', ?, ?, 1, 0, ?, ?,"
                    " ?, ?, ?)", [oid, oid, int(f"{day:%Y%m%d}"), key,
                                  day, day, rev, rev * 0.6, rev,
                                  rev * 0.6, rev * 0.4])
        con.execute("INSERT INTO marts.fct_orders VALUES (?, ?, ?, ?, "
                    "'r1', 'Web', ?, ?, 1, 1, ?, ?, ?)",
                    [oid, oid, int(f"{day:%Y%m%d}"), key, day, day,
                     rev, rev * 0.6, rev * 0.4])
    con.execute("INSERT INTO marts.fct_tickets (ticket_id, account_key, "
                "opened_date_key, opened_at, channel, status, "
                "comment_count) VALUES ('T1', 'a10v1', 20250415, "
                "'2025-04-15 12:00', 'email', 'closed', 0)")
    con.executemany(
        "INSERT INTO marts.fct_contracts VALUES (?, ?, 'r1', 0, 0, "
        "NULL, ?, ?, 12, false, 0, ?, ?)",
        [(1, "a10v1", date(2024, 8, 1), date(2025, 7, 31),
          "not_renewed", "Budget cuts"),
         (2, "a20v1", date(2025, 4, 1), date(2026, 3, 31), None, None)])
    for cid, key in ((1, "a10v1"), (2, "a20v1")):
        for m in (date(2025, 5, 31), date(2025, 6, 30)):
            con.execute("INSERT INTO marts.fct_contract_month VALUES "
                        "(?, ?, ?, 'r1', ?, 1, 1, 0, 1)",
                        [cid, int(f"{m:%Y%m%d}"), key, m])
    con.execute("INSERT INTO marts.fct_contract_month VALUES "
                "(2, 20250731, 'a20v1', 'r1', '2025-07-31', 1, 1, 0, 1)")
    con.executemany(
        "INSERT INTO marts.fct_portal_sessions (session_id, date_key, "
        "region_key, started_at, ended_at, event_count, searches, "
        "product_views, add_to_carts, checkouts_started, "
        "orders_submitted, is_bot) VALUES (?, 20251201, 'r1', "
        "'2025-12-01', '2025-12-01', 1, 0, 0, 0, 0, ?, ?)",
        [("s1", 1, False), ("s2", 0, False), ("s3", 0, True)])
    con.execute("CREATE SCHEMA audit")
    con.execute("CREATE TABLE audit.model_runs (run_id INTEGER)")
    con.execute("CREATE TABLE audit.load_log (load_id VARCHAR, source "
                "VARCHAR, table_name VARCHAR, load_date DATE, status "
                "VARCHAR)")
    for t in ("orders", "order_lines", "accounts", "contracts"):
        src = "tradeline" if "order" in t else "relay"
        con.execute("INSERT INTO audit.load_log VALUES (?, ?, ?, "
                    "'2026-01-04', 'succeeded')", [t, src, t])
    con.close()


@pytest.fixture
def wh(tmp_path):
    path = tmp_path / "warehouse.duckdb"
    build(path)
    return path


# ------------------------------------------------------------ metrics
def test_the_catalogue_reads_and_every_dimension_exists():
    cat = metrics.catalogue()
    assert {"revenue", "orders", "active_accounts", "not_renewed_rate",
            "margin_pct", "portal_conversion"} <= set(cat.metrics)


def test_revenue_by_region_is_the_sum_of_lines(wh):
    con = duckdb.connect(str(wh))
    rows = metrics.query(con, "revenue", by=["region"],
                         start=date(2025, 1, 1), end=date(2026, 1, 1))
    assert rows == [{"region": "Northeast", "revenue": 1349}]


def test_values_are_bound_never_spliced(wh):
    con = duckdb.connect(str(wh))
    assert metrics.query(con, "orders",
                         where={"region": "O'Brien'; DROP"}) \
        == [{"orders": 0}]


def test_a_dimension_the_metric_does_not_allow_is_refused():
    with pytest.raises(metrics.MetricError, match="channel"):
        metrics.compile("active_accounts", by=["channel"])
    with pytest.raises(metrics.MetricError, match="no metric"):
        metrics.compile("active")


def test_a_snapshot_takes_the_period_s_last_month_end(wh):
    con = duckdb.connect(str(wh))
    (year,) = metrics.query(con, "active_accounts", grain="year")
    monthly = metrics.query(con, "active_accounts", grain="month")
    assert year["active_accounts"] == 1               # July: one left
    assert [m["active_accounts"] for m in monthly] == [2, 2, 1]


def test_ratios_and_the_bot_filter(wh):
    con = duckdb.connect(str(wh))
    (nr,) = metrics.query(con, "not_renewed_rate")
    (pc,) = metrics.query(con, "portal_conversion")
    assert nr["not_renewed_rate"] == 100.0       # one ended, not renewed
    assert pc["portal_conversion"] == 50.0       # the bot left out


def test_publish_documents_the_wide_view(wh):
    con = duckdb.connect(str(wh))
    assert metrics.publish(con) == len(metrics.catalogue().metrics)
    docs = dict(con.execute(
        "SELECT column_name, comment FROM duckdb_columns() WHERE "
        "schema_name = 'metrics' AND table_name = 'sales'").fetchall())
    assert set(docs) == set(metrics.SALES_DOCS) and all(docs.values())
    (rev,) = con.execute("SELECT sum(revenue) FROM metrics.sales"
                         ).fetchone()
    assert rev == 1349


# ------------------------------------------------------------ the API
def get(app, url, **params):
    async def call():
        t = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=t,
                                     base_url="http://t") as c:
            return await c.get(url, params=params)
    r = asyncio.run(call())
    return r.status_code, r.json()


def test_the_api_answers_refuses_and_caches(wh):
    app = create_app(wh, now=NOW)
    status, index = get(app, "/metrics")
    assert status == 200 and len(index) == len(metrics.catalogue().metrics)
    status, body = get(app, "/metrics/revenue", by="region")
    assert status == 200 and body["cache"] == "miss"
    assert body["rows"] == [{"region": "Northeast", "revenue": "1349.00"}]
    assert get(app, "/metrics/revenue", by="region")[1]["cache"] == "hit"
    assert app.state.queries == 1
    assert get(app, "/metrics/revenue", by="owner")[0] == 400
    assert get(app, "/metrics/nope")[0] == 404


def test_freshness_follows_the_load_log(wh):
    late = create_app(wh, now=datetime(2026, 1, 6, 7))
    body = get(late, "/metrics/revenue")[1]
    assert body["freshness"]["stale"] is True
    with duckdb.connect(str(wh)) as con:
        for t in ("orders", "order_lines"):
            con.execute("INSERT INTO audit.load_log VALUES (?, "
                        "'tradeline', ?, '2026-01-05', 'succeeded')",
                        [t + "2", t])
    body = get(late, "/metrics/revenue")[1]
    assert body["cache"] == "miss"            # the warehouse changed
    assert body["freshness"]["stale"] is False


# ------------------------------------------------------- the features
def snapshot(con):
    cur = con.execute("SELECT * FROM features.renewal_snapshot "
                      "ORDER BY contract_id")
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def test_features_are_as_of_the_morning_of_the_mark(wh):
    con = duckdb.connect(str(wh))
    assert features.build(con, first_end=date(2025, 1, 1)) == 2
    one, two = snapshot(con)
    assert one["as_of"] == date(2025, 5, 2)
    assert one["orders_90d"] == 2             # not the one on the mark
    assert one["days_since_order"] == 31
    assert one["spend_365"] == 150
    assert one["tickets_90d"] == 1
    assert one["owner"] == "Ann"              # not the Retention desk
    assert one["not_renewed"] is True
    assert two["not_renewed"] is None         # still in force
    assert "cancellation_reason" not in one
    assert features.check(con) == []


def test_check_catches_a_feature_read_from_today(wh, monkeypatch):
    con = duckdb.connect(str(wh))
    leaky = features.SNAPSHOT.replace(
        "AND r.mark - INTERVAL 1 DAY >= a.valid_from",
        "AND a.valid_to IS NULL AND true")
    monkeypatch.setattr(features, "SNAPSHOT", leaky)
    features.build(con, first_end=date(2025, 1, 1))
    assert snapshot(con)[0]["owner"] == "Retention desk"
    assert any("changes" in p for p in features.check(con, cohorts=2))


# ------------------------------------------------------- Clarity, ETL
def test_the_description_names_every_marts_column_and_fits(wh):
    con = duckdb.connect(str(wh))
    text = clarity.describe(con)
    built = set(con.execute(
        "SELECT table_schema || '.' || table_name, column_name FROM "
        "information_schema.columns WHERE table_schema = 'marts'"
    ).fetchall())
    assert clarity.columns_named(text) == built
    assert max(map(len, text.splitlines())) <= 66
    assert "is_bot BOOLEAN (a script, not a person" in " ".join(
        text.split())


def test_reverse_etl_is_idempotent_and_closes_what_left(wh, tmp_path):
    con = duckdb.connect(str(wh), read_only=True)
    out = tmp_path / "outbox"
    first = reverse_etl.export(con, date(2025, 5, 15), out)
    rows = reverse_etl.read(first)
    assert [r["account_id"] for r in rows] == ["10"]  # Ann's account
    assert rows[0]["assign_to"] == "Ann"
    later = reverse_etl.export(con, date(2026, 1, 5), out)
    again = later.read_bytes()
    assert reverse_etl.export(con, date(2026, 1, 5), out).read_bytes() \
        == again
    rows = reverse_etl.read(later)
    assert {(r["account_id"], r["status"]) for r in rows} == {
        ("20", "open"), ("10", "closed")}
    assert rows[1]["external_id"] == reverse_etl.external_id(1)
    assert not list(out.glob("*.tmp"))


def test_the_command_publishes_and_checks(wh, capsys):
    from groundwork.serve.__main__ import main
    assert main(["--warehouse", str(wh)]) == 0
    assert "every promise kept" in capsys.readouterr().out
