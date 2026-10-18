"""groundwork.stream.sessions: session windows, a watermark, lateness,
clocks, bots, and fct_portal_sessions."""
from __future__ import annotations

import json
from datetime import datetime, timedelta

import duckdb
import pytest

from groundwork.stream import sessions as S
from groundwork.stream.log import Log

T0 = datetime(2025, 12, 9, 15, 0)
REGIONS = {7: 1, 8: 4}                 # Newark and Sacramento


def ev(n, sid="s-1", at=0.0, lag=1.5, etype="product_view", account=7,
       device="desktop"):
    """An event `at` minutes after T0 on the device, received `lag`
    seconds later."""
    occurred = T0 + timedelta(minutes=at)
    return {"event_id": f"e-{n}", "event_type": etype, "session_id": sid,
            "account_id": account, "device": device, "user_agent": "t",
            "occurred_at": occurred,
            "received_at": occurred + timedelta(seconds=lag),
            "sku": None, "value_cents": None, "order_number": None,
            "schema_version": 2}


def feed(s, events):
    rows = []
    for e in events:
        s.add(e)
        rows += s.advance()
    return rows


def test_a_session_closes_only_after_the_gap():
    s = S.Sessioniser(REGIONS)
    assert feed(s, [ev(1, at=0), ev(2, at=5, etype="add_to_cart")]) == []
    # Another session's event at +39 min moves the watermark to +34:
    # not yet 30 minutes past the first session's last event.
    assert feed(s, [ev(3, sid="s-2", at=39)]) == []
    rows = feed(s, [ev(4, sid="s-2", at=41)])
    assert [r["session_id"] for r in rows] == ["s-1"]
    r = rows[0]
    assert (r["event_count"], r["product_views"], r["add_to_carts"]) \
        == (2, 1, 1)
    assert r["started_at"] == T0 and r["late_events"] == 0


def test_duplicates_count_once():
    s = S.Sessioniser(REGIONS)
    assert s.add(ev(1)) == "ok"
    assert s.add(ev(1, lag=90)) == "duplicate"
    rows = s.close_all()
    assert rows[0]["event_count"] == 1
    assert s.counts["duplicates"] == 1


def test_a_late_event_revises_the_row():
    s = S.Sessioniser(REGIONS)
    feed(s, [ev(1, at=0), ev(2, sid="s-2", at=60)])
    late = ev(3, at=2, lag=2 * 3600, etype="add_to_cart")
    assert s.add(late) == "late"
    (row,) = s.advance()
    assert row["event_count"] == 2 and row["late_events"] == 1
    assert row["add_to_carts"] == 1


def test_an_event_later_than_the_lateness_is_left_out():
    s = S.Sessioniser(REGIONS, lateness=timedelta(hours=1))
    feed(s, [ev(1, at=0), ev(2, sid="s-2", at=200)])
    assert s.add(ev(3, at=1, lag=4 * 3600)) == "too_late"
    new_session = ev(4, sid="s-3", at=0, lag=4 * 3600)
    assert s.add(new_session) == "too_late"
    assert s.counts["too_late"] == 2


def test_a_clock_ahead_cannot_drag_the_watermark():
    s = S.Sessioniser(REGIONS)
    feed(s, [ev(1, at=0)])
    ahead = ev(2, sid="s-9", at=24 * 60, lag=-24 * 3600, device="mobile")
    feed(s, [ahead])
    assert s.watermark < T0 + timedelta(minutes=1)
    trusting = S.Sessioniser(REGIONS, clamp=False)
    feed(trusting, [ev(1, at=0), ahead])
    assert trusting.watermark > T0 + timedelta(hours=23)


def test_a_wrong_clock_is_corrected_from_the_quickest_event():
    s = S.Sessioniser(REGIONS)
    skew = -5 * 3600                          # device 5 hours ahead
    events = [ev(1, at=300, lag=skew + 1, device="mobile"),
              ev(2, at=302, lag=skew + 2, device="mobile"),
              ev(3, at=304, lag=skew + 3 * 3600, device="mobile")]
    for e in events:
        s.add(e)
    (row,) = s.close_all()
    assert row["clock_offset_s"] == skew + 1
    assert row["started_at"] == T0 + timedelta(seconds=1)


def test_a_late_event_is_not_mistaken_for_a_wrong_clock():
    s = S.Sessioniser(REGIONS)
    for e in (ev(1, at=0, lag=1), ev(2, at=1, lag=3 * 3600)):
        s.add(e)
    (row,) = s.close_all()
    assert row["clock_offset_s"] == 0 and row["started_at"] == T0


def test_a_bot_is_flagged_by_its_rate():
    s = S.Sessioniser(REGIONS)
    for i in range(150):
        s.add(ev(i, sid="bot", at=i * 0.01))
    for i in range(150, 170):
        s.add(ev(i, sid="person", at=i * 0.5))
    rows = {r["session_id"]: r for r in s.close_all()}
    assert rows["bot"]["is_bot"] and not rows["person"]["is_bot"]


def test_the_date_key_is_the_depots_date():
    s = S.Sessioniser(REGIONS)
    late_evening = T0.replace(hour=4)          # 20:00 in Sacramento
    e = ev(1, account=8)
    e["occurred_at"] = late_evening
    e["received_at"] = late_evening + timedelta(seconds=1)
    s.add(e)
    (row,) = s.close_all()
    assert row["date_key"] == 20251208
    assert row["region_key"] == "a87ff679a2f3e71d"   # md5("4")[:16]


def test_the_sink_replaces_a_revised_row():
    con = duckdb.connect()
    sink = S.SessionSink(con)
    s = S.Sessioniser(REGIONS)
    sink.upsert(feed(s, [ev(1), ev(2, sid="s-2", at=60)]))
    s.add(ev(3, at=1, lag=7200))
    sink.upsert(s.advance())
    assert con.execute("""SELECT event_count, late_events
                          FROM marts.fct_portal_sessions
                          WHERE session_id = 's-1'""").fetchall() \
        == [(2, 1)]
    S.SessionSink(con)                         # its DDL runs twice


def encode(e):
    return json.dumps({
        "event_id": e["event_id"], "event_type": e["event_type"],
        "schema_version": 2, "session_id": e["session_id"],
        "account_id": e["account_id"], "user_id": "u", "device": "desktop",
        "user_agent": "t", "properties": {},
        "occurred_at": e["occurred_at"].isoformat(timespec="milliseconds")
        + "Z",
        "received_at": e["received_at"].isoformat(timespec="milliseconds")
        + "Z"}).encode()


def test_a_run_that_stops_and_resumes_matches_one_that_does_not(tmp_path):
    log = Log(tmp_path / "log")
    for i in range(200):
        sid = f"s-{i // 5}"
        log.append(encode(ev(i, sid=sid, at=i * 3)))
        if i % 17 == 0:
            log.append(encode(ev(i, sid=sid, at=i * 3, lag=60)))
    one, two = duckdb.connect(), duckdb.connect()
    S.run(log, one, REGIONS, group="a", batch=13, until=90)
    S.run(log, one, REGIONS, group="a", batch=13)
    S.run(log, two, REGIONS, group="b", batch=500)
    query = "SELECT * FROM marts.fct_portal_sessions ORDER BY session_id"
    assert one.execute(query).fetchall() == two.execute(query).fetchall()
    assert len(one.execute(query).fetchall()) == 37   # 3 still open


def test_funnel_and_abandoned_carts():
    con = duckdb.connect()
    sink = S.SessionSink(con)
    s = S.Sessioniser(REGIONS)
    for e in (ev(1, sid="a", etype="add_to_cart"),
              ev(2, sid="b", etype="add_to_cart"),
              ev(3, sid="b", at=1, etype="checkout_started"),
              ev(4, sid="b", at=2, etype="order_submitted"),
              ev(5, sid="c", at=3)):
        s.add(e)
    sink.upsert(s.close_all())
    f = S.funnel(con, T0, T0 + timedelta(days=1))
    assert (f["sessions"], f["carted"], f["checked_out"], f["ordered"]) \
        == (3, 2, 1, 1)
    # Session a went quiet at T0; it counts as abandoned at T0 + 30 min.
    assert S.abandoned_carts(con, T0 + timedelta(minutes=45)) == 1
    assert S.abandoned_carts(con, T0 + timedelta(minutes=20)) == 0


@pytest.mark.slow
def test_the_bots_morning_from_the_real_files(sources, tmp_path):
    from groundwork.stream.log import append_files
    day = sorted((sources / "landing" / "portal" / "2025" / "11" / "17")
                 .glob("0[2-5].jsonl.gz"))
    log = Log(tmp_path / "log")
    append_files(log, day)
    con = duckdb.connect()
    S.SessionSink(con).upsert(S.run(log, con, {}).close_all())
    bots = con.execute("""SELECT account_id, event_count
                          FROM marts.fct_portal_sessions WHERE is_bot"""
                       ).fetchall()
    assert bots == [(2040, 4000)]
