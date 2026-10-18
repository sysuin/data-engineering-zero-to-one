"""groundwork.stream.consumer: committed offsets, at least once, and
a sink that stores each event once."""
from __future__ import annotations

import json
from datetime import datetime

import duckdb

from groundwork.stream.consumer import Consumer, EventSink, archive, parse
from groundwork.stream.log import Log


def event(n, schema=1, received="2025-11-10T15:00:01.000Z", **extra):
    props = ({"sku": "MRD-SAF-011", "qty": 2} if schema == 1 else
             {"product_sku": "MRD-SAF-011", "qty": 2,
              "cart_value_cents": 1234})
    e = {"event_id": f"e-{n}", "event_type": "add_to_cart",
         "schema_version": schema,
         "occurred_at": "2025-11-10T15:00:00.500Z",
         "session_id": "s-1", "account_id": 7, "user_id": "u-000007-1",
         "device": "desktop", "user_agent": "test",
         "properties": props, "received_at": received}
    e.update(extra)
    return json.dumps(e).encode()


def a_log(tmp_path, n=10, dup_every=None):
    log = Log(tmp_path / "log")
    for i in range(n):
        log.append(event(i))
        if dup_every and i % dup_every == 0:
            log.append(event(i, received="2025-11-10T15:03:00.000Z"))
    return log


def test_a_new_group_starts_at_the_start(tmp_path):
    log = a_log(tmp_path)
    c = Consumer(log, "g")
    assert c.position == 0 and c.committed() == 0
    assert [o for o, _ in c.poll(3)] == [0, 1, 2]
    assert c.position == 3


def test_a_restart_resumes_at_the_committed_offset(tmp_path):
    log = a_log(tmp_path)
    c = Consumer(log, "g")
    c.poll(4)
    c.commit()
    c.poll(4)                                  # read, never committed
    again = Consumer(log, "g")
    assert again.position == 4
    assert [o for o, _ in again.poll(2)] == [4, 5]


def test_groups_keep_their_own_offsets(tmp_path):
    log = a_log(tmp_path)
    a, b = Consumer(log, "a"), Consumer(log, "b")
    a.poll(6)
    a.commit()
    b.poll(1)
    b.commit()
    assert (Consumer(log, "a").position, Consumer(log, "b").position) \
        == (6, 1)
    assert a.lag == 4 and b.lag == 9


def test_state_is_saved_with_the_offset(tmp_path):
    log = a_log(tmp_path)
    c = Consumer(log, "g")
    c.poll(5)
    c.commit(state={"seen": 5})
    c.poll(2)
    c.commit()                                 # offset only, no state
    again = Consumer(log, "g")
    assert again.position == 7
    assert again.restore() == {"seen": 5}
    assert again.position == 5                 # the checkpoint's offset


def test_parse_gives_both_schemas_one_shape():
    v1, v2 = parse(event(1)), parse(event(2, schema=2))
    assert v1["sku"] == v2["sku"] == "MRD-SAF-011"
    assert v1["value_cents"] is None and v2["value_cents"] == 1234
    assert v1["occurred_at"] == datetime(2025, 11, 10, 15, 0, 0, 500000)
    assert v1["occurred_at"].tzinfo is None


def test_the_sink_stores_an_event_once():
    con = duckdb.connect()
    sink = EventSink(con)
    first = [parse(event(i)) for i in range(3)]
    assert sink.write(first) == 3
    assert sink.write(first + [parse(event(3))]) == 1
    assert sink.write([]) == 0
    assert con.execute("SELECT count(*) FROM stream.portal_events"
                       ).fetchone() == (4,)


def test_archive_after_a_crash_holds_each_event_once(tmp_path):
    log = a_log(tmp_path, n=50, dup_every=7)   # 50 events, 8 twice
    con = duckdb.connect()
    crashed = archive(log, con, batch=10, stop_after=30)
    assert crashed["committed"] is False
    assert Consumer(log, "archive").position == 20
    rest = archive(log, con, batch=10)
    assert crashed["read"] + rest["read"] == log.end_offset + 10
    assert con.execute("SELECT count(*) FROM stream.portal_events"
                       ).fetchone() == (50,)
