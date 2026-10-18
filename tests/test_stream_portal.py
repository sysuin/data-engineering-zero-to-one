"""groundwork.stream.portal: the nightly sessions step over hourly
files, catching up a chunk at a time, and keys from dim_account."""
from __future__ import annotations

import gzip
import json
import os
from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from groundwork.ddl import create
from groundwork.stream import portal

pytestmark = pytest.mark.unit

T0 = datetime(2025, 7, 1, 13, 0)


def event(n, sid, minutes, account, etype="product_view"):
    at = T0 + timedelta(minutes=minutes)
    stamp = at.strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return {"event_id": f"e-{n}", "event_type": etype, "session_id": sid,
            "account_id": account, "device": "desktop", "user_agent": "t",
            "occurred_at": stamp, "received_at": stamp, "properties": {},
            "schema_version": 1}


def landing(tmp_path, hours: int, per_hour: int = 6):
    """`hours` hourly files, each with sessions that start and finish
    inside the hour; arrival is five minutes after the hour ends."""
    n = 0
    for h in range(hours):
        when = T0 + timedelta(hours=h)
        path = (tmp_path / "landing" / "portal" / when.strftime(
            "%Y/%m/%d/%H")).with_suffix(".jsonl.gz")
        path.parent.mkdir(parents=True, exist_ok=True)
        lines = []
        for k in range(per_hour):
            sid = f"s-{h}-{k // 3}"
            account = 8 if h == 0 else 7
            lines.append(json.dumps(event(n, sid, h * 60 + k, account)))
            n += 1
        with gzip.open(path, "wt") as f:
            f.write("\n".join(lines) + "\n")
        arrived = (when + timedelta(minutes=65)).replace(
            tzinfo=timezone.utc).timestamp()
        os.utime(path, (arrived, arrived))
    return tmp_path / "landing"


def warehouse(tmp_path):
    path = tmp_path / "wh" / "warehouse.duckdb"
    path.parent.mkdir()
    con = duckdb.connect(str(path))
    create(con, "marts")
    con.execute("""INSERT INTO marts.dim_region VALUES
        ('r1', 1, 'Northeast', 'NE', 'Newark', 'America/New_York')""")
    cols = ("account_key, account_id, account_name, region_key, segment,"
            " owner, status, key_account, customer_since, closed_on,"
            " postcode, valid_from, valid_to, is_current")
    con.execute(f"""INSERT INTO marts.dim_account ({cols}) VALUES
        ('k-unknown', -1, 'Unknown', 'r1', 'Unknown', 'Unknown',
         'unknown', false, DATE '2021-01-01', NULL, '00000',
         DATE '2021-01-01', NULL, true),
        ('k7', 7, 'Seven', 'r1', 'Retail', 'A', 'active', false,
         DATE '2020-01-01', NULL, '07001', DATE '2022-01-01', NULL, true),
        ('k8', 8, 'Eight', 'r1', 'Retail', 'B', 'active', false,
         DATE '2025-08-01', NULL, '07002', DATE '2025-08-01', NULL, true)
    """)
    con.close()
    return path


def rows(path):
    con = duckdb.connect(str(path), read_only=True)
    try:
        return con.execute("SELECT * FROM marts.fct_portal_sessions "
                           "ORDER BY session_id").fetchall()
    finally:
        con.close()


def test_a_first_night_catches_up_and_fills_keys(tmp_path):
    land, wh = landing(tmp_path, 6), warehouse(tmp_path)
    night = portal.run(wh, land, by=T0 + timedelta(days=1))
    assert (night.files, night.records, night.read) == (6, 36, 36)
    assert night.keyless == 0 and night.lag == 0
    got = {r[0]: r[1] for r in rows(wh)}
    # Account 8 was not a customer until August: the unknown member.
    assert night.early == sum(k == "k-unknown" for k in got.values()) > 0
    assert {k for k in got.values()} <= {"k7", "k-unknown"}
    # The last hour's sessions are still open: no event after them.
    assert night.open == 2 and night.rows == len(got) == 10


def test_a_second_night_reads_nothing_twice(tmp_path):
    land, wh = landing(tmp_path, 6), warehouse(tmp_path)
    portal.run(wh, land, by=T0 + timedelta(days=1))
    again = portal.run(wh, land, by=T0 + timedelta(days=1))
    assert (again.files, again.records, again.read, again.written) \
        == (0, 0, 0, 0)


def test_a_chunk_at_a_time_writes_what_one_read_writes(tmp_path,
                                                       monkeypatch):
    monkeypatch.setattr(portal, "BATCH", 4)     # batches inside a chunk
    one, many = tmp_path / "one", tmp_path / "many"
    for folder, chunk in ((one, 1000), (many, 2)):
        folder.mkdir()
        land, wh = landing(folder, 9), warehouse(folder)
        portal.run(wh, land, by=T0 + timedelta(days=1), chunk=chunk)
    assert rows(one / "wh" / "warehouse.duckdb") \
        == rows(many / "wh" / "warehouse.duckdb")


def test_only_what_has_arrived_is_read(tmp_path):
    land, wh = landing(tmp_path, 6), warehouse(tmp_path)
    night = portal.run(wh, land, by=T0 + timedelta(hours=3))
    assert night.files == 2           # hours 0 and 1 have arrived
