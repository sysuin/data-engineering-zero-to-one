"""The Frontline loader, against the desk's API served over a small store."""
from __future__ import annotations

import json
import random
import sqlite3
from datetime import datetime, timedelta

import duckdb
import pyarrow.parquet as pq
import pytest

from groundwork import loadlog
from groundwork.config import SEED
from groundwork.ingest import frontline as fl
from groundwork.storage import raw_path
from meridian.sources import Clock, frontline

TOKEN = "test-token-not-a-secret"
SCHEMA = """
CREATE TABLE tickets (ticket_id TEXT PRIMARY KEY, created_at TEXT NOT NULL,
    requester_account_id INTEGER, requester_name TEXT, requester_email TEXT,
    channel TEXT, language TEXT, description TEXT, sku TEXT);
CREATE TABLE ticket_versions (ticket_id TEXT, updated_at TEXT, status TEXT,
    priority TEXT, category TEXT, desk TEXT, assignee TEXT,
    PRIMARY KEY (ticket_id, updated_at));
CREATE TABLE comments (comment_id INTEGER PRIMARY KEY, ticket_id TEXT,
    created_at TEXT, author TEXT, public INTEGER, body TEXT);
"""


def make_store(path, n=30):
    """n tickets, one opened each hour from 1 June 2025 00:00, each
    worked twice: opened, then solved three hours later."""
    con = sqlite3.connect(path)
    con.executescript(SCHEMA)
    t0 = datetime(2025, 6, 1)
    for i in range(1, n + 1):
        tid = f"FT-{i:06d}"
        opened = t0 + timedelta(hours=i - 1)
        lang = "fr" if i % 5 == 0 else "en"
        sku = f"MRD-FAC-{i:03d}" if i % 3 == 0 else None
        con.execute("INSERT INTO tickets VALUES (?,?,?,?,?,?,?,?,?)",
                    (tid, f"{opened:%Y-%m-%d %H:%M:%S}", 100 + i,
                     f"Person {i}", f"p{i}@example.test", "email", lang,
                     f"ticket {i}", sku))
        for hours, status in ((0, "open"), (3, "solved")):
            at = opened + timedelta(hours=hours)
            con.execute("INSERT INTO ticket_versions VALUES (?,?,?,?,?,?,?)",
                        (tid, f"{at:%Y-%m-%d %H:%M:%S}", status, "low",
                         "delivery", "north_desk", "Priya N"))
    con.commit()
    con.close()
    return path


@pytest.fixture
def env(tmp_path, monkeypatch):
    monkeypatch.setenv("FRONTLINE_TOKEN", TOKEN)
    monkeypatch.setattr(frontline, "TOKEN", TOKEN)
    return {"store": make_store(tmp_path / "store.db"),
            "root": tmp_path / "raw",
            "warehouse": tmp_path / "warehouse.duckdb"}


def run(env, day, at, *, waits=None, secret=None, **faults):
    """One nightly load against the store, as it stood at `at`."""
    base = frontline.serve(Clock(at), store=env["store"], **faults)
    client = fl.Client(base, secret=secret,
                       sleep=(waits.append if waits is not None
                              else lambda s: None))
    return fl.load(day, run_at=datetime.fromisoformat(at),
                   client=client, root=env["root"],
                   warehouse=env["warehouse"])


def test_first_load_takes_everything_and_reconciles(env):
    x = run(env, "2025-06-02", "2025-06-03 06:00")
    assert x.status == "succeeded" and x.reconciled
    assert x.rows_written == 30
    assert x.extra["since"] == fl.BEGINNING
    assert x.extra["watermark"] == "2025-06-02T08:00:00Z"
    assert x.extra["ticket_fields"] == 60
    assert x.extra["ticket_tags"] == 30 + 6 + 10     # category, fr, sku


def test_pages_land_first_and_parse_back_to_the_partition(env):
    run(env, "2025-06-02", "2025-06-03 06:00")
    pages = sorted((env["root"] / "frontline" / "pages").rglob("*.json"))
    assert [p.name for p in pages] == ["page-00001.json"]
    assert "attempt-1" in str(pages[0])
    tables, read = fl.parse(pages)
    landed = pq.read_table(raw_path("frontline", "tickets", "2025-06-02",
                                    env["root"]))
    assert read == 30
    assert tables["tickets"].column("ticket_id").to_pylist() == \
        landed.column("ticket_id").to_pylist()
    assert set(landed.column("_source_file").to_pylist()) == \
        {"frontline/pages/load_date=2025-06-02/attempt-1"}


def test_next_night_starts_at_the_watermark_less_the_overlap(env):
    run(env, "2025-06-01", "2025-06-01 12:00")
    x = run(env, "2025-06-02", "2025-06-03 06:00")
    assert x.extra["since"] == "2025-06-01T11:00:00Z"   # 12:00 less 1 h
    # Tickets 12 to 30 were opened at 11:00 or later; 9, 10 and 11
    # were solved then or later.
    assert x.rows_written == 22


def test_a_rerun_starts_where_the_first_attempt_did(env):
    run(env, "2025-06-01", "2025-06-01 12:00")
    first = run(env, "2025-06-02", "2025-06-03 06:00")
    again = run(env, "2025-06-02", "2025-06-03 07:00")
    assert again.load_id.endswith(".2")
    assert again.extra["since"] == first.extra["since"]
    assert again.rows_written == first.rows_written
    landed = pq.read_table(raw_path("frontline", "tickets", "2025-06-02",
                                    env["root"]))
    assert landed.num_rows == first.rows_written       # replaced, not added


def test_nothing_changed_keeps_the_watermark(env):
    run(env, "2025-06-02", "2025-06-03 06:00")
    x = run(env, "2025-06-03", "2025-06-04 06:00")
    assert x.rows_written == 2      # only the overlap, fetched again
    assert x.extra["watermark"] == "2025-06-02T08:00:00Z"


def test_429_waits_as_told_and_503_backs_off(env):
    waits = []
    x = run(env, "2025-06-02", "2025-06-03 06:00", waits=waits,
            rate_limit=2, fail_every=3)
    assert x.status == "succeeded" and x.rows_written == 30
    assert x.extra["retries"] == len(waits) >= 2
    assert 1.0 in waits                               # Retry-After: 1
    assert all(0 <= w <= 30 for w in waits)


def test_gives_up_after_its_attempts_and_logs_why(env):
    with pytest.raises(fl.FrontlineError, match="gave up after 6"):
        run(env, "2025-06-02", "2025-06-03 06:00", fail_every=1)
    con = duckdb.connect(str(env["warehouse"]))
    (x,) = loadlog.history(con, "frontline", "tickets")
    assert x.status == "failed" and "503" in x.extra["error"]
    assert not raw_path("frontline", "tickets", "2025-06-02",
                        env["root"]).exists()


def test_stops_at_once_on_a_bad_token(env):
    with pytest.raises(fl.FrontlineError) as e:
        run(env, "2025-06-02", "2025-06-03 06:00", secret="wrong")
    assert e.value.status == 401
    assert len(frontline.requests_made()) == 1


def test_the_token_is_never_written_down(env):
    run(env, "2025-06-02", "2025-06-03 06:00")
    con = duckdb.connect(str(env["warehouse"]))
    (x,) = loadlog.history(con, "frontline", "tickets")
    assert x.extra["token"] == fl.fingerprint(TOKEN)
    written = [p.read_bytes() for p in env["root"].rglob("*") if p.is_file()]
    written.append(env["warehouse"].read_bytes())
    assert not any(TOKEN.encode() in b for b in written)


def test_no_token_is_a_clear_error(monkeypatch):
    monkeypatch.delenv("FRONTLINE_TOKEN", raising=False)
    with pytest.raises(fl.FrontlineError, match="FRONTLINE_TOKEN"):
        fl.token()


def test_jitter_repeats_with_the_seed_and_stays_under_the_cap(env):
    a = fl.Client("http://127.0.0.1:9", rng=random.Random(SEED))
    b = fl.Client("http://127.0.0.1:9", rng=random.Random(SEED))
    assert [a.wait(n, None) for n in range(1, 10)] == \
        [b.wait(n, None) for n in range(1, 10)]
    c = fl.Client("http://127.0.0.1:9", cap=2.0)
    assert all(c.wait(n, None) <= min(2.0, 0.5 * 2 ** (n - 1))
               for n in range(1, 12))


def test_a_new_custom_field_or_key_does_not_break_flatten():
    t = {"id": "FT-1", "created_at": "c", "updated_at": "u",
         "custom_fields": [{"id": 1, "name": "desk", "value": "a"},
                           {"id": 9, "name": "region", "value": 3}],
         "tags": ["x"], "brand_new_key": {"nested": True}}
    row, fields, tags = fl.flatten(t, page=1)
    assert [f["value"] for f in fields] == ["a", "3"]
    assert json.loads(row["payload"])["brand_new_key"] == {"nested": True}
    assert row["requester_account_id"] is None


@pytest.mark.slow
def test_cursor_pull_misses_nothing_while_the_desk_moves(sources):
    since = "2025-11-25T00:00:00Z"
    moving = Clock("2025-12-02 20:00")
    c = fl.Client(frontline.serve(moving, tick=timedelta(minutes=3)))
    got = {}
    for body in fl.pull(c, since, limit=10):
        for t in json.loads(body)["tickets"]:
            got[t["id"]] = max(got.get(t["id"], ""), t["updated_at"])
    still = fl.Client(frontline.serve(Clock(moving.now
                                            - timedelta(minutes=3))))
    truth = {t["id"]: t["updated_at"] for body in fl.pull(still, since)
             for t in json.loads(body)["tickets"]}
    assert got == truth
