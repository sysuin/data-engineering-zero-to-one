"""Chapter 24's governance modules, on a small Groundwork built in tmp:
classification, masking, roles, retention, forgetting and the audit
log."""
from __future__ import annotations

import gzip
import json
from datetime import date, datetime

import duckdb
import pytest

from groundwork import storage
from groundwork.govern import audit, classify, forget, masking, retention
from groundwork.govern import roles

KEY = b"test key, not a secret"
AT = datetime(2026, 1, 5, 6, 0)
KEVIN = "kevin.anderson@bexham-estates.example"


# ------------------------------------------------------------ classify
def test_names_decide_first():
    assert classify.by_name("contact_email")[0] == classify.PERSONAL
    assert classify.by_name("owner")[0] == classify.PERSONAL
    assert classify.by_name("revenue")[0] == classify.CONFIDENTIAL
    assert classify.by_name("order_id")[0] == classify.PUBLIC
    assert classify.by_name("list_price_cents")[0] == classify.PUBLIC
    assert classify.by_name("contact_email_ref")[0] == classify.PERSONAL
    assert classify.by_name("mystery") == (classify.CONFIDENTIAL,
                                           classify.UNREVIEWED)


def test_find_emails_and_phones_but_not_order_numbers():
    text = "Call (214) 555-0152 or a.b@c-d.example about order 1000195"
    assert classify.find(text) == [("email", "a.b@c-d.example"),
                                   ("phone", "(214) 555-0152")]
    assert classify.find("where is order 1000195") == []
    assert classify.find("where is order 1000195",
                         phone=classify.LOOSE_PHONE)


def test_content_makes_a_column_personal():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA raw")
    con.execute("CREATE TABLE raw.t (id INTEGER, notes VARCHAR)")
    con.execute("INSERT INTO raw.t VALUES (1, 'fine'), "
                f"(2, 'mail me at {KEVIN}')")
    cols = {c.column: c for c in classify.classify(con, ("raw",))}
    assert cols["id"].cls == classify.PUBLIC
    assert cols["notes"].cls == classify.PERSONAL
    assert cols["notes"].hits == 1
    assert classify.save(con, list(cols.values())) == 2
    assert classify.personal(con) == {"raw.t": ["notes"]}


# ------------------------------------------------------------- masking
def test_redact_and_mask():
    assert (masking.redact(f"ring (214) 555-0152 or {KEVIN}.")
            == "ring [phone] or [email].")
    assert masking.mask_email(KEVIN) == "k***@bexham-estates.example"
    assert masking.mask_phone("(214) 555-0152") == "(***) ***-0152"


def test_pseudonyms_join_and_need_the_key(monkeypatch):
    a = masking.pseudonym(KEVIN, KEY)
    assert a == masking.pseudonym("  " + KEVIN.upper(), KEY)
    assert a != masking.pseudonym(KEVIN, b"another key")
    assert len(a) == 16 and masking.pseudonym("", KEY) is None
    monkeypatch.delenv(masking.KEY_VAR, raising=False)
    with pytest.raises(RuntimeError) as e:
        masking.pseudonym(KEVIN)
    assert "GROUNDWORK_PSEUDONYM_KEY" in str(e.value)


def test_a_plain_hash_is_undone_by_a_list():
    hashed = masking.plain_hash(KEVIN)
    guesses = {masking.plain_hash(g): g for g in ["x@y.example", KEVIN]}
    assert guesses[hashed] == KEVIN
    assert masking.salted_hash(KEVIN) != masking.salted_hash(KEVIN)


def test_vault_reveals_and_forgets():
    v = masking.Vault()
    t = v.token(KEVIN)
    assert t == v.token(KEVIN.upper()) == "tok_000001"
    assert v.reveal(t) == KEVIN
    assert v.forget(KEVIN) and not v.forget(KEVIN)
    with pytest.raises(KeyError):
        v.reveal(t)


def _staging(con):
    con.execute("CREATE SCHEMA staging")
    con.execute("""CREATE TABLE staging.stg_relay__accounts AS SELECT
        1 AS account_id, 'Kevin Anderson' AS contact_name,
        '(214) 555-0152' AS contact_phone, ? AS contact_email,
        'Sara Quell' AS owner_name""", [KEVIN])
    con.execute("""CREATE TABLE staging.stg_frontline__tickets AS SELECT
        'FT-1' AS ticket_id, 'Kevin Anderson' AS requester_name,
        ? AS requester_email""", [KEVIN])
    con.execute("""CREATE TABLE staging.stg_frontline__ticket_comments
        AS SELECT 1 AS comment_id, 'FT-1' AS ticket_id,
        'Kevin Anderson' AS author,
        'ring me on (214) 555-0152' AS body""")


def test_masked_tables_hold_no_contact_details():
    con = duckdb.connect()
    _staging(con)
    rows = masking.build(con, KEY)
    assert rows == {name: 1 for name in masking.MASKED}
    cols = classify.classify(con, ("masked",))
    assert sum(c.hits for c in cols) == 0
    left = {c.column for c in cols if c.cls == classify.PERSONAL}
    assert left == {"contact_email_ref", "requester_email_ref",
                    "owner_name"}
    (a, t) = con.execute("""SELECT a.contact_email_ref, t.requester_email_ref
        FROM masked.stg_relay__accounts a, masked.stg_frontline__tickets t
        """).fetchone()
    assert a == t == masking.pseudonym(KEVIN, KEY)
    (body,) = con.execute("SELECT body_redacted FROM "
                          "masked.stg_frontline__ticket_comments").fetchone()
    assert body == "ring me on [phone]"
    assert "test key" not in " ".join(
        s for (s,) in con.execute("SELECT sql FROM duckdb_tables()"
                                  ).fetchall())


# --------------------------------------------------------------- roles
def _marts(con):
    con.execute("CREATE SCHEMA marts")
    con.execute("""CREATE TABLE marts.dim_region AS SELECT * FROM (VALUES
        ('k1', 'Northeast'), ('k5', 'Southwest')) v(region_key, region_name)
        """)
    con.execute("""CREATE TABLE marts.fct_orders AS SELECT * FROM (VALUES
        (1, 'k1', 10.0), (2, 'k5', 20.0), (3, 'k5', 5.0))
        v(order_id, region_key, revenue)""")
    con.execute("""CREATE TABLE marts.fct_portal_sessions AS SELECT
        's-1' AS session_id, 'k5' AS region_key, 3 AS event_count""")


TEST_ROLES = {
    "analyst": roles.Role("x", {"marts": ("*",), "masked": ("*",)},
                          frozenset({"owner_name", "contact_email_ref",
                                     "requester_email_ref"}),
                          hide={"fct_portal_sessions": ("session_id",)}),
    "manager_southwest": roles.Role("y", {"marts": ("fct_orders",
                                                    "dim_region")},
                                    region="Southwest"),
}


def test_roles_are_schemas_of_views_and_checked(tmp_path):
    con = duckdb.connect()
    _staging(con)
    _marts(con)
    masking.build(con, KEY)
    made = roles.create(con, TEST_ROLES)
    assert made == {"analyst": 6, "manager_southwest": 2}
    assert roles.check(con, TEST_ROLES) == []
    assert con.execute("SELECT count(*) FROM role_manager_southwest"
                       ".fct_orders").fetchone() == (2,)
    cols = [d[0] for d in con.execute("SELECT * FROM role_analyst"
                                      ".fct_portal_sessions").description]
    assert "session_id" not in cols
    con.execute("""CREATE OR REPLACE VIEW role_analyst.leak AS
        SELECT requester_email FROM staging.stg_frontline__tickets""")
    problems = roles.check(con, TEST_ROLES)
    assert len(problems) == 1 and "requester_email" in problems[0]
    roles.create(con, TEST_ROLES)
    rows = roles.export(con, "analyst", tmp_path / "a.duckdb")
    assert rows["fct_orders"] == 3
    assert "main.leak" not in roles.tables(tmp_path / "a.duckdb")


# ------------------------------------------------- a small Groundwork
@pytest.fixture
def small(tmp_path):
    """A tiny Groundwork: raw files, landed exports, pages, a warehouse."""
    raw, landed = tmp_path / "raw", tmp_path / "landed"
    accounts = [
        {"account_id": "7", "contact_name": "Kevin Anderson",
         "contact_email": KEVIN, "contact_phone": "(214) 555-0152"},
        {"account_id": "8", "contact_name": "Kevin Anderson",
         "contact_email": "kevin.anderson@other.example",
         "contact_phone": "(214) 555-0152"},
    ]
    legacy = [{"account_id": "90007", "contact_name": "Anderson, Kevin",
               "contact_email": "", "contact_phone": "(214) 555-0152"}]
    for day, rows in (("2024-02-29", accounts + legacy),
                      ("2026-01-04", accounts)):
        storage.write_partition(rows, "relay", "accounts", day,
                                loaded_at=AT, load_id=f"relay.{day}.1",
                                root=raw)
    tickets = [
        {"ticket_id": "FT-1", "requester_account_id": 90007,
         "requester_name": "Kevin Anderson", "requester_email": KEVIN,
         "payload": json.dumps({"id": "FT-1", "requester": {
             "account_id": 90007, "name": "Kevin Anderson",
             "email": KEVIN}})},
        {"ticket_id": "FT-2", "requester_account_id": 8,
         "requester_name": "Kevin Anderson",
         "requester_email": "kevin.anderson@other.example",
         "payload": "{}"},
    ]
    storage.write_partition(tickets, "frontline", "tickets", "2026-01-04",
                            loaded_at=AT, load_id="frontline.t.1",
                            root=raw)
    storage.write_partition(
        [{"comment_id": 1, "ticket_id": "FT-1", "author": "Kevin Anderson",
          "body": f"mail me at {KEVIN}"},
         {"comment_id": 2, "ticket_id": "FT-2", "author": "Kevin Anderson",
          "body": "call (214) 555-0152"}],
        "frontline", "ticket_comments", "2026-01-04", loaded_at=AT,
        load_id="frontline.c.1", root=raw)
    for day in ("2025-09-01", "2025-12-01"):
        storage.write_partition(
            [{"event_id": f"e-{day}", "account_id": 7,
              "user_id": "u-000007-1", "session_id": "s-1"},
             {"event_id": f"f-{day}", "account_id": 8,
              "user_id": "u-000008-1", "session_id": "s-2"}],
            "portal", "events", day, loaded_at=AT,
            load_id=f"portal.{day}.1", root=raw)
    pages = raw / "frontline" / "pages" / "load_date=2026-01-04"
    for attempt in ("attempt-1", "attempt-2"):
        (pages / attempt).mkdir(parents=True)
        (pages / attempt / "page-00001.json").write_text(json.dumps(
            {"tickets": [json.loads(t["payload"]) | {"requester": {
                "account_id": t["requester_account_id"],
                "name": t["requester_name"],
                "email": t["requester_email"]}} for t in tickets]},
            ensure_ascii=False))
    text = ("﻿account_id,contact_name,contact_email,contact_phone\r\n"
            f"7,Kevin Anderson,{KEVIN},(214) 555-0152\r\n"
            "8,Kevin Anderson,kevin.anderson@other.example,"
            "(214) 555-0152\r\n")
    sha = "ab" + "0" * 62
    (landed / "relay" / "ab").mkdir(parents=True)
    (landed / "relay" / "ab" / f"{sha}.gz").write_bytes(
        gzip.compress(text.encode("utf-8"), mtime=0))
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    storage.raw_views(con, root=raw)
    con.execute("CREATE SCHEMA intermediate; CREATE SCHEMA audit")
    con.execute("""CREATE TABLE intermediate.int_account_identity AS
        SELECT * FROM (VALUES (7, 7), (90007, 7), (8, 8))
        v(crm_id, account_id)""")
    con.execute("""CREATE TABLE audit.file_ledger AS SELECT
        'relay' AS source, 'accounts' AS table_name, ? AS sha256,
        'relay/2026-01-04/accounts.csv' AS name,
        TIMESTAMP '2026-01-05 04:00' AS arrived_at""", [sha])
    return con, raw, landed


def test_forget_finds_erases_and_proves(small):
    con, raw, landed = small
    s = forget.subject(con, 7)
    assert s.crm_ids == [7, 90007] and s.user_ids == ["u-000007-1"]
    assert s.names == ["Kevin Anderson", "Anderson, Kevin"]
    found = {p.location: p.rows for p in forget.find(con, s, raw, landed)}
    assert found["raw.relay_accounts"] == 3          # the old id too
    assert found["raw.frontline_tickets"] == 1
    assert found["raw.frontline_ticket_comments"] == 1
    assert found["raw.portal_events"] == 2
    assert found["landed/relay (account exports)"] == 1
    assert len([k for k in found if k.startswith("raw pages")]) == 2
    forget.erase(con, s, raw, landed)
    assert forget.find(con, s, raw, landed) == []
    left = forget.sweep(con, s, raw, landed)
    kinds = {k for v in left.values() for k in v}
    assert kinds <= {"phone", "name"}                # the other Kevin
    (n,) = con.execute("SELECT count(*) FROM raw.relay_accounts WHERE"
                       " contact_email LIKE 'kevin.anderson@other%'"
                       ).fetchone()
    assert n == 2                                    # untouched
    (payload,) = con.execute("SELECT payload FROM raw.frontline_tickets"
                             " WHERE ticket_id = 'FT-1'").fetchone()
    assert json.loads(payload)["requester"]["email"] == forget.ERASED


def test_the_audit_keeps_hashes_and_a_restore_is_caught(small, tmp_path):
    con, raw, landed = small
    part = storage.raw_path("relay", "accounts", "2026-01-04", raw)
    backup = tmp_path / "backup.parquet"
    backup.write_bytes(part.read_bytes())
    s = forget.subject(con, 7)
    done = forget.erase(con, s, raw, landed)
    forget.record(con, "FR-1", s, AT, AT, done, {}, "", secret=KEY)
    dump = " ".join(str(v) for t in ("forget_requests", "forget_actions",
                                     "forget_suppress")
                    for row in con.execute(f"SELECT * FROM audit.{t}")
                    .fetchall() for v in row)
    assert KEVIN not in dump and "Kevin" not in dump
    assert forget.resurfaced(con, KEY)["raw.relay_accounts"] == 0
    part.write_bytes(backup.read_bytes())            # a restore
    assert forget.resurfaced(con, KEY)["raw.relay_accounts"] == 1
    forget.reapply(con, raw, landed, KEY)
    assert forget.resurfaced(con, KEY)["raw.relay_accounts"] == 0


def test_retention_deletes_what_is_due_once(small):
    con, raw, landed = small
    plan = retention.run(con, raw, landed, date(2026, 1, 5), dry_run=True)
    gone = {a.target for a in plan if a.action == "deleted"}
    assert gone == {"portal/events/load_date=2025-09-01",
                    "frontline/pages/load_date=2026-01-04/attempt-2"}
    assert (raw / "portal/events/load_date=2025-09-01").exists()
    policies = (retention.POLICIES[0],)
    retention.run(con, raw, landed, date(2026, 1, 5), policies=policies)
    assert not (raw / "portal/events/load_date=2025-09-01").exists()
    assert (raw / "portal/events/load_date=2025-12-01").exists()
    assert retention.run(con, raw, landed, date(2026, 1, 5),
                         dry_run=True, policies=policies) == []


def test_relay_exemptions_are_kept_with_their_reason(small):
    con, raw, landed = small
    con.execute("UPDATE audit.file_ledger SET name ="
                " 'relay/2024-02-29/accounts.csv',"
                " arrived_at = TIMESTAMP '2024-03-01 04:00'")
    (a,) = retention.due(retention.POLICIES[2], raw, landed, con,
                         date(2026, 1, 5))
    assert a.action == "exempt" and "212" in a.note
    con.execute("UPDATE audit.file_ledger SET name = 'relay/x.csv'")
    (a,) = retention.due(retention.POLICIES[2], raw, landed, con,
                         date(2026, 1, 5))
    assert a.action == "deleted"


def test_pages_policy_keeps_the_attempt_raw_was_parsed_from(small):
    con, raw, landed = small
    con.execute("CREATE OR REPLACE VIEW raw.frontline_tickets AS SELECT"
                " 'frontline.tickets.2026-01-04.2' AS _load_id")
    plan = retention.due(retention.POLICIES[1], raw, landed, con,
                         date(2026, 1, 5))
    assert [a.target for a in plan] == [
        "frontline/pages/load_date=2026-01-04/attempt-1"]


# --------------------------------------------------------------- audit
def test_sessions_are_logged_refused_and_scrubbed():
    con = duckdb.connect()
    _staging(con)
    _marts(con)
    con.execute("CREATE SCHEMA audit")
    masking.build(con, KEY)
    roles.create(con, TEST_ROLES)
    tomas = audit.Session(con, "tomas", "analyst")
    assert tomas.sql("SELECT count(*) FROM role_analyst.fct_orders",
                     AT) == [(3,)]
    with pytest.raises(audit.Denied):
        tomas.sql("SELECT * FROM staging.stg_frontline__tickets "
                  f"WHERE requester_email = '{KEVIN}'", AT)
    eng = audit.Session(con, "sam", "engineer")
    eng.sql("SELECT * FROM staging.stg_frontline__tickets", AT)
    log = con.execute("SELECT allowed, break_glass, rows, query FROM"
                      " audit.query_log ORDER BY query_no").fetchall()
    assert [r[:3] for r in log] == [(True, False, 1), (False, False, None),
                                    (True, True, 1)]
    assert KEVIN not in log[1][3] and "'?'" in log[1][3]
    read = audit.who_read(con, {"staging.stg_frontline__tickets"},
                          AT, datetime(2026, 1, 6))
    assert read == [("sam", "engineer", "staging.stg_frontline__tickets",
                     1, 1)]
