"""Contracts: the YAML files, the checks, the guard and the audit trail."""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from groundwork import contracts
from groundwork.contracts import check
from groundwork.ingest import files, price_lists, suppliers

RUN = datetime(2024, 8, 2, 6, 0)
PEM = "suppliers/pemberton-mills/PEMBERTON-PRICES-2024{:02d}.csv"
OLD = b"Item Code,Item,Cost (USD),Valid From\n"
NEW = b"Item Code,Item,Unit Cost USD,Valid From\n"
BODY = b"MRD-CLE-001,Cleaning item 1,58.50,01/{:02d}/2024\n"


def arrival(name):
    return files.Arrival(Path(name), name, 0, RUN)


def pemberton(month, header=OLD, cost=b"58.50"):
    body = BODY.replace(b"58.50", cost).replace(
        b"{:02d}", f"{month:02d}".encode())
    return header + body


@pytest.fixture
def c():
    return contracts.load("suppliers.price_lists")


def test_every_contract_parses_and_names_a_producer_and_owner():
    names = contracts.names()
    assert len(names) == 17
    for n in names:
        k = contracts.load(n)
        assert k.table == n and k.producer.get("who") and k.owner
        assert k.bad_rows in ("quarantine", "fail")
        for col in k.columns.values():
            assert col.type in contracts.TYPES


@pytest.mark.parametrize("header, kind", [
    (["a", "b", "c"], "same"), (["b", "a", "c"], "reordered"),
    (["a", "b", "c", "d"], "added"), (["a", "c"], "removed"),
    (["a", "x", "c"], "renamed"), (["a", "x"], "changed")])
def test_drift_names_each_kind_of_header_change(header, kind):
    assert check.drift(["a", "b", "c"], header)[0] == kind


def test_a_rename_is_breaking_and_an_addition_is_not(c):
    hdr = ["Item Code", "Item", "Price", "Valid From"]
    fmt, breaking, _ = check.schema(c, hdr)
    assert fmt == "pemberton" and "renamed?" in breaking[0]
    hdr = ["Item Code", "Item", "Cost (USD)", "Valid From", "Pack"]
    fmt, breaking, notes = check.schema(c, hdr)
    assert not breaking and "added" in notes[0]


def test_a_reader_and_a_header_that_disagree_break(c):
    _, breaking, _ = check.schema(
        c, NEW.decode().strip().split(","), note="pemberton")
    assert "read as pemberton" in breaking[0]


def test_row_rules_by_unit_and_layout(c):
    rows = [dict(supplier="kestrel-supply", sku="MRD-PAC-022",
                 cost="5529", currency=None, effective_from="2025-12-01",
                 cost_unit="cents", layout="kestrel", description=None),
            dict(supplier="kestrel-supply", sku="MRD-PAC-024",
                 cost="55.29", currency=None,
                 effective_from="2025-12-01", cost_unit="cents",
                 layout="kestrel", description=None),
            dict(supplier="voss-industrial", sku="MRD-FAC-034",
                 cost="5529", currency="USD",
                 effective_from="01/12/2025", cost_unit="dollars",
                 layout="standard", description="x"),
            dict(supplier="nobody", sku="mrd-1", cost=None,
                 currency="EUR", effective_from="2025-12-01",
                 cost_unit="dollars", layout="standard",
                 description="x")]
    bad = check.check_rows(c, rows)
    got = {(b.row, b.column, b.rule) for b in bad}
    assert not any(r == 1 for r, _, _ in got)
    assert (2, "cost", "type") in got
    assert {(3, "cost", "range"), (3, "effective_from", "type")} <= got
    assert {(4, "supplier", "allowed"), (4, "sku", "pattern"),
            (4, "cost", "missing"), (4, "currency", "allowed")} <= got


def test_a_key_seen_twice_fails_both_rows():
    k = contracts.load("reference.regions")
    rows = [dict(region_id="1", name="a", depot="b",
                 timezone="America/Chicago")] * 2
    assert {b.rule for b in check.check_rows(k, rows)} == {"key"}


def test_history_catches_a_cost_ten_times_the_last(c):
    h = check.History(c)
    feb = suppliers.read(pemberton(2), arrival(PEM.format(2))).rows
    h.add(feb)
    mar = suppliers.read(pemberton(3, cost=b"585.00"),
                         arrival(PEM.format(3))).rows
    (b,) = h.check(mar)
    assert b.rule == "history" and "10.0 times" in b.reason


def test_most_rows_failing_breaks_the_batch_and_policy_fail_breaks_on_one(c):
    rows = suppliers.read(pemberton(7, NEW), arrival(PEM.format(7))).rows
    res = check.check(c, rows)
    assert res.status == "broken" and "1 of 1 rows fail" in res.breaking[0]
    k = contracts.load("reference.regions")
    rows = [dict(region_id="1", name="a", depot="b", timezone="UTC")]
    res = check.check(k, rows + [dict(rows[0], region_id=str(i))
                                 for i in range(2, 5)])
    assert res.status == "broken" and "policy is to fail" in \
        res.breaking[0]


def test_a_typed_column_that_changed_type_breaks():
    k = contracts.load("tradeline.orders")
    import pyarrow as pa
    t = pa.table({n: pa.array(["1"]) for n in k.columns})
    res = check.check(k, t, types={"id": "double"})
    assert "arrived as double" in res.breaking[0]


def test_the_guard_refuses_a_rename_before_reading_it(c):
    v1 = replace(c, formats={k: v for k, v in c.formats.items()
                             if k != "pemberton-2024"})
    g = check.Guard(suppliers.read, v1)
    with pytest.raises(check.ContractBroken, match="renamed"):
        g(pemberton(7, NEW), arrival(PEM.format(7)))
    assert g.seen[-1][2].status == "broken"


def test_the_guard_parks_bad_rows_and_keeps_the_count_honest(c):
    data = pemberton(6) + b"MRD-FAC-038,Facilities item 9,,01/06/2024\n"
    g = check.Guard(suppliers.read, c)
    p = g(data, arrival(PEM.format(6)))
    assert len(p.rows) == 1 and p.counted == 1
    assert g.seen[-1][2].parked == 1


def test_park_and_record_write_once_however_often_they_run(c, tmp_path):
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    data = pemberton(6) + b"MRD-FAC-038,Facilities item 9,,01/06/2024\n"
    for _ in range(2):
        g = check.Guard(suppliers.read, c)
        g(data, arrival(PEM.format(6)))
        got = g.flush(con, load_id="x.1", at=RUN)
    assert got == {"files": 1, "rows": 2, "parked": 1, "broken": []}
    assert con.execute("SELECT count(*), any_value(reason) FROM "
                       "audit.quarantine").fetchone() == (
        1, "cost: is empty")
    assert con.execute("SELECT status, parked FROM audit.contract_runs"
                       ).fetchone() == ("parked", 1)


def test_the_new_reader_knows_pembertons_new_header(c):
    p = price_lists.read(pemberton(7, NEW), arrival(PEM.format(7)))
    assert p.note == "pemberton-2024"
    assert p.rows[0]["cost"] == "58.50" and p.rows[0]["layout"] == \
        "pemberton"
    p = check.Guard(price_lists.read, c)(pemberton(7, NEW),
                                         arrival(PEM.format(7)))
    assert len(p.rows) == 1


def test_compatible_says_who_a_change_can_break(c):
    cols = dict(c.columns)
    cols["extra"] = contracts.Column("extra", nullable=True)
    cols["currency"] = replace(cols["currency"], allowed=("USD", "CAD"))
    cols.pop("description")
    got = dict((what, who) for who, what in
               contracts.compatible(c, replace(c, columns=cols)))
    assert got == {"column extra added": "nobody",
                   "currency: new values ['CAD']": "consumers",
                   "column description removed": "consumers"}


def test_audit_and_reopen_mend_what_an_older_reader_loaded(c, tmp_path):
    landing = tmp_path / "landing"
    for m, header in ((6, OLD), (7, NEW)):
        f = landing / PEM.format(m)
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(pemberton(m, header))
    kw = dict(root=tmp_path / "raw", archive=tmp_path / "landed")
    con = duckdb.connect(str(tmp_path / "w.duckdb"))
    found = files.discover(landing, "suppliers", by=None)
    day = RUN.date()
    files.load(con, "suppliers", "price_lists", found, day, run_at=RUN,
               read=suppliers.read, folder="suppliers", **kw)
    from groundwork.storage import raw_views
    raw_views(con, kw["root"])
    broken = [ref for ref, _, r in check.audit(
        con, "suppliers.price_lists", archive=kw["archive"])
        if r.breaking]
    assert len(broken) == 1
    assert check.reopen(con, "suppliers", "price_lists", broken,
                        "test") == 1
    files.load(con, "suppliers", "price_lists", found, day, run_at=RUN,
               read=check.Guard(price_lists.read, c), folder="suppliers",
               **kw)
    raw_views(con, kw["root"])
    assert con.execute("SELECT count(*), count(cost) FROM "
                       "raw.suppliers_price_lists").fetchone() == (2, 2)
