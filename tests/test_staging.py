"""Groundwork's staging models, run by the runner over small hand-made raw tables."""
from __future__ import annotations

from datetime import date, datetime

import duckdb
import pytest

from groundwork.transform import runner

AT = datetime(2026, 1, 5, 6, 30)
STAMP = ("file", "file_sha256", "_load_id")


def raw_table(con, name, rows, *, load_date=date(2026, 1, 4)):
    """raw.<name> from dicts: every value text, as raw keeps it."""
    cols = list(rows[0])
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(f"CREATE TABLE raw.{name} ("
                + ", ".join(f'"{c}" VARCHAR' for c in cols)
                + ", load_date DATE)")
    for r in rows:
        con.execute(f"INSERT INTO raw.{name} VALUES ("
                    + ", ".join("?" for _ in cols) + ", ?)",
                    [r[c] for c in cols] + [r.get("_day", load_date)])


def in_force(con, *files):
    """audit.current_files holding (source, name, sha256) for each file."""
    con.execute("CREATE SCHEMA IF NOT EXISTS audit")
    con.execute("CREATE TABLE IF NOT EXISTS audit.current_files "
                "(source VARCHAR, name VARCHAR, sha256 VARCHAR)")
    for f in files:
        con.execute("INSERT INTO audit.current_files VALUES (?, ?, ?)", f)


def build(tmp_path, model, setup):
    wh = tmp_path / "wh.duckdb"
    con = duckdb.connect(str(wh))
    setup(con)
    con.close()
    refs = runner.discover()[model].refs          # seeds it reads
    results = runner.run(wh, run_at=AT, select=[model, *refs])
    got = {r.model: r for r in results}
    assert got[model].status == "built", got[model].error
    return duckdb.connect(str(wh))


ACCOUNT = dict(account_id="2485", account_name="Brennan Supply ",
               region="Northeast", segment="Small business",
               owner="Ada K", customer_since="2022-04-01",
               postcode="01498", key_account="false", status="active",
               closed_on="", contact_name="Zoë Brennan",
               contact_email="zoe@brennan.example",
               contact_phone="(973) 555-0101",
               updated_at="2024-03-01T09:15:00Z", source_format="relay",
               file="relay/2024-03-01/accounts.csv", file_sha256="new",
               _load_id="relay.accounts.2024-03-01.1",
               _day=date(2024, 3, 1))
LEGACY = dict(ACCOUNT, customer_since="04/01/2022", postcode="1498",
              key_account="N", status="Active",
              contact_name="Brennan, Zoë", updated_at=None,
              source_format="legacy", file="relay/2024-02-29/accounts.csv",
              file_sha256="old", _day=date(2024, 2, 29))


def test_relay_accounts_mean_the_same_whichever_crm_wrote_them(tmp_path):
    stale = dict(ACCOUNT, file_sha256="superseded", account_name="Stale")

    def setup(con):
        raw_table(con, "relay_accounts", [LEGACY, ACCOUNT, stale])
        in_force(con, ("relay", LEGACY["file"], "old"),
                 ("relay", ACCOUNT["file"], "new"))
    con = build(tmp_path, "stg_relay__accounts", setup)
    rows = con.execute(
        "SELECT snapshot_date, account_id, account_name, postcode, "
        "customer_since_date, is_key_account, status, closed_date, "
        "contact_name, source_format FROM staging.stg_relay__accounts "
        "ORDER BY snapshot_date").fetchall()
    assert len(rows) == 2                      # the superseded file is out
    old, new = rows
    assert old[1:-1] == new[1:-1]
    assert new[1:-1] == (2485, "Brennan Supply", "01498", date(2022, 4, 1),
                         False, "active", None, "Zoë Brennan")
    (moment,) = con.execute(
        "SELECT updated_at FROM staging.stg_relay__accounts "
        "WHERE source_format = 'relay'").fetchone()
    assert moment == datetime(2024, 3, 1, 9, 15)   # a TIMESTAMP, the Z gone
    types = dict(con.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'stg_relay__accounts'").fetchall())
    assert types["updated_at"] == "TIMESTAMP"
    assert types["customer_since_date"] == "DATE"


def test_relay_contracts_read_both_vocabularies(tmp_path):
    base = dict(contract_id="7", account_id="2485", start_date="2024-01-01",
                end_date="2024-12-31", term_months="12",
                legacy_terms="false", discount_pct="", outcome="not_renewed",
                notice_date="2024-10-02", cancellation_reason="",
                updated_at="2025-01-03T10:00:00Z", source_format="relay",
                file="relay/n/contracts.csv", file_sha256="new",
                _load_id="x")
    old = dict(base, start_date="01/01/2024", end_date="12/31/2024",
               legacy_terms="N", outcome="Not renewed",
               notice_date="10/02/2024", updated_at=None,
               source_format="legacy", file="relay/o/contracts.csv",
               file_sha256="old")

    def setup(con):
        raw_table(con, "relay_contracts", [base, old])
        in_force(con, ("relay", base["file"], "new"),
                 ("relay", old["file"], "old"))
    con = build(tmp_path, "stg_relay__contracts", setup)
    rows = con.execute(
        "SELECT start_date, end_date, is_legacy_terms, discount_pct, "
        "outcome, notice_date, cancellation_reason "
        "FROM staging.stg_relay__contracts").fetchall()
    assert rows[0] == rows[1] == (date(2024, 1, 1), date(2024, 12, 31),
                                  False, None, "not_renewed",
                                  date(2024, 10, 2), None)


def ticket(tid, updated, day, **kw):
    return dict(dict(ticket_id=tid, created_at="2025-11-01T10:00:00Z",
                     updated_at=updated, status="open", priority="low",
                     channel="web", language="en",
                     requester_account_id="5", requester_name="A B",
                     requester_email="a@b.example", assignee="",
                     description="hello", sku=" mrd-cle-001", payload="{}",
                     _page="1", _load_id=f"frontline.{day}",
                     _source_file="pages", _day=day), **kw)


def test_frontline_keeps_each_tickets_latest_version(tmp_path):
    d1, d2 = date(2025, 12, 1), date(2025, 12, 2)
    tickets = [ticket("FT-000001", "2025-12-01T09:00:00Z", d1),
               ticket("FT-000001", "2025-12-02T08:00:00Z", d2,
                      status="solved"),
               ticket("FT-000002", "2025-12-01T09:30:00Z", d1),
               ticket("FT-000002", "2025-12-01T09:30:00Z", d2,
                      priority="high")]          # same moment: later pull
    tags = [dict(ticket_id="FT-000001", updated_at="2025-12-01T09:00:00Z",
                 position="0", tag="old", _page="1", _load_id="a",
                 _day=d1),
            dict(ticket_id="FT-000002", updated_at="2025-12-01T09:30:00Z",
                 position="0", tag="kept", _page="1", _load_id="b",
                 _day=d2)]

    def setup(con):
        raw_table(con, "frontline_tickets", tickets)
        raw_table(con, "frontline_ticket_tags", tags)
    con = build(tmp_path, "stg_frontline__tickets", setup)
    rows = con.execute(
        "SELECT ticket_id, updated_at, status, priority, sku, assignee "
        "FROM staging.stg_frontline__tickets ORDER BY 1").fetchall()
    assert rows == [
        ("FT-000001", datetime(2025, 12, 2, 8, 0), "solved", "low",
         "MRD-CLE-001", None),
        ("FT-000002", datetime(2025, 12, 1, 9, 30), "open", "high",
         "MRD-CLE-001", None)]
    con.close()
    results = runner.run(tmp_path / "wh.duckdb", run_at=AT,
                         select=["stg_frontline__ticket_tags"])
    assert results[0].status == "built", results[0].error
    con = duckdb.connect(str(tmp_path / "wh.duckdb"))
    # FT-000001's latest version has no tags: its old tag must not return
    assert con.execute("SELECT ticket_id, tag FROM "
                       "staging.stg_frontline__ticket_tags").fetchall() == [
        ("FT-000002", "kept")]


def price(supplier, sku, cost, unit, effective, layout, sha):
    return dict(supplier=supplier, sku=sku, description="", cost=cost,
                currency=None, effective_from=effective, cost_unit=unit,
                layout=layout, file=f"suppliers/{supplier}/{sha}",
                file_sha256=sha, _load_id="x")


def test_price_lists_in_cents_with_dates_by_layout(tmp_path):
    rows = [price("kestrel-supply", "mrd-pac-022 ", "5529", "cents",
                  "2024-03-01", "kestrel", "k"),
            price("pemberton-mills", "MRD-CLE-001", "58.5", "dollars",
                  "01/03/2024", "pemberton", "p"),
            price("aldridge-co", "MRD-FAC-030", "79.07", "dollars",
                  "1 March 2024", "aldridge", "a2"),
            price("aldridge-co", "MRD-FAC-030", "790.70", "dollars",
                  "1 March 2024", "aldridge", "a1")]   # superseded

    def setup(con):
        raw_table(con, "suppliers_price_lists", rows)
        in_force(con, *[("suppliers", r["file"], r["file_sha256"])
                        for r in rows[:3]])
    con = build(tmp_path, "stg_suppliers__price_lists", setup)
    got = con.execute(
        "SELECT supplier_id, sku, unit_cost_cents, effective_date, "
        "description FROM staging.stg_suppliers__price_lists "
        "ORDER BY supplier_id").fetchall()
    assert got == [(2, "MRD-CLE-001", 5850, date(2024, 3, 1), None),
                   (3, "MRD-FAC-030", 7907, date(2024, 3, 1), None),
                   (4, "MRD-PAC-022", 5529, date(2024, 3, 1), None)]


def test_catalogue_skus_names_and_prices(tmp_path):
    rows = [{"SKU": "mrd-pac-022", "Product Name": "Packaging item 3",
             "Category": "Packaging", "Supplier": "Kestrel",
             "List Price": "$70.88", "Launched": "", "Notes": "",
             "file": "catalogue/catalogue.csv", "file_sha256": "c",
             "_load_id": "x"},
            {"SKU": "MRD-SAN-043 ", "Product Name": "Sanitation item 5",
             "Category": "Sanitation", "Supplier": "Voss Industrial ",
             "List Price": "$56.53", "Launched": "01-Apr-2024",
             "Notes": "New range", "file": "catalogue/catalogue.csv",
             "file_sha256": "c", "_load_id": "x"}]

    def setup(con):
        raw_table(con, "catalogue_products", rows)
        in_force(con, ("catalogue", "catalogue/catalogue.csv", "c"))
    con = build(tmp_path, "stg_catalogue__products", setup)
    got = con.execute(
        "SELECT sku, supplier_name, supplier_id, list_price_cents, "
        "launched_date, notes FROM staging.stg_catalogue__products "
        "ORDER BY sku").fetchall()
    assert got == [("MRD-PAC-022", "Kestrel", 4, 7088, None, None),
                   ("MRD-SAN-043", "Voss Industrial", 1, 5653,
                    date(2024, 4, 1), "New range")]


def test_seed_has_one_supplier_per_spelling():
    con = duckdb.connect()
    seed = runner.MODELS / "seeds" / "supplier_names.csv"
    rows = con.execute(f"SELECT source, spelling, supplier_id FROM "
                       f"read_csv('{seed}', all_varchar = true)").fetchall()
    keys = [(s, n) for s, n, _ in rows]
    assert len(keys) == len(set(keys))
    assert all(n == n.strip() and i.isdigit() for _, n, i in rows)
    assert (runner.MODELS / "seeds" / "supplier_names.yaml").exists()


@pytest.mark.slow
def test_seed_covers_every_spelling_in_raw(warehouse):
    con = duckdb.connect(str(warehouse), read_only=True)
    seed = runner.MODELS / "seeds" / "supplier_names.csv"
    (missing,) = con.execute(f"""
        SELECT count(*) FROM (
            SELECT 'catalogue' AS source, trim("Supplier") AS spelling
            FROM raw.catalogue_products
            UNION SELECT 'suppliers', trim(supplier)
            FROM raw.suppliers_price_lists)
        ANTI JOIN read_csv('{seed}', all_varchar = true)
        USING (source, spelling)""").fetchone()
    assert missing == 0
