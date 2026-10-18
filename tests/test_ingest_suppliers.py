"""Supplier price lists: four layouts to one shape, and the resend."""
from __future__ import annotations

import io
import os
from datetime import datetime
from pathlib import Path

import duckdb
import openpyxl
import pytest

from groundwork.ingest import files, suppliers

RUN = datetime(2024, 3, 5, 6, 0)


def arrival(name):
    return files.Arrival(Path(name), name, 0, RUN)


def sheet(rows, *, merge=None, sheets=1) -> bytes:
    """A spreadsheet laid out as Aldridge lays one out."""
    book = openpyxl.Workbook()
    ws = book.active
    ws.title = "Trade prices"
    for r in rows:
        ws.append(list(r))
    if merge:
        ws.merge_cells(merge)
    for i in range(1, sheets):
        book.create_sheet(f"Sheet{i}")
    out = io.BytesIO()
    book.save(out)
    return out.getvalue()


ALDRIDGE = [("Aldridge & Co — Trade Price List",),
            ("Prices valid from 1 March 2024",), (),
            ("Product Code", "Description", "Trade Price"),
            ("MRD-FAC-030", "Facilities item 1", 79.07),
            ("MRD-SAF-015", "Safety item 5", 94.74)]


def test_each_csv_layout_maps_to_one_shape():
    std = suppliers.read(
        b"sku,description,unit_cost,currency,effective_from\n"
        b"MRD-A,Thing,90.35,USD,2024-03-01\n",
        arrival("suppliers/voss-industrial/voss-industrial_2024-03.csv"))
    kes = suppliers.read(
        b"sku,cost_cents,effective\nMRD-B,5529,2024-03-01\n",
        arrival("suppliers/kestrel-supply/kestrel_20240301.csv"))
    pem = suppliers.read(
        b"Item Code,Item,Cost (USD),Valid From\n"
        b"MRD-C,Other,58.50,01/03/2024\n",
        arrival("suppliers/pemberton-mills/PEMBERTON-PRICES-202403.csv"))
    assert [p.logical_key for p in (std, kes, pem)] == [
        "voss-industrial/2024-03", "kestrel-supply/2024-03",
        "pemberton-mills/2024-03"]
    assert [p.note for p in (std, kes, pem)] == [
        "standard", "kestrel", "pemberton"]
    assert set(std.rows[0]) == set(kes.rows[0]) == set(pem.rows[0])
    assert (std.rows[0]["cost"], std.rows[0]["cost_unit"]) == ("90.35",
                                                              "dollars")
    assert (kes.rows[0]["cost"], kes.rows[0]["cost_unit"]) == ("5529",
                                                              "cents")
    assert pem.rows[0]["effective_from"] == "01/03/2024"   # as sent
    assert kes.rows[0]["description"] is None


def test_an_unknown_layout_is_refused():
    with pytest.raises(ValueError, match="no known layout"):
        suppliers.read(b"code,price\nX,1\n",
                       arrival("suppliers/new-co/prices.csv"))


def test_the_spreadsheet_is_read_by_finding_its_header_and_date():
    p = suppliers.read(sheet(ALDRIDGE),
                       arrival("suppliers/aldridge-co/Mar 2024.xlsx"))
    assert p.logical_key == "aldridge-co/2024-03" and p.counted == 2
    assert [(r["sku"], r["cost"], r["effective_from"]) for r in p.rows] \
        == [("MRD-FAC-030", "79.07", "1 March 2024"),
            ("MRD-SAF-015", "94.74", "1 March 2024")]


@pytest.mark.parametrize("change, message", [
    (dict(merge="A1:C1"), "merged cells"),
    (dict(sheets=2), "2 sheets"),
])
def test_a_changed_spreadsheet_stops_the_load(change, message):
    with pytest.raises(ValueError, match=message):
        suppliers.read(sheet(ALDRIDGE, **change),
                       arrival("suppliers/aldridge-co/x.xlsx"))


def test_a_banner_without_a_date_stops_the_load():
    rows = [ALDRIDGE[0], ("Trade prices",)] + ALDRIDGE[2:]
    with pytest.raises(ValueError, match="no date in the banner"):
        suppliers.read(sheet(rows), arrival("suppliers/aldridge-co/x.xlsx"))


def put(path: Path, data: bytes, at: datetime):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    t = (at - datetime(1970, 1, 1)).total_seconds()
    os.utime(path, (t, t))


def test_a_corrected_resend_is_the_version_in_force(tmp_path):
    env = {"landing": tmp_path / "landing", "root": tmp_path / "raw",
           "warehouse": tmp_path / "w.duckdb",
           "archive": tmp_path / "landed"}
    folder = env["landing"] / "suppliers" / "aldridge-co"
    wrong = [r if r[:1] != ("MRD-FAC-030",) else
             ("MRD-FAC-030", "Facilities item 1", 790.7) for r in ALDRIDGE]
    put(folder / "Mar 2024.xlsx", sheet(wrong), datetime(2024, 3, 1, 21))
    suppliers.load("2024-03-01", run_at=datetime(2024, 3, 2, 6), **env)
    put(folder / "Mar 2024 (corrected).xlsx", sheet(ALDRIDGE),
        datetime(2024, 3, 4, 23))
    x = suppliers.load("2024-03-04", run_at=datetime(2024, 3, 5, 6),
                       **env)
    assert x.extra["new_files"] == 1 and x.reconciled
    con = duckdb.connect(str(env["warehouse"]))
    (n,) = con.execute("SELECT count(*) FROM raw.suppliers_price_lists "
                       "WHERE sku = 'MRD-FAC-030'").fetchone()
    assert n == 2                          # raw keeps both versions
    (now,) = files.current(con, "suppliers")
    assert now.name.endswith("(corrected).xlsx")
    (cost,) = con.execute(
        "SELECT cost FROM raw.suppliers_price_lists r JOIN "
        "audit.current_files c ON c.sha256 = r.file_sha256 "
        "WHERE sku = 'MRD-FAC-030'").fetchone()
    assert cost == "79.07"


@pytest.mark.slow
def test_every_list_on_disk_reads(sources):
    from groundwork.config import LANDING
    keys, rows = set(), 0
    for a in files.discover(LANDING, "suppliers"):
        p = suppliers.read(a.path.read_bytes(), a)
        assert p.counted == len(p.rows)
        keys.add(p.logical_key)
        rows += len(p.rows)
    assert len(keys) == 767 and rows == 2034
