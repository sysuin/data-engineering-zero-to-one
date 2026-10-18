"""The raw layer's layout, its atomic writes and its DuckDB views."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from groundwork import storage
from groundwork.storage import partitions, raw_path, raw_views, write_partition

RUN = datetime(2026, 1, 5, 6, 0, tzinfo=timezone.utc)
ROWS = [{"order_id": 1, "sku": "MRD-FAC-034", "unit_price_cents": 1999},
        {"order_id": 2, "sku": "MRD-CLE-006", "unit_price_cents": 250}]


def write(root, rows=ROWS, day="2026-01-04", **kw):
    kw.setdefault("loaded_at", RUN)
    kw.setdefault("load_id", "test-1")
    kw.setdefault("source_file", "orders.csv")
    return write_partition(rows, "tradeline", "orders", day, root=root, **kw)


def test_the_path_follows_the_layout(tmp_path):
    p = raw_path("relay", "accounts", date(2024, 2, 29), root=tmp_path)
    assert p == (tmp_path / "relay" / "accounts" / "load_date=2024-02-29"
                 / "part-0.parquet")
    assert raw_path("relay", "accounts", "2024-02-29", root=tmp_path) == p


@pytest.mark.parametrize("source,table", [("Relay", "accounts"),
                                          ("relay", "accounts.csv"),
                                          ("../x", "t"), ("relay", "")])
def test_names_must_be_plain(tmp_path, source, table):
    with pytest.raises(ValueError):
        raw_path(source, table, "2026-01-04", root=tmp_path)


def test_a_moment_is_not_a_load_date(tmp_path):
    with pytest.raises(TypeError):
        raw_path("relay", "accounts", RUN, root=tmp_path)


def test_rows_are_kept_as_sent_plus_three_columns(tmp_path):
    p = write(tmp_path)
    t = pq.read_table(p)
    assert t.column_names == ["order_id", "sku", "unit_price_cents",
                              "_loaded_at", "_load_id", "_source_file"]
    assert t.schema.field("unit_price_cents").type == pa.int64()
    assert t.column("_loaded_at").to_pylist() == [datetime(2026, 1, 5, 6)] * 2
    assert t.column("_load_id").to_pylist() == ["test-1"] * 2
    assert t.column("_source_file").to_pylist() == ["orders.csv"] * 2


def test_loaded_at_is_stored_as_utc_without_a_zone(tmp_path):
    newark = timezone(timedelta(hours=-5))
    p = write(tmp_path, loaded_at=datetime(2026, 1, 5, 1, 0, tzinfo=newark))
    t = pq.read_table(p)
    assert t.schema.field("_loaded_at").type == pa.timestamp("us")
    assert t.column("_loaded_at")[0].as_py() == datetime(2026, 1, 5, 6, 0)


def test_a_query_source_records_its_query(tmp_path):
    p = write(tmp_path, source_file=None, source_query="SELECT * FROM orders")
    t = pq.read_table(p)
    assert "_source_file" not in t.column_names
    assert t.column("_source_query")[0].as_py() == "SELECT * FROM orders"


def test_file_and_query_together_are_refused(tmp_path):
    with pytest.raises(ValueError):
        write(tmp_path, source_query="SELECT 1")


def test_columns_raw_adds_are_refused_in_the_data(tmp_path):
    with pytest.raises(ValueError):
        write(tmp_path, rows=[{"id": 1, "_load_id": "x"}])
    with pytest.raises(ValueError):
        write(tmp_path, rows=[{"id": 1, "load_date": "2026-01-04"}])


@pytest.mark.parametrize("kind", ["arrow", "relation", "pandas", "reader"])
def test_every_kind_of_input_writes_the_same_rows(tmp_path, kind):
    con = duckdb.connect()
    rel = con.sql("SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, name) "
                  "ORDER BY id")
    data = {"arrow": rel.to_arrow_table(), "relation": rel,
            "pandas": rel.df(), "reader": rel.arrow()}[kind]
    p = write_partition(data, "x", "y", "2026-01-04", loaded_at=RUN,
                        load_id="l", root=tmp_path)
    t = pq.read_table(p)
    assert t.column("id").to_pylist() == [1, 2]
    assert t.column("name").to_pylist() == ["a", "b"]


def test_an_empty_load_still_writes_a_partition(tmp_path):
    empty = pa.table({"order_id": pa.array([], pa.int64())})
    p = write(tmp_path, rows=empty)
    assert pq.read_table(p).num_rows == 0
    assert partitions("tradeline", "orders", root=tmp_path) == [date(2026, 1, 4)]


def test_writing_a_day_again_replaces_it(tmp_path):
    write(tmp_path)
    p = write(tmp_path, rows=ROWS[:1], load_id="test-2")
    assert pq.read_table(p).num_rows == 1
    assert sorted(x.name for x in p.parent.iterdir()) == ["part-0.parquet"]
    assert pq.read_table(p).column("_load_id")[0].as_py() == "test-2"


def test_a_failed_write_leaves_the_old_partition_whole(tmp_path, monkeypatch):
    p = write(tmp_path)
    before = p.read_bytes()

    def dies_half_way(table, where, **kw):
        with open(where, "wb") as f:
            f.write(b"PAR1 half a file")
        raise OSError("disk full")

    monkeypatch.setattr(storage.pq, "write_table", dies_half_way)
    with pytest.raises(OSError):
        write(tmp_path, load_id="test-2")
    assert p.read_bytes() == before
    assert sorted(x.name for x in p.parent.iterdir()) == ["part-0.parquet"]


def test_partitions_lists_load_dates_in_order(tmp_path):
    for day in ["2026-01-04", "2025-12-30", "2026-01-01"]:
        write(tmp_path, day=day)
    assert partitions("tradeline", "orders", root=tmp_path) == [
        date(2025, 12, 30), date(2026, 1, 1), date(2026, 1, 4)]
    assert partitions("tradeline", "missing", root=tmp_path) == []


def test_raw_views_read_every_partition_with_its_load_date(tmp_path):
    write(tmp_path, day="2026-01-03")
    write(tmp_path, day="2026-01-04", rows=ROWS[:1])
    write_partition([{"account_id": 7}], "relay", "accounts", "2026-01-04",
                    loaded_at=RUN, load_id="r", root=tmp_path)
    con = duckdb.connect()
    assert raw_views(con, root=tmp_path) == ["raw.relay_accounts",
                                             "raw.tradeline_orders"]
    got = con.execute("SELECT load_date, count(*) FROM raw.tradeline_orders "
                      "GROUP BY 1 ORDER BY 1").fetchall()
    assert got == [(date(2026, 1, 3), 2), (date(2026, 1, 4), 1)]


def test_raw_views_line_up_a_column_added_later(tmp_path):
    write(tmp_path, day="2026-01-03")
    later = [dict(r, channel="web") for r in ROWS]
    write(tmp_path, day="2026-01-04", rows=later)
    con = duckdb.connect()
    raw_views(con, root=tmp_path)
    got = con.execute("SELECT load_date, channel FROM raw.tradeline_orders "
                      "ORDER BY load_date, order_id").fetchall()
    assert got == [(date(2026, 1, 3), None), (date(2026, 1, 3), None),
                   (date(2026, 1, 4), "web"), (date(2026, 1, 4), "web")]


def test_raw_views_ignore_a_leftover_temporary_file(tmp_path):
    p = write(tmp_path)
    (p.parent / ".part-0.parquet.99.tmp").write_bytes(b"PAR1 half")
    con = duckdb.connect()
    raw_views(con, root=tmp_path)
    assert con.execute("SELECT count(*) FROM raw.tradeline_orders"
                       ).fetchone() == (2,)


def test_raw_views_on_an_empty_root_make_the_schema_only(tmp_path):
    con = duckdb.connect()
    assert raw_views(con, root=tmp_path / "nothing") == []
    assert con.execute("SELECT count(*) FROM information_schema.schemata "
                       "WHERE schema_name = 'raw'").fetchone() == (1,)
