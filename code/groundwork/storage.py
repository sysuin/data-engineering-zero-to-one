"""
The raw layer: where it lives, how a partition is written, how DuckDB
sees it.

Raw is what each source sent, as the source typed it, kept as Parquet
files laid out by source, table and load date:

    data/groundwork/raw/<source>/<table>/load_date=YYYY-MM-DD/
        part-0.parquet

One load of one table on one day is one file. Loading the same day
again replaces that file whole, so a rerun never doubles a partition,
and a reader never sees half of one: the file is written under a
temporary name in the same folder and renamed into place, which on
one file system happens all at once or not at all.

Every row carries three columns the source did not send:

    _loaded_at     when the load ran (UTC, no zone), from the run
    _load_id       which load wrote it
    _source_file   the file it came from (or _source_query, for a
                   source read with a query)

In DuckDB each table is a view, raw.<source>_<table>, over all its
partitions, with load_date (a DATE) read from the folder names.

The API, which Chapters 6 to 10 build every loader on:

    raw_path(source, table, load_date)       where a partition lives
    write_partition(data, source, table, load_date,
                    loaded_at=..., load_id=..., source_file=...)
    partitions(source, table)                load dates written so far
    raw_views(con)                           one view per raw table
"""
from __future__ import annotations

import os
import re
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from groundwork.config import RAW

NAME = re.compile(r"^[a-z][a-z0-9_]*$")
PART = "part-0.parquet"
ADDED = ("_loaded_at", "_load_id", "_source_file", "_source_query",
         "load_date")


def _name(kind: str, value: str) -> str:
    """Names become folders and view names: keep them plain."""
    if not NAME.match(value):
        raise ValueError(f"{kind} {value!r}: use a-z, 0-9 and _")
    return value


def _day(load_date) -> date:
    """A load date is a calendar date: a date or 'YYYY-MM-DD'."""
    if isinstance(load_date, datetime):
        raise TypeError("load_date is a calendar date, not a moment")
    if isinstance(load_date, date):
        return load_date
    return date.fromisoformat(str(load_date))


def _utc(moment: datetime) -> datetime:
    """Stored moments are UTC with no zone; naive input is UTC."""
    if moment.tzinfo is not None:
        moment = moment.astimezone(timezone.utc).replace(tzinfo=None)
    return moment


def raw_path(source: str, table: str, load_date,
             root: Path | None = None) -> Path:
    """The file that holds one table's load for one day."""
    root = Path(root) if root is not None else RAW
    folder = f"load_date={_day(load_date).isoformat()}"
    return (root / _name("source", source) / _name("table", table)
            / folder / PART)


def _arrow(data) -> pa.Table:
    """A pyarrow Table from whatever a loader has to hand."""
    if isinstance(data, pa.Table):
        return data
    if hasattr(data, "to_arrow_table"):        # a DuckDB relation
        return data.to_arrow_table()
    if isinstance(data, pa.RecordBatchReader):
        return data.read_all()
    if hasattr(data, "to_parquet"):            # a pandas DataFrame
        return pa.Table.from_pandas(data, preserve_index=False)
    if isinstance(data, list):                 # a list of dicts
        return pa.Table.from_pylist(data)
    raise TypeError(f"cannot write a {type(data).__name__}")


def write_partition(data, source: str, table: str, load_date, *,
                    loaded_at: datetime, load_id: str,
                    source_file: str | None = None,
                    source_query: str | None = None,
                    root: Path | None = None) -> Path:
    """
    Write one table's rows for one load date, replacing what was there.

    `data` is a pyarrow Table, a DuckDB relation, a pandas DataFrame
    or a list of dicts, and its columns are kept as they are.
    `loaded_at` and `load_id` come from the run. Pass `source_query`
    for a source read with a query; otherwise `source_file`, which
    may be None. Returns the path written.
    """
    if source_file is not None and source_query is not None:
        raise ValueError("rows come from a file or a query, not both")
    tbl = _arrow(data)
    clash = [c for c in tbl.column_names if c in ADDED]
    if clash:
        raise ValueError(f"the data already has raw's columns: {clash}")

    n = tbl.num_rows
    lineage, origin = (("_source_query", source_query)
                       if source_query is not None
                       else ("_source_file", source_file))
    added = {"_loaded_at": pa.array([_utc(loaded_at)] * n,
                                    pa.timestamp("us")),
             "_load_id": pa.array([load_id] * n, pa.string()),
             lineage: pa.array([origin] * n, pa.string())}
    for name, values in added.items():
        tbl = tbl.append_column(name, values)

    path = raw_path(source, table, load_date, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    # A hidden name in the same folder: readers' globs skip it, and a
    # rename within one folder is atomic.
    tmp = path.with_name(f".{PART}.{os.getpid()}.tmp")
    try:
        pq.write_table(tbl, tmp, compression="zstd")
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)
    return path


def partitions(source: str, table: str,
               root: Path | None = None) -> list[date]:
    """The load dates a table has on disk, oldest first."""
    folder = raw_path(source, table, date.min, root).parent.parent
    if not folder.is_dir():
        return []
    return sorted(date.fromisoformat(p.name.removeprefix("load_date="))
                  for p in folder.iterdir()
                  if p.name.startswith("load_date=")
                  and (p / PART).exists())


def raw_views(con, root: Path | None = None) -> list[str]:
    """
    Create or replace raw.<source>_<table> for every raw table on disk.

    Each view reads all the table's partitions, takes load_date from
    the folder names, and lines columns up by name, so a partition
    written after a source added a column still reads. Returns the
    view names, sorted.
    """
    root = Path(root) if root is not None else RAW
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    made = []
    if not root.is_dir():
        return made
    for src in sorted(root.iterdir()):
        if not (src.is_dir() and NAME.match(src.name)):
            continue
        for tbl in sorted(src.iterdir()):
            if not (tbl.is_dir() and NAME.match(tbl.name)):
                continue
            if not partitions(src.name, tbl.name, root):
                continue
            files = (tbl / "load_date=*" / PART).as_posix()
            files = files.replace("'", "''")
            view = f"raw.{src.name}_{tbl.name}"
            con.execute(
                f"CREATE OR REPLACE VIEW {view} AS "
                f"SELECT * FROM read_parquet('{files}', "
                "hive_partitioning = true, union_by_name = true, "
                "hive_types = {'load_date': DATE})")
            made.append(view)
    return made
