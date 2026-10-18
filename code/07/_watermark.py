# A watermark loader small enough to read whole, and Tradeline replayed
# night by night at 02:05 UTC; this chapter's listings share both.
import sqlite3
from datetime import datetime, timedelta

import duckdb
import pyarrow as pa

from groundwork.config import SANDBOX
from meridian.sources import tradeline

DB = SANDBOX / "ch07" / "tradeline.db"         # the one working copy
KEYS = {"orders": "id", "order_lines": "order_id, line_no"}
FIRST = datetime(2025, 8, 20, 2, 5)            # the snapshot night


def nights(first=FIRST, n=30):
    """Tradeline as it stood at 02:05 on the first night and n more."""
    for k in range(n + 1):
        moment = first + timedelta(days=k)
        tradeline.at(moment, DB)               # moves forward cheaply
        src = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
        try:
            yield k, moment, src
        finally:
            src.close()


class Loader:
    """Each night, copy the rows past the watermark; keep them all."""

    def __init__(self, column="updated_at", op=">",
                 lookback=timedelta(0)):
        self.column, self.op, self.lookback = column, op, lookback
        self.mark, self.rows_read = {}, 0
        self.db = duckdb.connect()             # the warehouse's copy

    def run(self, night, src):
        for table in KEYS:
            where, args = "", ()
            if table not in self.mark:         # first night: all of it
                self.db.execute(
                    f"CREATE TABLE got_{table} AS SELECT *, {night} AS "
                    f"night FROM sqlite_scan('{DB}', '{table}')")
                (n,) = self.db.execute(
                    f"SELECT count(*) FROM got_{table}").fetchone()
            else:                              # then rows past the mark
                since = self.mark[table]
                if self.lookback:
                    since = str(datetime.fromisoformat(since)
                                - self.lookback)
                where = f"WHERE {self.column} {self.op} ?"
                args = (since,)
                n = self.land(table, night, src.execute(
                    f"SELECT * FROM {table} {where}", args))
            self.rows_read += n
            (top,) = src.execute(f"SELECT max({self.column}) FROM "
                                 f"{table} {where}", args).fetchone()
            if top is not None:                # the new high-water mark
                self.mark[table] = max(top, self.mark.get(table, top))

    def land(self, table, night, cur):
        """Append the night's rows, tagged with the night they came."""
        rows = cur.fetchall()
        if rows:
            batch = pa.table({d[0]: c for d, c in
                              zip(cur.description, zip(*rows))})
            batch = batch.append_column(
                "night", pa.array([night] * len(rows), pa.int32()))
            self.db.from_arrow(batch).insert_into(f"got_{table}")
        return len(rows)

    def copy(self, table):
        """The copy as it stands: the latest version of every key."""
        return (f"(SELECT * EXCLUDE (night) FROM got_{table} "
                f"QUALIFY row_number() OVER (PARTITION BY "
                f"{KEYS[table]} ORDER BY night DESC) = 1)")


def compare(loader):
    """How the copy differs from Tradeline, one order at a time.
    Leaves Tradeline attached to the loader's database as `src`."""
    db = loader.db
    db.execute(f"ATTACH '{DB}' AS src (TYPE sqlite, READ_ONLY)")
    differ = []                    # rows on one side and not the other
    for t, key in KEYS.items():
        db.execute(f"CREATE OR REPLACE TEMP VIEW copy_{t} AS "
                   f"SELECT * FROM {loader.copy(t)}")
        for a, b in ((f"copy_{t}", f"src.{t}"),
                     (f"src.{t}", f"copy_{t}")):
            differ.append(f"SELECT {key.split(',')[0]} AS id, "
                          f"'{t}' AS t FROM (FROM {a} EXCEPT FROM {b})")
    wrong = db.execute(
        "SELECT id, bool_or(t = 'order_lines'), any_value(c.status), "
        "any_value(s.status), any_value(s.updated_at) "
        f"FROM ({' UNION '.join(differ)}) "
        "LEFT JOIN copy_orders c USING (id) "
        "LEFT JOIN src.orders s USING (id) GROUP BY id ORDER BY id"
    ).fetchall()
    kinds = dict.fromkeys(("deleted in Tradeline", "cancelled since",
                           "lines amended at 02:00",
                           "only updated_at differs", "other"), 0)
    for _, lines, ours, theirs, stamp in wrong:
        if theirs is None:
            kinds["deleted in Tradeline"] += 1
        elif ours != theirs:
            kinds["cancelled since"] += 1
        elif lines and stamp.endswith(" 02:00:00"):
            kinds["lines amended at 02:00"] += 1
        elif not lines:
            kinds["only updated_at differs"] += 1
        else:
            kinds["other"] += 1
    return wrong, kinds
