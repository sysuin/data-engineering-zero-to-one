"""
Tradeline, Meridian's orders application: its database as it stood at any moment.

`tradeline.db` under data/meridian-sources/tradeline/ is the database as it stands today.
`_history.db` beside it holds every version of every row, with the moment it committed.
This module replays that history into a working copy:

    from meridian.sources import tradeline
    path = tradeline.at("2025-03-14 02:05:00", "data/sandbox/tradeline.db")

A working copy remembers the moment it stands at, so asking for a later moment applies
only the changes in between, the way a replica catches up. Asking for an earlier one
rebuilds it. Rows that have been written but not committed by that moment are invisible,
exactly as they would be to a reader of the real database.

The copy also carries `replication_log`: the change log a database keeps for replicas,
for the seven days before the copy's moment, one row per committed change.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import timedelta

from meridian.sources import NOW, SOURCES, parse_moment

HISTORY = SOURCES / "tradeline" / "_history.db"
TODAY_DB = SOURCES / "tradeline" / "tradeline.db"
LOG_RETENTION = timedelta(days=7)

SCHEMA = """
CREATE TABLE channels (code TEXT PRIMARY KEY, label TEXT NOT NULL);
CREATE TABLE orders (
    id           INTEGER PRIMARY KEY,
    order_number INTEGER NOT NULL UNIQUE,
    account_id   INTEGER NOT NULL,
    placed_at    TEXT NOT NULL,
    channel      TEXT NOT NULL REFERENCES channels(code),
    status       TEXT NOT NULL,
    created_at   TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);
CREATE TABLE order_lines (
    order_id         INTEGER NOT NULL REFERENCES orders(id),
    line_no          INTEGER NOT NULL,
    sku              TEXT NOT NULL,
    qty              INTEGER NOT NULL,
    unit_price_cents INTEGER NOT NULL,
    discount_pct     INTEGER NOT NULL,
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL,
    PRIMARY KEY (order_id, line_no)
);
CREATE INDEX idx_orders_updated ON orders(updated_at);
CREATE INDEX idx_orders_account ON orders(account_id);
CREATE INDEX idx_lines_updated ON order_lines(updated_at);
CREATE TABLE replication_log (
    lsn          INTEGER PRIMARY KEY,   -- log sequence number: the order changes committed in
    committed_at TEXT NOT NULL,
    table_name   TEXT NOT NULL,
    op           TEXT NOT NULL,         -- I insert, U update, D delete
    row_json     TEXT NOT NULL          -- the row after the change; for a delete, its key
);
CREATE TABLE _sim (as_of TEXT NOT NULL);
"""
CHANNELS = [("WEB", "Web"), ("FLD", "Field sales"), ("TEL", "Telesales"), ("PTR", "Partner")]

ORDER_COLS = "id, order_number, account_id, placed_at, channel, status, created_at, updated_at"
LINE_COLS = ("order_id, line_no, sku, qty, unit_price_cents, discount_pct, created_at, "
             "updated_at")
# The history keeps moments as seconds since 1970; the application writes them as text.
T = "strftime('%Y-%m-%d %H:%M:%S', {}, 'unixepoch')"
ORDER_SELECT = (f"id, order_number, account_id, {T.format('placed_at')}, channel, status, "
                f"{T.format('created_at')}, {T.format('updated_at')}")
LINE_SELECT = (f"order_id, line_no, sku, qty, unit_price_cents, discount_pct, "
               f"{T.format('created_at')}, {T.format('updated_at')}")


def _stamp(when) -> str:
    return parse_moment(when).strftime("%Y-%m-%d %H:%M:%S")


def _secs(stamp: str) -> int:
    return int(parse_moment(stamp).timestamp())


def _apply(con: sqlite3.Connection, lo: str | None, hi: str) -> None:
    """Apply every change committed in (lo, hi]: for each key, its last version wins."""
    window = "committed_at <= :hi" + ("" if lo is None else " AND committed_at > :lo")
    p = {"lo": None if lo is None else _secs(lo), "hi": _secs(hi)}
    for table, key, cols, select in (("orders", "id", ORDER_COLS, ORDER_SELECT),
                                     ("order_lines", "order_id, line_no", LINE_COLS, LINE_SELECT)):
        latest = (f"SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY {key} "
                  f"ORDER BY lsn DESC) AS rn FROM h.{table}_v WHERE {window}) WHERE rn = 1")
        con.execute(f"INSERT OR REPLACE INTO {table} ({cols}) SELECT {select} FROM ({latest}) "
                    f"WHERE op <> 'D'", p)
        keys = key.split(", ")
        match = " AND ".join(f"{table}.{k} = d.{k}" for k in keys)
        con.execute(f"DELETE FROM {table} WHERE EXISTS (SELECT 1 FROM ({latest}) d "
                    f"WHERE d.op = 'D' AND {match})", p)


def _log(con: sqlite3.Connection, hi: str) -> None:
    """Refill the replication log with the changes of the seven days up to `hi`."""
    lo = _secs(hi) - int(LOG_RETENTION.total_seconds())
    t = T.format
    con.execute("DELETE FROM replication_log")
    con.execute(
        f"INSERT INTO replication_log SELECT lsn, {t('committed_at')}, 'orders', op, "
        f"CASE op WHEN 'D' THEN json_object('id', id) ELSE json_object('id', id, "
        f"'order_number', order_number, 'account_id', account_id, "
        f"'placed_at', {t('placed_at')}, 'channel', channel, 'status', status, "
        f"'created_at', {t('created_at')}, 'updated_at', {t('updated_at')}) END "
        f"FROM h.orders_v WHERE committed_at > ? AND committed_at <= ?", (lo, _secs(hi)))
    con.execute(
        f"INSERT INTO replication_log SELECT lsn, {t('committed_at')}, 'order_lines', op, "
        f"CASE op WHEN 'D' THEN json_object('order_id', order_id, 'line_no', line_no) "
        f"ELSE json_object('order_id', order_id, 'line_no', line_no, 'sku', sku, 'qty', qty, "
        f"'unit_price_cents', unit_price_cents, 'discount_pct', discount_pct, "
        f"'created_at', {t('created_at')}, 'updated_at', {t('updated_at')}) END "
        f"FROM h.order_lines_v WHERE committed_at > ? AND committed_at <= ?", (lo, _secs(hi)))


def materialise(history, dest, when) -> str:
    """Build a fresh copy of the database at `when`. Used by the generator and by `at`."""
    dest = str(dest)
    if os.path.exists(dest):
        os.remove(dest)
    os.makedirs(os.path.dirname(os.path.abspath(dest)), exist_ok=True)
    hi = _stamp(when)
    con = sqlite3.connect(dest)
    con.executescript(SCHEMA)
    con.executemany("INSERT INTO channels VALUES (?, ?)", CHANNELS)
    con.execute("ATTACH DATABASE ? AS h", (str(history),))
    _apply(con, None, hi)
    _log(con, hi)
    con.execute("INSERT INTO _sim VALUES (?)", (hi,))
    con.commit()
    con.execute("DETACH DATABASE h")
    con.close()
    return dest


def at(when, dest, history=HISTORY) -> str:
    """
    A working copy of Tradeline as it stood at `when`, at path `dest`. If `dest` already
    holds an earlier moment, only the changes since are applied.
    """
    dest = str(dest)
    hi = _stamp(when)
    if parse_moment(when) > NOW:
        raise ValueError(f"Tradeline's history ends at {NOW:%Y-%m-%d %H:%M} UTC")
    if os.path.exists(dest):
        con = sqlite3.connect(dest)
        try:
            (lo,) = con.execute("SELECT as_of FROM _sim").fetchone()
        except sqlite3.Error:
            lo = None
        if lo is not None and lo <= hi:
            con.execute("ATTACH DATABASE ? AS h", (str(history),))
            _apply(con, lo, hi)
            _log(con, hi)
            con.execute("UPDATE _sim SET as_of = ?", (hi,))
            con.commit()
            con.execute("DETACH DATABASE h")
            con.close()
            return dest
        con.close()
    return materialise(history, dest, when)


def connect(path=TODAY_DB) -> sqlite3.Connection:
    """Tradeline as it stands today, read-only: a reader may look, and may not write."""
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True)
