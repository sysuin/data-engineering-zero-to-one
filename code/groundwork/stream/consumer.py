"""
Reading the log: a consumer that remembers where it got to.

A consumer belongs to a group. The group's committed offset is the
next record it has not finished with, kept in a small file beside the
log. Each group reads the whole log at its own pace, and the log
neither knows nor cares how many groups there are.

    consumer = Consumer(log, "archive")
    while batch := consumer.poll(5000):
        sink.write(parse(v) for _, v in batch)    # do the work first
        consumer.commit()                         # then say so

Work first, commit second. A crash between the two means the batch is
read again after the restart: every record is processed at least
once, and some twice. That is why the sinks here are idempotent. The
event sink is keyed on event_id, so an event written twice is stored
once, whether it came twice from a replay or twice from the portal.
"""
from __future__ import annotations

import json
import os
import pickle
from collections.abc import Iterable
from datetime import datetime
from pathlib import Path
import pyarrow as pa

from groundwork.stream.log import Log


class Consumer:
    def __init__(self, log: Log, group: str):
        self.log, self.group = log, group
        self._file = log.path / "offsets" / f"{group}.json"
        self.position = self.committed()

    def committed(self) -> int:
        """The group's committed offset, or the log's start if none."""
        if self._file.exists():
            return json.loads(self._file.read_text())["offset"]
        return self.log.start_offset

    def poll(self, max_records: int = 1000) -> list[tuple[int, bytes]]:
        """The next records after this consumer's position."""
        records = self.log.read(self.position, max_records)
        self.position += len(records)
        return records

    def commit(self, state=None) -> None:
        """
        Record the position, all at once or not at all. A consumer that
        keeps state (open sessions, say) passes it here, and the state
        and the offset are saved as one checkpoint: restoring one
        without the other would count some records twice or never.
        """
        self._file.parent.mkdir(exist_ok=True)
        if state is not None:
            _replace(self._file.with_suffix(".state"), pickle.dumps(
                {"offset": self.position, "state": state}))
        _replace(self._file, json.dumps(
            {"offset": self.position}).encode())

    def restore(self):
        """The state saved with the last checkpoint, or None."""
        saved = self._file.with_suffix(".state")
        if not saved.exists():
            return None
        checkpoint = pickle.loads(saved.read_bytes())
        self.position = checkpoint["offset"]
        return checkpoint["state"]

    def seek(self, offset: int) -> None:
        self.position = offset

    @property
    def lag(self) -> int:
        """Records in the log this group has not committed."""
        return self.log.end_offset - self.committed()


def _replace(path: Path, data: bytes) -> None:
    """Write beside the file, then rename over it in one step."""
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


# ------------------------------------------------------------ events
def _moment(text: str) -> datetime:
    """'2025-11-10T15:00:34.486Z' as a UTC datetime without a zone."""
    return datetime.fromisoformat(text).replace(tzinfo=None)


def parse(value: bytes) -> dict:
    """
    One portal event, in the one shape Groundwork uses. The portal's
    schema 2 (web from 1 October 2025, the app from 10 November)
    renamed sku to product_sku and added cart_value_cents; both
    versions come out with `sku` and `value_cents` (None in v1).
    """
    e = json.loads(value)
    p = e["properties"]
    return {
        "event_id": e["event_id"],
        "event_type": e["event_type"],
        "session_id": e["session_id"],
        "account_id": e["account_id"],
        "device": e["device"],
        "user_agent": e["user_agent"],
        "occurred_at": _moment(e["occurred_at"]),
        "received_at": _moment(e["received_at"]),
        "sku": p.get("product_sku", p.get("sku")),
        "value_cents": p.get("cart_value_cents"),
        "order_number": p.get("order_number"),
        "schema_version": e["schema_version"],
    }


EVENTS_DDL = """
CREATE SCHEMA IF NOT EXISTS stream;
CREATE TABLE IF NOT EXISTS stream.portal_events (
    event_id        VARCHAR PRIMARY KEY,
    event_type      VARCHAR NOT NULL,
    session_id      VARCHAR NOT NULL,
    account_id      INTEGER NOT NULL,
    device          VARCHAR,
    user_agent      VARCHAR,
    occurred_at     TIMESTAMP NOT NULL,   -- the device's clock
    received_at     TIMESTAMP NOT NULL,   -- the portal's server
    sku             VARCHAR,
    value_cents     BIGINT,
    order_number    INTEGER,
    schema_version  INTEGER NOT NULL
);
"""
COLUMNS = ("event_id", "event_type", "session_id", "account_id",
           "device", "user_agent", "occurred_at", "received_at", "sku",
           "value_cents", "order_number", "schema_version")


class EventSink:
    """Events in DuckDB, keyed on event_id: a second copy is ignored."""

    def __init__(self, con):
        self.con = con
        con.execute(EVENTS_DDL)

    def write(self, events: Iterable[dict]) -> int:
        """Insert the events not yet stored; return how many."""
        rows = list(events)
        if not rows:
            return 0
        batch = pa.Table.from_pylist(rows).select(list(COLUMNS))
        self.con.register("_batch", batch)
        try:
            (n,) = self.con.execute(
                "INSERT OR IGNORE INTO stream.portal_events "
                "SELECT * FROM _batch").fetchone()
        finally:
            self.con.unregister("_batch")
        return n


def archive(log: Log, con, group: str = "archive", batch: int = 5000,
            stop_after: int | None = None) -> dict:
    """
    Copy the log into stream.portal_events, committing after each
    batch. `stop_after` stops (as a crash would) once that many records
    have been read, before committing the last batch.
    """
    consumer, sink = Consumer(log, group), EventSink(con)
    read = new = 0
    while records := consumer.poll(batch):
        read += len(records)
        new += sink.write(parse(v) for _, v in records)
        if stop_after is not None and read >= stop_after:
            return {"read": read, "new": new, "committed": False}
        consumer.commit()
    return {"read": read, "new": new, "committed": True}
