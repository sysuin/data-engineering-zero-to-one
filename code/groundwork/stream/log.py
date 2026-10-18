"""
An append-only log on disk: Groundwork's stand-in for a Kafka topic.

A log is a folder of segment files. Each record is one line of bytes,
and its offset is its position in the log: the first record ever
appended is offset 0, and offsets never change, are never reused and
never skip. A reader asks for "everything from offset n", which is how
several consumers read one log at their own pace without the log
knowing any of them.

    log = Log(SANDBOX / "ch19" / "portal-log")
    offset = log.append(b'{"event_id": "..."}')
    for offset, value in log.read(0, 1000): ...

On disk (one partition; a Kafka topic is several of these side by side):

    00000000000000000000.log   the records, one per line
    00000000000000000000.idx   where each record ends, 8 bytes a record

A segment holds `segment_records` records; then the log starts a new
one, named by its first offset. Retention deletes whole old segments
and never touches the rest. A crash can leave the last record half
written: the record is written first and its index entry second, and
opening the log trims anything the index does not vouch for.
"""
from __future__ import annotations

import gzip
import struct
from bisect import bisect_right
from collections.abc import Iterable
from pathlib import Path

ENTRY = struct.Struct("<Q")          # one index entry: an end position


class OffsetOutOfRange(LookupError):
    """A read below the oldest retained offset or beyond the end."""


class Log:
    def __init__(self, path, segment_records: int = 100_000):
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        self.segment_records = segment_records
        self._bases = sorted(int(p.stem)
                             for p in self.path.glob("*.log"))
        if not self._bases:
            self._bases = [0]
        self._open_active()

    # -------------------------------------------------------- layout
    def _files(self, base: int) -> tuple[Path, Path]:
        return (self.path / f"{base:020d}.log",
                self.path / f"{base:020d}.idx")

    def _open_active(self) -> None:
        """Open the newest segment to append to; trim a torn end."""
        log, idx = self._files(self._bases[-1])
        log.touch()
        idx.touch()
        whole = idx.stat().st_size // ENTRY.size * ENTRY.size
        with open(idx, "r+b") as f:
            f.truncate(whole)
            if whole:
                f.seek(whole - ENTRY.size)
                end = ENTRY.unpack(f.read(ENTRY.size))[0]
            else:
                end = 0
        with open(log, "r+b") as f:
            f.truncate(end)
        self._count = whole // ENTRY.size
        self._pos = end
        self._log = open(log, "ab")
        self._idx = open(idx, "ab")

    def _roll(self) -> None:
        self.close()
        self._bases.append(self.end_offset)
        self._open_active()

    # ------------------------------------------------------- offsets
    @property
    def start_offset(self) -> int:
        """The oldest offset still on disk."""
        return self._bases[0]

    @property
    def end_offset(self) -> int:
        """The offset the next record will get."""
        return self._bases[-1] + self._count

    # ------------------------------------------------------- writing
    def append(self, value: bytes) -> int:
        """Add one record at the end; return its offset."""
        return self.append_many([value])

    def append_many(self, values: Iterable[bytes]) -> int:
        """Add records in order; return the offset of the last one."""
        last = self.end_offset - 1
        batch: list[bytes] = []
        for value in values:
            if b"\n" in value:
                raise ValueError("a record is one line: no newlines")
            if self._count + len(batch) == self.segment_records:
                self._write(batch)
                batch = []
                self._roll()
            batch.append(value)
            last += 1
        self._write(batch)
        return last

    def _write(self, batch: list[bytes]) -> None:
        """Records first, then the index that vouches for them."""
        if not batch:
            return
        ends = []
        for value in batch:
            self._pos += len(value) + 1
            ends.append(ENTRY.pack(self._pos))
        self._log.write(b"\n".join(batch) + b"\n")
        self._log.flush()
        self._idx.write(b"".join(ends))
        self._idx.flush()
        self._count += len(batch)

    # ------------------------------------------------------- reading
    def read(self, offset: int, max_records: int = 1000
             ) -> list[tuple[int, bytes]]:
        """Up to max_records records from `offset` on, with offsets."""
        if not self.start_offset <= offset <= self.end_offset:
            raise OffsetOutOfRange(
                f"offset {offset} is outside {self.start_offset}"
                f"..{self.end_offset}")
        out: list[tuple[int, bytes]] = []
        while len(out) < max_records and offset < self.end_offset:
            base = self._bases[bisect_right(self._bases, offset) - 1]
            log, idx = self._files(base)
            i = offset - base
            held = idx.stat().st_size // ENTRY.size
            take = min(max_records - len(out), held - i)
            with open(idx, "rb") as f:
                f.seek(max(i - 1, 0) * ENTRY.size)
                raw = f.read((take + (i > 0)) * ENTRY.size)
            ends = [e for (e,) in ENTRY.iter_unpack(raw)]
            start = ends.pop(0) if i > 0 else 0
            with open(log, "rb") as f:
                f.seek(start)
                data = f.read(ends[-1] - start)
            lines = data.split(b"\n")[:-1]
            out.extend(zip(range(offset, offset + take), lines))
            offset += take
        return out

    # ----------------------------------------------------- retention
    def retain(self, keep_from: int) -> int:
        """Delete whole segments that end before keep_from."""
        dropped = 0
        while len(self._bases) > 1 and self._bases[1] <= keep_from:
            for p in self._files(self._bases.pop(0)):
                p.unlink()
            dropped += 1
        return dropped

    def close(self) -> None:
        self._log.close()
        self._idx.close()

    def __enter__(self) -> Log:
        return self

    def __exit__(self, *exc) -> None:
        self.close()


def append_files(log: Log, paths: Iterable[Path]) -> int:
    """
    Append every line of gzipped JSON-lines files, in the order given,
    and return how many records were added. A file is recorded in
    `produced.txt` once all its lines are in, so a rerun skips it; a
    crash half way through a file appends its lines again, which is
    at-least-once, and the consumers downstream are built for it.
    """
    ledger = log.path / "produced.txt"
    done = (set(ledger.read_text().splitlines()) if ledger.exists()
            else set())
    added = 0
    with open(ledger, "a") as record:
        for path in paths:
            name = Path(path).as_posix()
            if name in done:
                continue
            with gzip.open(path, "rb") as f:
                lines = f.read().splitlines()
            log.append_many(lines)
            added += len(lines)
            record.write(name + "\n")
            record.flush()
    return added
