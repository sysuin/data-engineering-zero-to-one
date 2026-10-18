"""groundwork.stream.log: an append-only log with offsets, on disk."""
from __future__ import annotations

import gzip

import pytest

from groundwork.stream.log import Log, OffsetOutOfRange, append_files


def records(n, start=0):
    return [f'{{"n": {i}}}'.encode() for i in range(start, start + n)]


def test_offsets_start_at_zero_and_count_up(tmp_path):
    log = Log(tmp_path / "log")
    assert [log.append(v) for v in records(3)] == [0, 1, 2]
    assert log.end_offset == 3
    assert log.read(0, 10) == list(enumerate(records(3)))


def test_append_many_returns_the_last_offset(tmp_path):
    log = Log(tmp_path / "log")
    assert log.append_many(records(5)) == 4
    assert log.append_many([]) == 4


def test_reading_from_the_middle(tmp_path):
    log = Log(tmp_path / "log")
    log.append_many(records(10))
    assert log.read(7, 2) == [(7, b'{"n": 7}'), (8, b'{"n": 8}')]
    assert log.read(10, 5) == []


def test_segments_roll_and_reads_cross_them(tmp_path):
    log = Log(tmp_path / "log", segment_records=4)
    log.append_many(records(10))
    names = sorted(p.name for p in (tmp_path / "log").glob("*.log"))
    assert names == [f"{b:020d}.log" for b in (0, 4, 8)]
    assert [o for o, _ in log.read(2, 7)] == list(range(2, 9))
    assert log.read(3, 3)[-1] == (5, b'{"n": 5}')


def test_a_reopened_log_carries_on(tmp_path):
    with Log(tmp_path / "log", segment_records=4) as log:
        log.append_many(records(6))
    log = Log(tmp_path / "log", segment_records=4)
    assert log.end_offset == 6
    assert log.append(b"x") == 6
    assert log.read(5, 2) == [(5, b'{"n": 5}'), (6, b"x")]


def test_a_torn_record_is_trimmed_on_open(tmp_path):
    with Log(tmp_path / "log") as log:
        log.append_many(records(3))
    with open(tmp_path / "log" / f"{0:020d}.log", "ab") as f:
        f.write(b'{"half')
    log = Log(tmp_path / "log")
    assert log.end_offset == 3
    assert log.append(b"next") == 3
    assert log.read(3, 1) == [(3, b"next")]


def test_a_torn_index_entry_is_trimmed_on_open(tmp_path):
    with Log(tmp_path / "log") as log:
        log.append_many(records(3))
    with open(tmp_path / "log" / f"{0:020d}.idx", "ab") as f:
        f.write(b"\x01\x02\x03")
    assert Log(tmp_path / "log").end_offset == 3


def test_a_record_is_one_line(tmp_path):
    with pytest.raises(ValueError):
        Log(tmp_path / "log").append(b"two\nlines")


def test_reading_outside_the_log_raises(tmp_path):
    log = Log(tmp_path / "log")
    log.append_many(records(2))
    with pytest.raises(OffsetOutOfRange):
        log.read(3)


def test_retention_drops_whole_old_segments(tmp_path):
    log = Log(tmp_path / "log", segment_records=2)
    log.append_many(records(7))
    assert log.retain(keep_from=5) == 2
    assert log.start_offset == 4
    with pytest.raises(OffsetOutOfRange):
        log.read(1)
    assert [o for o, _ in log.read(4, 10)] == [4, 5, 6]
    assert Log(tmp_path / "log", segment_records=2).start_offset == 4


def test_retention_never_drops_the_active_segment(tmp_path):
    log = Log(tmp_path / "log", segment_records=2)
    log.append_many(records(3))
    assert log.retain(keep_from=100) == 1
    assert log.start_offset == 2 and log.end_offset == 3


def test_append_files_skips_files_already_produced(tmp_path):
    folder = tmp_path / "a folder with spaces"
    folder.mkdir()
    paths = []
    for name, lines in (("01", 3), ("02", 2)):
        p = folder / f"{name}.jsonl.gz"
        with gzip.open(p, "wb") as f:
            f.write(b"".join(v + b"\n" for v in records(lines)))
        paths.append(p)
    log = Log(tmp_path / "log")
    assert append_files(log, paths) == 5
    assert append_files(log, paths) == 0
    assert log.end_offset == 5
