# A table format in forty lines: data files, plus a log of snapshots
# that says which files make up the table at each version.
import hashlib
import json
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from _order_lines import HERE, lines_parquet


class TinyTable:
    def __init__(self, root: Path):
        self.root, self.log = root, root / "_log"
        self.log.mkdir(parents=True, exist_ok=True)

    def versions(self) -> list[int]:
        return sorted(int(p.stem) for p in self.log.glob("*.json"))

    def files(self, version: int | None = None) -> list[str]:
        """Replay the log to a version: the files live at that point."""
        live: list[str] = []
        for v in self.versions():
            if version is not None and v > version:
                break
            entry = json.loads((self.log / f"{v:05d}.json").read_text())
            live = [f for f in live if f not in entry["remove"]]
            live += entry["add"]
        return live

    def commit(self, op: str, table=None, remove=()) -> int:
        add = []
        if table is not None:                   # data first, named by
            buf = pa.BufferOutputStream()       # its content, so two
            pq.write_table(table, buf)          # writers never collide
            data = buf.getvalue().to_pybytes()
            digest = hashlib.sha256(data).hexdigest()[:8]
            add = [f"part-{digest}.parquet"]
            (self.root / add[0]).write_bytes(data)
        v = len(self.versions())
        entry = {"version": v, "op": op, "add": add,
                 "remove": list(remove)}
        # Then the log entry: written whole under a private name, and
        # linked into place only if no other writer has taken v.
        tmp = self.log / f".{v:05d}.{os.getpid()}.tmp"
        tmp.write_text(json.dumps(entry))
        try:
            os.link(tmp, self.log / f"{v:05d}.json")
        finally:
            tmp.unlink()
        return v

    def read(self, version: int | None = None) -> str:
        """SQL for the table as it was at a version (default latest)."""
        paths = [str(self.root / f) for f in self.files(version)]
        return f"read_parquet({paths!r})"


root = HERE / "tiny_table"
shutil.rmtree(root, ignore_errors=True)
t = TinyTable(root)
con = duckdb.connect()
day = """SELECT * FROM '{}' WHERE placed_at::DATE = DATE '{}' {}
         ORDER BY order_id, line_no"""
src = lines_parquet()
d1 = con.sql(day.format(src, "2025-12-01", "")).to_arrow_table()
d2 = con.sql(day.format(src, "2025-12-02", "")).to_arrow_table()
kept = con.sql(day.format(src, "2025-12-01",
                          "AND status <> 'cancelled'")).to_arrow_table()

t.commit("append 1 Dec", d1)
t.commit("append 2 Dec", d2)
t.commit("rewrite 1 Dec, less cancelled", kept,
         remove=t.files(0))                 # 1 Dec's file, from v0

print("version  files  rows    cents          operation")
for v in t.versions():
    rows, cents = con.execute(f"SELECT count(*), sum(qty * "
                              f"unit_price_cents) FROM {t.read(v)}"
                              ).fetchone()
    op = json.loads((t.log / f"{v:05d}.json").read_text())["op"]
    print(f"{v:>7} {len(t.files(v)):>6} {rows:>5,} {cents:>13,}  {op}")
on_disk = sorted(p.name for p in root.glob("*.parquet"))
print(f"live now {', '.join(t.files())} ({len(on_disk)} on disk)")

Path(__file__).with_suffix(".json").write_text(json.dumps({
    "log": [json.loads(p.read_text()) for p in sorted(t.log.iterdir())],
    "disk": on_disk}, indent=1))
