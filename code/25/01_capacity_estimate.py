# Meridian measured, and four platforms sized before any boxes.
import gzip
import json
import shutil
from dataclasses import asdict, dataclass
from pathlib import Path

import duckdb

from groundwork.config import LANDING, SANDBOX, SOURCES, TRADELINE_DB

DAY = 86_400             # seconds
OUTAGE_H = 48            # a weekend with the pipeline down
CATCH_UP_H = 6           # hours allowed to clear what piled up


@dataclass
class Workload:
    name: str
    rows_day: float      # rows (events, changes, readings) a day
    peak_ratio: float    # busiest hour's rate over the mean hour's
    sent_bytes: float    # one row as the source sends it
    stored_bytes: float  # one row in compressed columnar files
    keep_days: int       # history kept online

    def estimate(self) -> dict:
        mean_s = self.rows_day / DAY
        backlog = self.rows_day * OUTAGE_H / 24
        return {
            "rows a day": self.rows_day,
            "peak rows/s": mean_s * self.peak_ratio,
            "sent MB/day": self.rows_day * self.sent_bytes / 1e6,
            "stored MB/day": self.rows_day * self.stored_bytes / 1e6,
            "stored GB kept": self.rows_day * self.stored_bytes
            * self.keep_days / 1e9,
            "backlog MB": backlog * self.sent_bytes / 1e6,
            "catch-up rows/s": mean_s + backlog / (CATCH_UP_H * 3600)}


def show(x: float) -> str:
    """Three significant figures, with k, M or G for large numbers."""
    if 0 < abs(x) < 0.01:
        return "<0.01"
    for unit, size in (("G", 1e9), ("M", 1e6), ("k", 1e3)):
        if abs(x) >= size:
            return f"{x / size:.3g}{unit}"
    return f"{x:.3g}"


def parquet_bytes(con, query: str, path: Path) -> int:
    """Write a query's result as Parquet and return the file's size."""
    con.execute(f"COPY ({query}) TO '{path}' (FORMAT parquet)")
    return path.stat().st_size


def measure(con, work: Path) -> tuple[dict, float]:
    """Rows a day, and bytes a row as sent and as stored, per source."""
    con.execute(f"ATTACH '{TRADELINE_DB}' AS t (TYPE sqlite, READ_ONLY)")
    new = "WHERE created_at >= '2025-01-01' AND created_at < '2026-01-01'"
    rows = con.execute(f"""SELECT (SELECT COUNT(*) FROM t.orders {new})
        + (SELECT COUNT(*) FROM t.order_lines {new})""").fetchone()[0]
    (sent,) = con.execute("""SELECT AVG(length(lsn::VARCHAR)
        + length(committed_at) + length(table_name) + length(op)
        + length(row_json)) FROM t.replication_log""").fetchone()
    stored = sum(parquet_bytes(con, f"SELECT * FROM t.{table} {new}",
                               work / f"{table}.parquet")
                 for table in ("orders", "order_lines"))
    out = {"tradeline": (rows / 365, sent, stored / rows)}

    files = sorted((LANDING / "portal" / "2025" / "12").glob("*/*.gz"))
    per_hour, text = [], 0
    for f in files:
        lines = gzip.decompress(f.read_bytes()).splitlines()
        per_hour.append(len(lines))
        text += sum(len(line) + 1 for line in lines)
    events = sum(per_hour)
    stored = parquet_bytes(con, "SELECT * FROM read_json("
                           f"{[str(f) for f in files]})",
                           work / "portal.parquet")
    out["portal"] = (events / 31, text / events, stored / events)
    peak = max(per_hour) / (events / len(per_hour))

    night = max((LANDING / "relay").glob("20*"))
    rows = text = stored = 0
    for name in ("accounts", "contracts"):
        csv = night / f"{name}.csv"
        text += csv.stat().st_size
        rows += con.execute("SELECT COUNT(*) FROM read_csv("
                            f"'{csv}')").fetchone()[0]
        stored += parquet_bytes(con, f"SELECT * FROM read_csv('{csv}')",
                                work / f"{name}.parquet")
    out["relay"] = (rows, text / rows, stored / rows)
    return out, peak


def main():
    work = SANDBOX / "ch25"
    work.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    sources, peak = measure(con, work)
    shutil.rmtree(work)
    manifest = json.loads((SOURCES / "manifest.json").read_text())
    tickets = manifest["counts"]["tickets"] / 1461

    print(f"{'Meridian, measured':24}{'rows/day':>10}{'sent B':>10}"
          f"{'stored B':>10}{'ratio':>8}")
    for name, (rows, sent, stored) in sources.items():
        print(f"{name:24}{rows:>10,.0f}{sent:>10.0f}{stored:>10.1f}"
              f"{sent / stored:>8.1f}")
    print(f"frontline tickets a day: {tickets:.1f}; portal's busiest "
          f"hour: {peak:.1f}x its mean\n")

    rows = sum(r for r, _, _ in sources.values())
    sent = sum(r * s for r, s, _ in sources.values()) / rows
    stored = sum(r * s for r, _, s in sources.values()) / rows
    click = sources["portal"][2] / sources["portal"][1]
    table = sources["tradeline"][2] / sources["tradeline"][1]
    # Meridian is measured. The rest are assumptions, written down so
    # that they can be argued with: rows a day, peak ratio, bytes sent,
    # bytes stored (scaled by Meridian's measured ratios), days kept.
    systems = [
        Workload("Meridian", rows, peak, sent, stored, 1461),
        Workload("Clicks", 5e6 * 60, 3, 800, 800 * click, 730),
        Workload("CDC", 60e6, 4, 700, 700 * table, 365),
        Workload("IoT", 200e3 * 20 * DAY / 10, 1.2, 40, 3, 90),
        Workload("Finance", 400e3 * 8, 5, 400, 400 * table, 7 * 365)]
    est = {s.name: s.estimate() for s in systems}
    print(f"{'':16}" + "".join(f"{n:>10}" for n in est))
    for metric in est["Meridian"]:
        print(f"{metric:16}" + "".join(
            f"{show(e[metric]):>10}" for e in est.values()))
    Path("code/25/01_capacity_estimate.json").write_text(json.dumps(
        {"systems": [asdict(s) for s in systems], "estimates": est},
        indent=1))


if __name__ == "__main__":
    main()
