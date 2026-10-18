# A platform's bill in units that do not date: bytes, hours, people.
import json
from dataclasses import dataclass
from pathlib import Path

MONTH = 30               # days
GB_PER_CORE_H = 30       # assumed: GB a warehouse core reads, joins and
                         # writes in an hour of ordinary transformation
BUILD_DAYS = 10          # assumed: engineer-days to build one connector
RUN_DAYS = 2             # ... and to keep it working, a month
SETUP_DAYS = 1           # a bought connector: to set it up
WATCH_DAYS = 0.25        # ... and to watch it, a month


@dataclass
class Usage:
    queries_day: int     # dashboard refreshes and analysts' queries
    days_read: float     # days of history a typical query needs
    cols_read: float     # share of the columns it needs
    runs_day: int        # transformation runs a day
    days_run: float      # days of data an incremental run reads
    amplify: float       # bytes rewritten per byte changed (merges)
    rollup: float        # share of raw bytes a dashboard reads, if
                         # it reads a pre-aggregated table instead
    sources: int         # systems the platform ingests from
    buyable: int         # ... of which an off-the-shelf connector
                         # exists (databases, SaaS APIs, files)


def units(est: dict, keep: int, u: Usage, designed: bool) -> dict:
    """A month's usage, in units a price list multiplies."""
    day_gb = est["stored MB/day"] / 1e3
    if designed:         # partitions, rollups, incremental runs
        per_query = day_gb * u.days_read * u.cols_read * u.rollup
        per_run = day_gb * u.days_run * u.amplify
    else:                # no partitions, no rollups, full refreshes
        per_query = day_gb * keep * u.cols_read
        per_run = day_gb * keep
    queries = u.queries_day * per_query * MONTH
    runs = u.runs_day * per_run * MONTH
    return {"stored GB-mo": est["stored GB kept"],
            "queries TB": queries / 1e3,
            "runs TB": runs / 1e3,
            "core-h": (queries + runs) / GB_PER_CORE_H}


def people(u: Usage, buy: bool) -> tuple[float, float]:
    """Engineer-days to build, then a month to run, the connectors."""
    bought = u.buyable if buy else 0
    built = u.sources - bought
    return (built * BUILD_DAYS + bought * SETUP_DAYS,
            built * RUN_DAYS + bought * WATCH_DAYS)


def bill(month: dict, prices: dict) -> float:
    """Units times the prices you look up in Appendix C."""
    return sum(month[unit] * prices[unit] for unit in prices)


def show(x: float) -> str:
    """Three significant figures, with k, M or G for large numbers."""
    if 0 < abs(x) < 0.01:
        return "<0.01"
    for unit, size in (("G", 1e9), ("M", 1e6), ("k", 1e3)):
        if abs(x) >= size:
            return f"{x / size:.3g}{unit}"
    return f"{x:.3g}"


USAGE = {  # assumptions, one line a platform, to be argued with
    # nightly, reading yesterday and a three-day lookback
    "Meridian": Usage(200, 90, 0.25, 1, 4, 1, 1, 7, 4),
    # hourly builds, plus a nightly rebuild of three days
    "Clicks": Usage(2000, 30, 0.1, 24, 4 / 24, 1, 1, 3, 2),
    # a merge every fifteen minutes, rewriting ten times its bytes
    "CDC": Usage(1500, 30, 0.2, 96, 1 / 96, 10, 1, 40, 40),
    # dashboards read minute rollups; a nightly three-day rebuild
    "IoT": Usage(5000, 1, 0.5, 1, 3, 1, 1 / 6, 2, 0),
    # nightly, three days for late settlements; merges rewrite 2x
    "Finance": Usage(300, 31, 0.3, 1, 3, 2, 1, 6, 4)}


def main():
    data = json.loads(Path("code/25/01_capacity_estimate.json")
                      .read_text())
    keep = {s["name"]: s["keep_days"] for s in data["systems"]}
    est = data["estimates"]
    print(f"{'a month':18}{'stored':>9}{'queries':>9}{'runs':>9}"
          f"{'compute':>9}{'naive /':>10}")
    print(f"{'':18}{'GB-mo':>9}{'TB read':>9}{'TB read':>9}"
          f"{'core-h':>9}{'designed':>10}")
    for name, u in USAGE.items():
        naive = units(est[name], keep[name], u, designed=False)
        good = units(est[name], keep[name], u, designed=True)
        ratio = naive["core-h"] / good["core-h"]
        for label, m, tail in (("naive", naive, ""),
                               ("designed", good, f"{show(ratio)}x")):
            print(f"{name + ' ' + label:18}" + "".join(
                f"{show(v):>9}" for v in m.values()) + f"{tail:>10}")

    print(f"\n{'connectors':12}{'sources':>8}{'build, days':>18}"
          f"{'run, days/mo':>18}{'rows':>9}")
    print(f"{'':20}{'by hand':>9}{'buy':>9}{'by hand':>9}{'buy':>9}"
          f"{'M/mo':>9}")
    for name, u in USAGE.items():
        (b0, r0), (b1, r1) = people(u, False), people(u, True)
        rows = est[name]["rows a day"] * MONTH / 1e6
        print(f"{name:12}{u.sources:>8}{b0:>9g}{b1:>9g}{r0:>9g}"
              f"{r1:>9g}{show(rows):>9}")


if __name__ == "__main__":
    main()
