"""
Chapter 18's sandboxes. Nothing here touches data/groundwork.

    today(name)    Groundwork as it stands, in work/NAME: the warehouse
                   copied, raw's files hard-linked (drill.as_of)
    quarter()      an empty Groundwork backfilled from 16 December 2024
                   to the night of 16 March 2025, built and reconciled
                   as of 06:00 on 17 March: listing 03 makes it, and
                   the listings after it read it (made here if missing)
    tidy(name)     delete work/NAME

Not printed in the book.
"""
import shutil
from dataclasses import replace
from datetime import date

from groundwork.config import SANDBOX
from groundwork.orchestrate import backfill, drill, runner
from meridian.sources import relay

HERE = SANDBOX / "ch18"
QUARTER = HERE / "quarter"
FIRST, LAST = date(2024, 12, 16), date(2025, 3, 16)
STAMP = QUARTER / "BUILT"


def today(name: str) -> runner.Env:
    return drill.as_of(runner.Env(), date(2026, 1, 4),
                       HERE / "work" / name)


def tidy(name: str) -> None:
    shutil.rmtree(HERE / "work" / name, ignore_errors=True)


def quarter_env() -> runner.Env:
    return backfill.sandbox_env(QUARTER)


OLD_CRM = date(2024, 2, 29)            # the old CRM's last export


def start_quarter() -> runner.Env:
    """An empty Groundwork for the quarter, holding what the real one
    held before its first night: the old CRM's last export (Chapter 9
    loaded it; Chapter 12 resolves 212 companies with it) and the
    catalogue, which has no past (one file, November 2025)."""
    shutil.rmtree(QUARTER, ignore_errors=True)
    QUARTER.mkdir(parents=True)
    env = quarter_env()
    seed = QUARTER / "seed"
    relay.export(OLD_CRM, seed / "relay")
    lines = backfill.load_relay(replace(env, landing=seed), OLD_CRM,
                                backfill.due(OLD_CRM))
    backfill.load_as_it_stands(env, FIRST, ["catalogue"])
    shutil.rmtree(seed)
    return env, lines


def replay(parallel: int = 1) -> backfill.Replay:
    return backfill.Replay(QUARTER / "replay", parallel)


def finished(b: backfill.Backfill) -> None:
    """Mark the quarter built, and drop the replayed sources."""
    if b.state == "succeeded":
        shutil.rmtree(QUARTER / "replay", ignore_errors=True)
        STAMP.write_text(f"{b.first} {b.last}\n")


def quarter() -> runner.Env:
    """The backfilled quarter, made the way listing 03 makes it (without
    the crash) when it is not there."""
    if not STAMP.exists():
        env, _ = start_quarter()
        b = backfill.run(FIRST, LAST, env=env, replay=replay())
        finished(b)
        if b.state != "succeeded":
            raise RuntimeError(b.report())
    return quarter_env()


def copy_of_quarter(name: str) -> runner.Env:
    """A scratch copy of the quarter that a listing may write to."""
    return drill.as_of(quarter(), LAST, HERE / "work" / name)
