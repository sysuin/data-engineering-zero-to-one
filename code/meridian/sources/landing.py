"""
The landing zone: the folder other teams' files arrive in.

Each landed file's modification time is the moment it arrived, which is how a pipeline
discovers what is new. `arrived()` lists what had arrived by a moment; `stage()` copies
it into a sandbox folder, so a listing can play a pipeline forward day by day and see
only what that day could have seen.

Relay's nightly exports are the exception: only the last fortnight is on disk, and older
nights are written on demand by `meridian.sources.relay.export`.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path

from meridian.sources import LANDING, parse_moment


def arrived(by=None, under: str = "", root=LANDING) -> list[Path]:
    """Every file under root/under whose arrival is at or before `by`, oldest first."""
    base = Path(root) / under
    limit = parse_moment(by).timestamp() if by is not None else None
    found = []
    for dirpath, _, files in os.walk(base):
        for f in files:
            p = Path(dirpath) / f
            t = p.stat().st_mtime
            if limit is None or t <= limit:
                found.append((t, str(p), p))
    return [p for _, _, p in sorted(found)]


def arrival(path) -> float:
    return Path(path).stat().st_mtime


def stage(by, dest, under: str = "", root=LANDING) -> list[Path]:
    """Copy what had arrived by `by` into dest, keeping paths and arrival times."""
    out = []
    for p in arrived(by, under, root):
        target = Path(dest) / p.relative_to(root)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            shutil.copy2(p, target)
        out.append(target)
    return out
