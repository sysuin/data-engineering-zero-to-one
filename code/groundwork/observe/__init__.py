"""
Groundwork's observability (Chapter 21): the four signals measured on
every night, lineage from each column back to its source, and alerts
that a person reads.

    metrics.py   per-night signals from the audit tables
    lineage.py   table and column lineage, and impact
    alerts.py    the watch: rules, routing, dedup, the freshness SLO

The monitors join the nightly DAG through Chapter 17's `checks` hook,
after the build and before publish, so a night whose numbers look
wrong is not announced. The watch is not in the DAG: it runs on its
own an hour after the night is due, and reads only what the night
recorded, so that it still runs when the night did not.

    nightly()      Chapter 17's nightly DAG with the monitor task added
    monitor(ctx)   that task: measure, record, stop publish on an error
"""
from __future__ import annotations

from groundwork.orchestrate.dag import DAG, Fatal, Task, judged_at
from groundwork.orchestrate.dag import nightly as _nightly


def monitor(ctx) -> str:
    """The night's four signals, recorded in audit.metrics. An alarm at
    severity error stops publish; a warning is left to the watch. The
    published day's freshness is the watch's to judge: this task runs
    before the night publishes, so it would always find it a day
    behind."""
    import duckdb

    from groundwork.observe import metrics
    at = judged_at(ctx)
    con = duckdb.connect(str(ctx.env.warehouse))
    try:
        signals = [s for s in metrics.collect(con, ctx.day, at,
                                              ctx.env.root)
                   if s.subject != "published"]
        metrics.record(con, signals, ctx.day, at)
    finally:
        con.close()
    errors = [s for s in signals if s.alarm and s.severity == "error"]
    if errors:
        raise Fatal("; ".join(f"{s.signal} {s.subject}: {s.measure} "
                              f"{s.value}" for s in errors[:3]))
    alarms = sum(s.alarm for s in signals)
    return f"{len(signals)} signals, {alarms} in alarm"


MONITORS = (Task("observe", monitor, timeout=900, isolate=True),)


def nightly() -> DAG:
    """The nightly DAG, observed: Chapter 17's, with the monitors."""
    return _nightly(checks=MONITORS)
