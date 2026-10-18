# compile-only: needs Dagster, its daemon and its storage
# The nightly as Dagster assets: partitioned by logical date, built
# when a sensor sees Relay's export rather than at a fixed time.
import json
import subprocess
from datetime import timedelta

from dagster import (AssetExecutionContext, Backoff,
                     DailyPartitionsDefinition, Definitions, Failure,
                     RetryPolicy, RunRequest, SensorEvaluationContext,
                     asset, define_asset_job, sensor)

from groundwork.config import LANDING

DAYS = DailyPartitionsDefinition(start_date="2022-01-01",
                                 timezone="UTC")
RETRY = RetryPolicy(max_retries=2, delay=300,
                    backoff=Backoff.EXPONENTIAL)
SOURCES = ("relay", "tradeline", "frontline", "suppliers",
           "catalogue", "ledger", "reference")


def moment(context, hours):
    """The partition's day ends at midnight; the run is due at 06:00
    the next morning, and the checks judge it as of 07:00."""
    return context.partition_time_window.end + timedelta(hours=hours)


def make_raw(source):
    @asset(name=f"raw_{source}", partitions_def=DAYS,
           retry_policy=RETRY, group_name="raw")
    def raw(context: AssetExecutionContext) -> None:
        from groundwork.ingest.__main__ import BAD, run
        lines = run(context.partition_key, only=[source])
        bad = [x for x in lines if x.status in BAD]
        if bad:
            raise Failure(bad[0].detail,     # a broken contract is
                          allow_retries=bad[0].status != "broken")
    return raw


raws = [make_raw(s) for s in SOURCES]


@asset(partitions_def=DAYS, deps=["raw_frontline"],
       retry_policy=RETRY, group_name="raw")
def raw_ticket_comments(context: AssetExecutionContext) -> None:
    from groundwork.ingest import frontline_comments
    from meridian.sources import Clock, frontline
    base = frontline.serve(Clock(moment(context, 6)))
    frontline_comments.load(context.partition_key,
                            run_at=moment(context, 6), base=base)


@asset(partitions_def=DAYS, deps=[f"raw_{s}" for s in SOURCES])
def contracts_held(context: AssetExecutionContext) -> None:
    done = subprocess.run(["python", "-m", "groundwork.contracts.check",
                           "--load-date", context.partition_key])
    if done.returncode:
        raise Failure("a contract broke", allow_retries=False)


@asset(partitions_def=DAYS, deps=[f"raw_{s}" for s in SOURCES])
def loads_checked(context: AssetExecutionContext) -> None:
    at = f"{moment(context, 7):%Y-%m-%dT%H:%M}"
    done = subprocess.run(["python", "-m", "groundwork.quality",
                           "--at", at])
    if done.returncode:
        raise Failure("a load check failed", allow_retries=False)


@asset(partitions_def=DAYS, deps=["contracts_held", "loads_checked",
                                  "raw_ticket_comments"],
       retry_policy=RetryPolicy(max_retries=1, delay=600))
def marts(context: AssetExecutionContext) -> None:
    night = context.partition_key
    subprocess.run(["dbt", "build", "--target", "prod", "--vars",
                    json.dumps({"night": night})],
                   cwd="/srv/groundwork/dbt/groundwork", check=True)


@asset(partitions_def=DAYS, deps=["marts"])
def reconciled(context: AssetExecutionContext) -> None:
    at = f"{moment(context, 7):%Y-%m-%dT%H:%M}"
    done = subprocess.run(["python", "-m", "groundwork.reconcile",
                           "--at", at])
    if done.returncode:
        raise Failure("the warehouse does not tie", allow_retries=False)


nightly = define_asset_job("nightly", partitions_def=DAYS,
                           tags={"dagster/max_runtime": 4 * 3600})


@sensor(job=nightly, minimum_interval_seconds=600)
def relay_export(context: SensorEvaluationContext):
    """A run for night D as soon as D's export has landed."""
    for night in DAYS.get_partition_keys()[-3:]:
        folder = LANDING / "relay" / night
        if (folder / "contracts.csv").exists():
            yield RunRequest(run_key=night, partition_key=night)


defs = Definitions(assets=[*raws, raw_ticket_comments, contracts_held,
                           loads_checked, marts, reconciled],
                   jobs=[nightly],
                   sensors=[relay_export])
