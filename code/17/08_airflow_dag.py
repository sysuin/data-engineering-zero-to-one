# compile-only: needs Airflow, its scheduler and its metadata database
# Groundwork's nightly DAG in Airflow: the same tasks, the same edges.
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.timetables.interval import CronDataIntervalTimetable

SOURCES = ("relay", "tradeline", "frontline", "suppliers",
           "catalogue", "ledger", "reference")


def ingest(source, ds, data_interval_end, **_):
    """Chapter 10's ingest for one source and the logical date."""
    from groundwork.ingest.__main__ import BAD, run
    lines = run(ds, run_at=data_interval_end, only=[source])
    bad = [x for x in lines if x.status in BAD]
    if any(x.status == "broken" for x in bad):
        raise AirflowFailException(bad[0].detail)    # no retry
    if bad:
        raise RuntimeError(bad[0].detail)            # retried


def comments(ds, data_interval_end, **_):
    from groundwork.ingest import frontline_comments
    from meridian.sources import Clock, frontline
    base = frontline.serve(Clock(data_interval_end))
    frontline_comments.load(ds, run_at=data_interval_end, base=base)


# Chapter 15's checks judge the night as of 07:00 on D+1, when it falls
# due: the end of the interval plus an hour.
AT = ("{{ (data_interval_end + macros.timedelta(hours=1))"
      ".strftime('%Y-%m-%dT%H:%M') }}")


def publish(ds, run_id, **_):
    print(f"published {ds} by {run_id}")     # audit.published, here


with DAG(
    dag_id="groundwork_nightly",
    # The interval 06:00 D to 06:00 D+1, run when it ends: {{ ds }}
    # is D. Say so: Airflow 3's plain cron schedule runs with ds
    # equal to the run's own date instead.
    schedule=CronDataIntervalTimetable("0 6 * * *", timezone="UTC"),
    start_date=datetime(2022, 1, 1),
    catchup=False,                  # backfills are Chapter 18's
    max_active_runs=1,              # one night at a time
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(hours=1),
        "execution_timeout": timedelta(hours=1),
    },
) as dag:
    wait_relay = FileSensor(
        task_id="wait_relay",
        filepath="relay/{{ ds }}/contracts.csv",
        fs_conn_id="meridian_landing",       # its root is LANDING
        poke_interval=600, timeout=6 * 3600, mode="reschedule",
        retries=0)
    loads = {s: PythonOperator(task_id=f"ingest_{s}",
                               python_callable=ingest,
                               op_kwargs={"source": s})
             for s in SOURCES}
    load_comments = PythonOperator(task_id="load_comments",
                                   python_callable=comments)
    check_contracts = BashOperator(
        task_id="check_contracts", retries=0,
        bash_command="python -m groundwork.contracts.check "
                     "--load-date {{ ds }}")
    check_quality = BashOperator(
        task_id="check_quality", retries=0,
        bash_command=f"python -m groundwork.quality --at {AT}")
    vars_ = json.dumps({"night": "{{ ds }}", "since":
                        "{{ prev_data_interval_end_success or '' }}"})
    dbt_build = BashOperator(
        task_id="dbt_build", retries=1,
        cwd="/srv/groundwork/dbt/groundwork",
        bash_command=f"dbt build --target prod --vars '{vars_}'")
    reconcile = BashOperator(
        task_id="reconcile", retries=0,
        bash_command=f"python -m groundwork.reconcile --at {AT}")
    publish_ = PythonOperator(task_id="publish",
                              python_callable=publish, retries=0)

    wait_relay >> loads["relay"]
    loads["frontline"] >> load_comments
    list(loads.values()) >> check_contracts
    list(loads.values()) >> check_quality
    [check_contracts, check_quality, load_comments] >> dbt_build
    dbt_build >> reconcile >> publish_
