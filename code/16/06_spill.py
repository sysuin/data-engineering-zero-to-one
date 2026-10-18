# nondeterministic: timings and spill sizes vary by machine and load
# timeout: 900
# A sort and an aggregate from Groundwork, run inside a memory limit.
import shutil
import time

import duckdb
from _plans import HERE, analyze, model, tidy, warehouse

TEMP = HERE / "spill"                      # spill files, deleted below
JOBS = {                                   # job: (SQL, memory limit)
    "sort the lines by day": (
        "SELECT * FROM marts.fct_order_lines "
        "ORDER BY order_date, order_id, line_no", "250MB"),
    "fct_orders' GROUP BY": (model("fct_orders"), "120MB"),
}


def run(sql, limit, spill):
    """One job in a fresh connection; its result written to a file."""
    shutil.rmtree(TEMP, ignore_errors=True)
    con = duckdb.connect(config={
        "threads": 4, "memory_limit": limit,
        "temp_directory": str(TEMP) if spill else ""})
    con.execute(f"ATTACH '{warehouse()}' AS warehouse (READ_ONLY)")
    con.execute("USE warehouse")
    start = time.perf_counter()
    try:
        profile = analyze(con, f"COPY ({sql}) TO "
                               f"'{HERE / 'result.parquet'}'")
    except duckdb.OutOfMemoryException:
        return "out of memory", None, None
    finally:
        con.close()
    return ("finished", time.perf_counter() - start,
            profile["system_peak_temp_dir_size"])


print(f"{'job':<21}{'limit':>7}{'spill':>6}  {'outcome':<14}"
      f"{'seconds':>8}{'spilled':>9}")
for job, (sql, limit) in JOBS.items():
    seconds = {}
    for cap, spill in (("2GB", True), (limit, False), (limit, True)):
        outcome, took, spilled = run(sql, cap, spill)
        seconds[cap, spill] = took
        shown = (f"{took:>8.1f}{spilled / 2**20:>6.0f} MB" if took
                 else "")
        print(f"{job if cap == '2GB' else '':<21}{cap:>7}"
              f"{'on' if spill else 'off':>6}  {outcome:<14}{shown}"
              .rstrip())
    if seconds[limit, True]:
        print(f"{'':<21}spilling slower than not: "
              f"{seconds[limit, True] > seconds['2GB', True]}")
tidy("spill", "result.parquet", "tmp_result.parquet")
