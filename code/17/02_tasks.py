# Groundwork's night as a graph: the tasks, what each waits for, the
# order that follows, and the two mistakes a graph refuses to hold.
import json
import textwrap
from dataclasses import replace
from pathlib import Path

from groundwork.orchestrate.dag import DAG, CycleError, nightly

dag = nightly()
lv = dag.levels()
print(f"{dag.name}: {len(dag.tasks)} tasks, {len(dag.edges())} edges, "
      f"{max(lv.values()) + 1} levels")
for level in range(max(lv.values()) + 1):
    names = [n for n in dag.order() if lv[n] == level]
    print(f"level {level}: {len(names)} task{'s' * (len(names) > 1)}")
    for n in names:
        t = dag.tasks[n]
        ups = ", ".join(t.upstream) if len(t.upstream) < 3 else \
            f"{len(t.upstream)} tasks"
        kind = "sensor" if t.sensor else f"retries {t.retries}"
        print(f"  {n:<17} {kind:<10} after {ups or '-'}")

print("\nwhat waits for ingest_frontline:")
after = ", ".join(sorted(dag.downstream(["ingest_frontline"])))
print(textwrap.fill(after, 66, initial_indent="  ",
                    subsequent_indent="  "))

# A typo in a name, and a cycle: both refused before anything runs.
tasks = list(dag.tasks.values())
bad = [replace(t, upstream=("check_contract",))
       if t.name == "dbt_build" else t for t in tasks]
try:
    DAG("typo", bad)
except ValueError as e:
    print(f"\nValueError: {e}")
loop = [replace(t, upstream=(*t.upstream, "publish"))
        if t.name == "ingest_relay" else t for t in tasks]
try:
    DAG("loop", loop)
except CycleError as e:
    head, names = str(e).split(": ", 1)
    print(f"CycleError: {head}:")
    print(textwrap.fill(names, 66, initial_indent="  ",
                        subsequent_indent="  "))

Path(__file__).with_suffix(".json").write_text(json.dumps({
    "levels": lv, "order": dag.order(), "edges": dag.edges(),
    "tasks": {n: {"sensor": t.sensor, "retries": t.retries,
                  "timeout": t.timeout, "isolate": t.isolate}
              for n, t in dag.tasks.items()}}, indent=0))
