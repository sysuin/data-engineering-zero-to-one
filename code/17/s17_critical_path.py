# Exercise 3: the nightly DAG one task at a time and all at once, with
# illustrative durations, and the chain that decides the second.
from groundwork.orchestrate.dag import nightly

MINUTES = {"wait_relay": 0, "ingest_tradeline": 2,
           "ingest_frontline": 3,
           "ingest_suppliers": 1, "ingest_catalogue": 1,
           "ingest_ledger": 1, "ingest_reference": 1, "ingest_relay": 1,
           "load_comments": 3, "check_contracts": 6, "check_quality": 1,
           "dbt_build": 3, "reconcile": 2, "publish": 1}

dag = nightly()
print(f"one at a time: {sum(MINUTES.values())} min")
end, via = {}, {}
for name in dag.order():               # earliest finish of each task
    ups = dag.tasks[name].upstream
    start = max((end[u] for u in ups), default=0)
    via[name] = max(ups, key=lambda u: end[u]) if ups else None
    end[name] = start + MINUTES[name]
last = max(end, key=end.get)
print(f"all at once:   {end[last]} min")
chain, n = [], last
while n:
    chain.append(n)
    n = via[n]
print("the critical path, first to last:")
for n in reversed(chain):
    print(f"  {n:<17}{MINUTES[n]:>2} min, done at {end[n]:>2}")
