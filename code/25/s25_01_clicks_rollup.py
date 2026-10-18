# Exercise 25.3: the clickstream's dashboards read a sessions table.
import dataclasses
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "cost", "code/25/02_cost_model.py")
cost = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cost)

data = json.loads(Path("code/25/01_capacity_estimate.json").read_text())
est = data["estimates"]["Clicks"]
before = cost.USAGE["Clicks"]
after = dataclasses.replace(before, rollup=1 / 12)  # twelve events a
                                                    # session
for label, u in (("raw events", before), ("sessions", after)):
    m = cost.units(est, 730, u, designed=True)
    print(f"{label:12}" + "".join(
        f"{k} {cost.show(v):>7}   " for k, v in m.items()
        if k != "stored GB-mo"))
