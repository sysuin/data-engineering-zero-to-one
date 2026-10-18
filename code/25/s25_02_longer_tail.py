# Exercise 25.4: a fleet whose tail is ten times Meridian's mobile one.
import dataclasses
import importlib.util
import json
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    "cost", "code/25/02_cost_model.py")
cost = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cost)

data = json.loads(Path("code/25/01_capacity_estimate.json").read_text())
est = data["estimates"]["IoT"]
# Ten times longer: what Meridian's mobile events had left after 3
# days, the fleet has left after 30.
for days in (3, 30):
    u = dataclasses.replace(cost.USAGE["IoT"], days_run=days)
    m = cost.units(est, 90, u, designed=True)
    print(f"rebuild {days:>2} days: runs read "
          f"{cost.show(m['runs TB'])} TB, "
          f"compute {cost.show(m['core-h'])} core-h a month")
