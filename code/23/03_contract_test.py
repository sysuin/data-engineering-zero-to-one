# The project bound, not built, over empty raw tables made from the
# contracts; then Relay renames a column, and the test says who breaks.
import shutil
import sys
import time

import duckdb
import yaml
from _sandbox import HERE

from groundwork import contracts
from groundwork.testing import bind_project

sys.path.insert(0, "tests")
from test_boundaries import NO_CONTRACT  # noqa: E402

start = time.perf_counter()
con = duckdb.connect()
got = bind_project(con, uncontracted=NO_CONTRACT)
seconds = time.perf_counter() - start
rows = sum(con.execute(f"SELECT count(*) FROM {s}.{t}").fetchone()[0]
           for s, t in con.execute(
               "SELECT table_schema, table_name FROM "
               "information_schema.tables WHERE table_schema = 'raw'"
           ).fetchall())
con.close()
print("every model created as a view over the contracts' empty tables:")
print(f"  {sum(e is None for e in got.values())} of {len(got)} bind; "
      f"raw rows read: {rows}; under five seconds: {seconds < 5}")
print(f"  raw tables read with no contract: {', '.join(NO_CONTRACT)}")

# Relay's administrator renames a field, and a new contract says so.
folder = HERE / "contracts"
shutil.rmtree(folder, ignore_errors=True)
shutil.copytree(contracts.HERE, folder,
                ignore=shutil.ignore_patterns("*.py", "__pycache__"))
path = folder / "relay.contracts.yaml"
doc = yaml.safe_load(path.read_text())
doc["columns"] = {("discount" if k == "discount_pct" else k): v
                  for k, v in doc["columns"].items()}
doc["formats"]["relay"] = ["discount" if h == "discount_pct" else h
                           for h in doc["formats"]["relay"]]
path.write_text(yaml.safe_dump(doc, sort_keys=False))

old = contracts.load("relay.contracts")
new = contracts.load("relay.contracts", folder)
print("\nrelay.contracts, discount_pct renamed discount:")
print("  compatible(old, new):")
for who, what in contracts.compatible(old, new):
    print(f"    {who:<10} {what}")
con = duckdb.connect()
after = bind_project(con, uncontracted=NO_CONTRACT, folder=folder)
con.close()
print("  models that no longer bind:")
for model, error in sorted(after.items(), key=lambda kv: kv[1] or ""):
    if error:
        why = error.removeprefix("Binder Error: ")
        print(f"    {model}: {why}" if len(why) < 40
              else f"    {model}:\n      {why}")
print(f"  version {old.version} -> {new.version}: a change that can "
      f"break consumers needs\n  a new version and a note, or the "
      f"contract test fails")
shutil.rmtree(folder)
