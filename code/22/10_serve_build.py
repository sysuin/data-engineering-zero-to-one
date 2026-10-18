# The Build's command, run against a copy of the warehouse: publish
# the metrics schema and build the feature table, checked.
# timeout: 300
from groundwork.serve.__main__ import main

from _serve import tidy, warehouse

code = main(["--warehouse", str(warehouse())])
print(f"exit {code}")
tidy("warehouse.duckdb")
