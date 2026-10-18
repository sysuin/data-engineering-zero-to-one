# Exercise 2: a contract for raw.frontline_ticket_comments, written
# from what the table holds, and the project bound with it.
import shutil

import duckdb
import yaml
from _sandbox import HERE, base

from groundwork import contracts
from groundwork.testing import bind_project, contract_for

con = duckdb.connect(str(base()), read_only=True)
cols = con.execute(
    "SELECT column_name, data_type FROM information_schema.columns "
    "WHERE table_schema = 'raw' AND table_name = "
    "'frontline_ticket_comments' AND column_name NOT LIKE '\\_%' "
    "ESCAPE '\\' AND column_name <> 'load_date' "
    "ORDER BY ordinal_position").fetchall()
empty = {c: con.execute(
    f'SELECT count(*) FILTER (WHERE "{c}" IS NULL), count(*) '
    f"FROM raw.frontline_ticket_comments").fetchone() for c, _ in cols}
con.close()
types = {"BIGINT": "integer", "BOOLEAN": "boolean", "VARCHAR": "text"}
doc = {"table": "frontline.ticket_comments", "version": 1,
       "description": "One row per comment on a ticket, as the desk's "
                      "API sends it, fetched per ticket.",
       "producer": {"who": "Frontline, the support desk (its API)"},
       "owner": "data engineering (Groundwork ingest)",
       "grain": "one row per comment per pull", "key": ["comment_id"],
       "columns": {}, "policy": {"bad_rows": "fail"},
       "changes": [{"version": 1, "note": "first written"}]}
for c, t in cols:
    spec = {"type": types[t]}
    if empty[c][0]:
        spec["nullable"] = True
    if c == "created_at":
        spec.update(type="timestamp", format="%Y-%m-%dT%H:%M:%SZ",
                    unit="UTC")
    doc["columns"][c] = spec
folder = HERE / "contracts-comments"
shutil.rmtree(folder, ignore_errors=True)
shutil.copytree(contracts.HERE, folder,
                ignore=shutil.ignore_patterns("*.py", "__pycache__"))
(folder / "frontline.ticket_comments.yaml").write_text(
    yaml.safe_dump(doc, sort_keys=False))
print(yaml.safe_dump(doc["columns"], sort_keys=False).rstrip())
for c, (nulls, n) in empty.items():
    print(f"{c:<12} empty in {nulls:,} of {n:,} rows")
print(f"contract for the table: "
      f"{contract_for('frontline_ticket_comments', folder)}")
con = duckdb.connect()
got = bind_project(con, folder=folder)
con.close()
print(f"bound with no uncontracted tables: "
      f"{sum(e is None for e in got.values())} of {len(got)}")
shutil.rmtree(folder)
