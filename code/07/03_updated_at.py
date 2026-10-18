# The same thirty nights with the watermark on updated_at, beside the
# created_at loader, and how Tradeline answers each loader's query.
import json
from pathlib import Path

from _watermark import Loader, compare, nights

loaders = {"created_at": Loader(column="created_at"),
           "updated_at": Loader(column="updated_at")}
marks = {}                     # each night's marks, for the figure
for night, moment, src in nights():
    for loader in loaders.values():
        loader.run(night, src)
    marks[str(moment)] = {c: x.mark["order_lines"]
                          for c, x in loaders.items()}
    if night == 30:
        plans = {c: src.execute(f"EXPLAIN QUERY PLAN SELECT * FROM "
                                f"orders WHERE {c} > ?", ("",)
                                ).fetchall()[0][3] for c in loaders}
        log = src.execute("SELECT committed_at, row_json FROM "
                          "replication_log WHERE table_name = "
                          "'order_lines' ORDER BY lsn").fetchall()

found = {c: compare(x) for c, x in loaders.items()}
print(f"{'watermark on':<26} {'created_at':>11} {'updated_at':>11}")
print(f"{'rows read in all':<26}", *(
    f"{x.rows_read:>11,}" for x in loaders.values()))
print(f"{'orders that differ':<26}", *(
    f"{len(w):>11,}" for w, _ in found.values()))
for kind in found["created_at"][1]:
    print(f"  {kind:<24}", *(f"{k[kind]:>11,}" for _, k in
                             found.values()))
for column, plan in plans.items():
    print(f"how Tradeline finds {column} > ?:\n  {plan}")

# An order the created_at copy has wrong and the updated_at copy right
right = {w[0] for w in found["updated_at"][0]}
oid = max(w[0] for w in found["created_at"][0]
          if w[1] and w[2] == w[3] and w[0] not in right)
ours = loaders["created_at"].db
line = ours.execute(
    "SELECT c.line_no, c.qty, s.qty, s.created_at, s.updated_at "
    "FROM copy_order_lines c JOIN src.order_lines s USING (order_id, "
    "line_no) WHERE order_id = ? AND c.qty <> s.qty", [oid]).fetchone()
no, before, after, created, updated = line
committed = next(at for at, row in log
                 if json.loads(row)["order_id"] == oid
                 and json.loads(row)["updated_at"] == updated)
print(f"order {oid}, line {no}: created {created}")
print(f"  qty {before} -> {after}, updated_at {updated}, "
      f"committed {committed[11:]}")
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"order": oid, "line": no, "created": created, "updated": updated,
     "committed": committed, "qty": [before, after], "marks": marks},
    indent=1) + "\n")
