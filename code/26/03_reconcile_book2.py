# Groundwork's compat schema against Book 2's warehouse, table by table,
# every row and every value; then the account history under Appendix
# F's four rules.
import json
from pathlib import Path

from _sandbox import connect

from groundwork import reconcile_books as rb
from groundwork.config import BOOK2_WAREHOUSE

con = connect()
tables = rb.book2(con)
history, notes = rb.history(con)
con.close()

print(f"compat against {BOOK2_WAREHOUSE.name}, every row and value")
print(f"{'table':<16}{'Book 2':>12}{'Groundwork':>12}{'differing':>11}")
for x in tables:
    print(f"{x.item:<16}{x.theirs:>12,}{x.ours:>12,}{x.differing:>11,}")
cells = sum(x.theirs for x in tables)
print(f"{len(tables)} relations, {cells:,} rows a side, "
      f"{sum(x.differing for x in tables)} differing")

print(f"\naccount_history: Book 2 {history.theirs:,} rows, "
      f"Groundwork {history.ours:,}")
for rule, n in notes.items():
    print(f"  {rule:<36}{n:>6,}")
print(f"  {'differing from Groundwork':<36}{history.differing:>6,}")

Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"tables": [[x.item, x.theirs, x.ours, x.differing]
                for x in tables],
     "history": [history.theirs, history.ours, history.differing],
     "rules": notes}))
