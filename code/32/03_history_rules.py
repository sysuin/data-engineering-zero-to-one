# Book 2's account history, as a CRM could have recorded it.
# timeout: 300
import sqlite3
import sys

from groundwork.config import BOOK2_WAREHOUSE, ROOT, SOURCES

sys.path.insert(0, str(ROOT / "code" / "meridian"))
import verify_sources as v  # noqa: E402

b2 = sqlite3.connect(f"file:{BOOK2_WAREHOUSE}?mode=ro", uri=True)
rows = b2.execute("SELECT * FROM account_history "
                  "ORDER BY account_id, valid_from, rowid").fetchall()
now = b2.execute("SELECT account_id, segment, account_manager "
                 "FROM accounts").fetchall()
expected, notes = v.book2_history_as_a_crm_saw_it(rows, now)
ours = set(v.rebuild(str(SOURCES))["history"])

print(f"Book 2's history rows            {len(rows):>6,}")
for what, n in sorted(notes.items()):
    print(f"  {what:<31}{n:>6,}")
print(f"as a CRM could have recorded it  {len(expected):>6,}")
print(f"rebuilt from Relay's audit       {len(ours):>6,}")
print(f"rows that differ                 {len(ours ^ expected):>6,}")
