# A backup restored after the request: the person comes back, the
# suppression list recognises them by keyed hash, and they go again.
# timeout: 600
import shutil
from datetime import datetime

from groundwork.govern import forget, masking

from _govern import base, tidy, work

backup = base()                          # last night's copy, untouched
copy = work("restore")
con = copy.connect()
masking.build(con)
s = forget.subject(con, 3574)
done = forget.erase(con, s, copy.raw, copy.landed)
forget.record(con, "FR-2026-0001", s, datetime(2026, 1, 5, 9, 12),
              datetime(2026, 1, 5, 11, 40), done, {}, "")
print("after the request, rows on the suppression list: "
      f"{sum(forget.resurfaced(con).values())}")

# Wednesday: a disk fails and raw's Relay and Frontline folders are
# restored from Monday night's backup.
for folder in ("relay/accounts", "frontline/tickets"):
    shutil.rmtree(copy.raw / folder)
    shutil.copytree(backup.raw / folder, copy.raw / folder)
print("restored relay/accounts and frontline/tickets from the backup")
back = forget.resurfaced(con)
for relation, n in back.items():
    print(f"  {relation:<24}{n:>4} rows came back")

again = forget.reapply(con, copy.raw, copy.landed)
redacted = sum(p.rows for p in again if p.layer == "raw")
print(f"forgotten again: {redacted} raw rows redacted")
print(f"on the list now: {sum(forget.resurfaced(con).values())}")
(ref,) = con.execute(
    "SELECT ref FROM audit.forget_suppress WHERE kind = 'email'"
).fetchone()
print(f"what the list holds for the email: {ref}")
con.close()
tidy("restore")
