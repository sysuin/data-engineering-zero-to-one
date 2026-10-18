# A request to be forgotten, end to end, on a copy of Groundwork: who
# the person is, where they are, erase, look again, and the audit row.
# timeout: 900
import json
from datetime import datetime
from pathlib import Path

from groundwork.config import LANDING
from groundwork.govern import forget, masking

from _govern import tidy, work

copy = work("forget")
con = copy.connect()
masking.build(con)                       # as the nightly run leaves it
s = forget.subject(con, 3574)
print(f"contact of account {s.account_id}: CRM ids {s.crm_ids}, "
      f"names {len(s.names)} ways,\n  1 email, 1 phone, "
      f"{len(s.user_ids)} portal login, {len(s.tickets)} tickets")

email_only = {k: v for k, v in forget.sweep(
    con, s, copy.raw, copy.landed, kinds=("strong",)).items()
    if "email" in v}
places = forget.find(con, s, copy.raw, copy.landed)
print(f"\nsearching by email alone: {len(email_only)} places, "
      f"{sum(v['email'] for v in email_only.values())} rows or files")
print(f"{'tied to them':<46}{'rows':>6}  then")
for p in places:
    print(f"  {p.location:<44}{p.rows:>6}  {p.action}")
legacy = [r for r in con.execute(
    f"SELECT contact_name, contact_email FROM raw.relay_accounts WHERE"
    f" account_id = '{s.crm_ids[-1]}'").fetchall()]
print(f"the old CRM's record, id {s.crm_ids[-1]}: {legacy[0][0]!r}, "
      f"email {legacy[0][1]!r}")

before = forget.sweep(con, s, copy.raw, copy.landed)
received = datetime(2026, 1, 5, 9, 12)
done = forget.erase(con, s, copy.raw, copy.landed)
after = forget.sweep(con, s, copy.raw, copy.landed)
again = forget.find(con, s, copy.raw, copy.landed)


def total(found, kind):
    return sum(v.get(kind, 0) for v in found.values())


print(f"\n{'identifier':<14}{'before':>8}{'after':>8}")
for kind in ("email", "portal login", "phone", "name"):
    b, a = total(before, kind), total(after, kind)
    print(f"{kind:<14}{b:>8,}{a:>8,}")
print(f"tied rows still holding anything: {sum(p.rows for p in again)}")
phone_others, name_others = con.execute(f"""
    SELECT count(*) FILTER (WHERE contact_phone = '{s.phone}'),
           count(*) FILTER (WHERE contact_name = '{s.names[0]}')
    FROM raw.relay_accounts WHERE load_date = DATE '2026-01-04'
    """).fetchone()
print(f"other contacts with his phone: {phone_others}; "
      f"with his name: {name_others}")

elsewhere = forget.outside(s, [LANDING / "relay", LANDING / "portal"])
note = "; ".join(f"{Path(k).name}: {n} files" for k, n in
                 elsewhere.items())
print(f"still in the sources' own files: {note}")
forget.record(con, "FR-2026-0001", s, received,
              datetime(2026, 1, 5, 11, 40), done, after, note)
row = con.execute("SELECT * FROM audit.forget_requests").fetchone()
cols = [d[0] for d in con.description]
print("\naudit.forget_requests:")
for c, v in zip(cols, row):
    print(f"  {c:<14}{v}")
(n,) = con.execute(
    "SELECT count(*) FROM audit.forget_suppress").fetchone()
print(f"suppression list: {n} keyed hashes")

Path(__file__).with_suffix(".json").write_text(json.dumps({
    "places": [[p.layer, p.location, p.rows, p.action] for p in done],
    "before": before, "after": after, "outside": elsewhere,
    "suppress": n},
    indent=1, default=str) + "\n")
con.close()
tidy("forget")
