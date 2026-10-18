# Look for contact details in Frontline's free text, with the pattern
# people write first and with a stricter one, and check every hit.
import re

from groundwork.govern.classify import EMAIL, LOOSE_PHONE, PHONE, find

from _govern import base

con = base().connect(read_only=True)
comments = con.execute("""
    SELECT c.comment_id, c.body, t.requester_email,
           coalesce(a.contact_phone, '') AS phone
    FROM raw.frontline_ticket_comments AS c
    JOIN raw.frontline_tickets AS t USING (ticket_id)
    LEFT JOIN intermediate.int_account_identity AS i
      ON i.crm_id = CAST(t.requester_account_id AS INTEGER)
    LEFT JOIN raw.relay_accounts AS a
      ON a.load_date = DATE '2026-01-04'
     AND CAST(a.account_id AS INTEGER) = i.account_id
    ORDER BY c.comment_id""").fetchall()
notes = [d for (d,) in con.execute(
    "SELECT description FROM raw.frontline_tickets ORDER BY ticket_id"
).fetchall()]

print(f"{'pattern':<12}{'comments':>10}{'descriptions':>14}")
for label, phone in (("loose phone", LOOSE_PHONE), ("phone", PHONE)):
    rx = re.compile(phone)
    n_c = sum(1 for _, body, *_ in comments if rx.search(body))
    n_d = sum(1 for d in notes if d and rx.search(d))
    print(f"{label:<12}{n_c:>10,}{n_d:>14,}")
rx = re.compile(EMAIL)
n_c = sum(1 for _, b, *_ in comments if rx.search(b))
n_d = sum(1 for d in notes if d and rx.search(d))
print(f"{'email':<12}{n_c:>10,}{n_d:>14,}")

loose = re.compile(LOOSE_PHONE)
sample = next(d for d in notes if d and loose.search(d))
print(f"\na loose hit:\n  {sample!r}")
texts = [b for _, b, *_ in comments] + [d for d in notes if d]
n_loose = sum(1 for t in texts if loose.search(t))
n_true = sum(1 for _, b, _, ph in comments
             if loose.search(b) and ph in b)
print(f"loose phone over all free text: {n_loose:,} flagged, "
      f"{n_true} right ({n_true / n_loose:.1%})")

# Check each strict hit: is it the requester's own email, or the phone
# Relay holds for the requester's account?
flagged = right = 0
kinds = {"email": 0, "phone": 0}
for _, body, email, phone in comments:
    hits = find(body)
    if not hits:
        continue
    flagged += 1
    for kind, value in hits:
        kinds[kind] += 1
    right += all(v == (email if k == "email" else phone)
                 for k, v in hits)
print(f"\ncomments flagged {flagged:,} of {len(comments):,}: "
      f"{kinds['email']} emails, {kinds['phone']} phones")
print(f"hits that are the requester's own: {right:,} of {flagged:,} "
      f"({right / flagged:.1%})")

# The simulator's own check knows the fictional ranges (Appendix F).
known = {cid for (cid,) in con.execute("""
    SELECT comment_id FROM raw.frontline_ticket_comments
    WHERE body LIKE '%555-01%' OR body LIKE '%.example%'""").fetchall()}
mine = {cid for cid, body, *_ in comments if find(body)}
print(f"planted (by the fictional ranges) {len(known):,}; found "
      f"{len(mine & known):,};\n  missed {len(known - mine)}; "
      f"extra {len(mine - known)}")

named = [d for d in notes if d and re.search(
    r"[A-Z][a-z]+ from our account|contact to [A-Z][a-z]+", d)]
print(f"\ndescriptions naming a person: {len(named)}, for example\n"
      f"  {named[0]!r}")
