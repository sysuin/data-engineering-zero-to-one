# One customer's email and comments under each way of showing less;
# then the masked schema built from staging, and scanned.
import json
from pathlib import Path

from groundwork.govern import classify, masking

from _govern import base

con = base().connect()
email, phone = con.execute("""
    SELECT contact_email, contact_phone FROM raw.relay_accounts
    WHERE load_date = DATE '2026-01-04' AND account_id = '3574'
    """).fetchone()
comments = [b for (b,) in con.execute("""
    SELECT c.body FROM staging.stg_frontline__ticket_comments AS c
    JOIN staging.stg_frontline__tickets AS t USING (ticket_id)
    WHERE t.requester_email = ? AND (c.body LIKE '%555-01%'
       OR c.body LIKE '%.example%')
    ORDER BY c.comment_id""", [email]).fetchall()]

vault = masking.Vault()
shown = {
    "as held": email,
    "redacted": masking.redact(email),
    "masked": masking.mask_email(email),
    "sha-256": masking.plain_hash(email)[:16] + "...",
    "keyed": masking.pseudonym(email),
    "token": vault.token(email),
}
for how, value in shown.items():
    print(f"{how:<10}{value}")
print(f"{'phone':<10}{phone} -> {masking.mask_phone(phone)}")
print("salted twice, same hash? "
      f"{masking.salted_hash(email) == masking.salted_hash(email)}")
print("keyed twice, same?       "
      f"{masking.pseudonym(email) == masking.pseudonym(email)}")
print("keyed, in capitals, same? "
      f"{masking.pseudonym(email) == masking.pseudonym(email.title())}")

print(f"\n{len(comments)} of his comments carry contact details:")
for body in comments:
    print(f"  {masking.redact(body)}")

rows = masking.build(con)
print("\nmasked tables, built from staging:")
for name, n in rows.items():
    print(f"  masked.{name:<32}{n:>7,} rows")
cols = classify.classify(con, ("masked",))
found = sum(c.hits for c in cols)
refs = [c.column for c in cols if c.cls == classify.PERSONAL]
print(f"rows with an email or phone in them: {found}")
print(f"personal columns left: {', '.join(refs[:2])},\n"
      f"  {', '.join(refs[2:])}")

Path(__file__).with_suffix(".json").write_text(json.dumps({
    "email": email, "phone": phone, "comment": comments[0],
    "shown": shown, "comment_redacted": masking.redact(comments[0]),
    "phone_masked": masking.mask_phone(phone)}, indent=1) + "\n")
