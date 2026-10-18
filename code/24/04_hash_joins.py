# Which way of hiding an email still joins tickets to the CRM, and
# which can be undone by anyone holding a list of addresses?
import json
from pathlib import Path

from groundwork.govern import masking

from _govern import base

con = base().connect(read_only=True)
tickets = [e for (e,) in con.execute(
    "SELECT requester_email FROM staging.stg_frontline__tickets"
    " ORDER BY ticket_id").fetchall()]
contacts = [e for (e,) in con.execute("""
    SELECT contact_email FROM staging.stg_relay__accounts
    WHERE snapshot_date = DATE '2026-01-04' ORDER BY account_id
    """).fetchall()]
people = sorted(set(tickets))
print(f"{len(tickets):,} tickets from {len(people):,} people; "
      f"the CRM lists {len(contacts):,} contacts")

# A list of addresses from somewhere else: here, the CRM's own.
guesses = sorted(set(contacts))
vault = masking.Vault()
ways = {
    "plain text": lambda v: masking.normalise(v),
    "sha-256": masking.plain_hash,
    "sha-256 salted": masking.salted_hash,
    "keyed (hmac)": masking.pseudonym,
    "token (vault)": vault.token,
}
table = {}
print(f"\n{'hidden as':<16}{'tickets joined':>15}"
      f"{'people undone':>15}")
for how, hide in ways.items():
    crm = {hide(e) for e in contacts}
    joined = sum(1 for e in tickets if hide(e) in crm)
    hidden = {hide(e) for e in people}
    if how == "keyed (hmac)":
        # The attacker hashes the list, but has no key: use a guess.
        tried = {masking.pseudonym(g, b"a guessed key")
                 for g in guesses}
    elif how == "token (vault)":
        tried = set()                      # tokens are not computed
    elif how == "sha-256 salted":
        tried = {masking.salted_hash(g) for g in guesses}
    else:
        tried = {hide(g) for g in guesses}
    undone = len(hidden & tried)
    table[how] = [joined, undone]
    print(f"{how:<16}{joined:>15,}{undone:>15,}")

target = "kevin.anderson@bexham-estates.example"
column = {masking.plain_hash(e) for e in tickets}
print(f"\none guess, one hash: is {target.split('@')[0]} a "
      f"requester? {masking.plain_hash(target) in column}")
Path(__file__).with_suffix(".json").write_text(json.dumps(
    {"tickets": len(tickets), "people": len(people), "ways": table},
    indent=1) + "\n")
