"""
Ways to show less of a person, and which of them survive a join.

    redact(text)          emails and phone numbers in text replaced by
                          [email] and [phone]; the rest of the words kept
    mask_email(e)         k***@bexham-estates.example: enough for a
    mask_phone(p)         (***) ***-0152   colleague to confirm a caller
    plain_hash(v)         SHA-256 of the value. Joins survive, and so
                          does the value: hash a list of guesses and
                          look the hashes up. Never use it for people.
    salted_hash(v)        SHA-256 with a fresh random salt: nothing can
                          be looked up, and nothing joins either
    pseudonym(v)          HMAC-SHA-256 under a secret key: the same
                          value gives the same pseudonym in every table,
                          so joins survive; without the key there is
                          nothing to look up
    Vault                 tokenisation: a random token per value, and a
                          table that maps back, which few may read

    build(con)            the masked schema, rebuilt from staging

The key comes from the environment, GROUNDWORK_PSEUDONYM_KEY, and is
never printed, logged or written into SQL. That last rule is why the
masked relations are tables and not views: a view's text is readable
by anyone who can read the view, and a keyed hash computed in SQL would
carry its key in that text. The tables are built with a Python
function registered on the connection for the build, and removed after.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import re
import secrets

from groundwork.govern.classify import EMAIL, PHONE

KEY_VAR = "GROUNDWORK_PSEUDONYM_KEY"


def key() -> bytes:
    """The pseudonym key, from the environment. Never print it."""
    value = os.environ.get(KEY_VAR)
    if not value:
        raise RuntimeError(f"set {KEY_VAR} (see .env.example)")
    return value.encode()


def normalise(value: str) -> str:
    """One spelling per value: hashes of 'A@x' and ' a@x' must agree."""
    return value.strip().lower()


def redact(text: str | None) -> str | None:
    if text is None:
        return None
    return re.sub(PHONE, "[phone]", re.sub(EMAIL, "[email]", text))


def mask_email(email: str) -> str:
    user, _, domain = email.partition("@")
    return f"{user[:1]}***@{domain}"


def mask_phone(phone: str) -> str:
    return "(***) ***-" + re.sub(r"\D", "", phone)[-4:]


def plain_hash(value: str) -> str:
    return hashlib.sha256(normalise(value).encode()).hexdigest()


def salted_hash(value: str) -> str:
    salt = secrets.token_bytes(16)
    return hashlib.sha256(salt + normalise(value).encode()).hexdigest()


def pseudonym(value: str | None,
              secret: bytes | None = None) -> str | None:
    """A keyed hash: 16 hex characters, the same for the same value."""
    if value is None or value == "":
        return None
    secret = key() if secret is None else secret
    digest = hmac.new(secret, normalise(value).encode(), hashlib.sha256)
    return digest.hexdigest()[:16]


class Vault:
    """Tokens handed out in order of first sight, and the way back."""

    def __init__(self):
        self._token: dict[str, str] = {}
        self._value: dict[str, str] = {}

    def token(self, value: str) -> str:
        value = normalise(value)
        if value not in self._token:
            t = f"tok_{len(self._token) + 1:06d}"
            self._token[value], self._value[t] = t, value
        return self._token[value]

    def reveal(self, token: str) -> str:
        return self._value[token]

    def forget(self, value: str) -> bool:
        """Delete a value's entry: its token now points at nothing."""
        t = self._token.pop(normalise(value), None)
        if t is None:
            return False
        del self._value[t]
        return True


# What the masked schema shows for each staging relation with a
# customer's personal data. Customer names are left out; emails become
# keyed pseudonyms, which still join; free text is redacted.
MASKED = {
    "stg_relay__accounts": {
        "drop": ["contact_name", "contact_phone"],
        "pseudonym": ["contact_email"]},
    "stg_frontline__tickets": {
        "drop": ["requester_name"],
        "pseudonym": ["requester_email"]},
    "stg_frontline__ticket_comments": {
        "drop": ["author"],
        "redact": ["body"]},
}


_UDFS = iter(range(1, 1 << 30))


def udf(con, fn) -> str:
    """Register a text -> text Python function under a fresh name."""
    name = f"_keyed_{next(_UDFS)}"
    con.create_function(name, fn, ["VARCHAR"], "VARCHAR",
                        null_handling="special")
    return name


def _redact_sql(column: str) -> str:
    return (f"regexp_replace(regexp_replace({column}, '{EMAIL}', "
            f"'[email]', 'g'), '{PHONE}', '[phone]', 'g')")


def build(con, secret: bytes | None = None, source: str = "staging",
          target: str = "masked") -> dict[str, int]:
    """Build the masked tables from staging; rows in each."""
    secret = key() if secret is None else secret
    fn = udf(con, lambda v: pseudonym(v, secret))
    out = {}
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {target}")
        for name, how in MASKED.items():
            cols = [c for (c,) in con.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_schema = ? AND table_name = ?"
                " ORDER BY ordinal_position", [source, name]).fetchall()]
            select = []
            for c in cols:
                if c in how.get("drop", []):
                    continue
                if c in how.get("pseudonym", []):
                    select.append(f"{fn}({c}) AS {c}_ref")
                elif c in how.get("redact", []):
                    select.append(f"{_redact_sql(c)} AS {c}_redacted")
                else:
                    select.append(c)
            con.execute(f"CREATE OR REPLACE TABLE {target}.{name} AS "
                        f"SELECT {', '.join(select)} FROM {source}.{name}")
            (out[name],) = con.execute(
                f"SELECT count(*) FROM {target}.{name}").fetchone()
    finally:
        con.remove_function(fn)
    return out
