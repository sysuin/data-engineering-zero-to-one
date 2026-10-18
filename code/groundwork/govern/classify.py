"""
Every column Groundwork holds, classified: personal, confidential or
public.

    personal       about a person who could be identified: a contact's
                   name, email or phone, a portal login, a colleague's
                   name, and any text that turns out to contain them
    confidential   Meridian's business: money, terms, which customer
                   pays what, and anything nobody has reviewed yet
    public         harmless on its own: ids, dates, codes, calendar
                   columns, list prices the portal shows everyone

A column is classified twice. First by its name, with RULES, in order,
first match wins; then by what it holds: every text column is scanned
for email addresses and phone numbers, and a column with any is
personal whatever its name says. A column no rule names is
confidential until its owner says otherwise, and is reported.

    find(text)                 [(kind, value), ...] in one string
    scan(con, relation, cols)  rows per column with an email or phone
    classify(con, schemas)     one Column per column, every relation
    save(con, columns)         govern.classification
"""
from __future__ import annotations

import re
from dataclasses import dataclass

PERSONAL, CONFIDENTIAL, PUBLIC = "personal", "confidential", "public"
LAYERS = ("raw", "staging", "intermediate", "marts")

# What a person's contact details look like in text. RE2 (DuckDB) and
# Python's re read both the same way.
EMAIL = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}"
PHONE = r"\(?\b[2-9][0-9]{2}\)?[ .-]?[0-9]{3}[ .-][0-9]{4}\b"
# The pattern people write first: a digit, six or more digits,
# spaces, dots, dashes or brackets, a digit.
LOOSE_PHONE = r"\+?[0-9][0-9 ().-]{5,}[0-9]"

FREE_TEXT = "free text: classified by what it holds"
RULES = [
    (r"_ref$", PERSONAL, "a keyed pseudonym: still personal data"),
    (r"_redacted$", CONFIDENTIAL, "redacted text"),
    (r"^(contact_email|requester_email|contact_phone|email|phone)$",
     PERSONAL, "a customer's contact detail"),
    (r"^(user_id|session_id)$", PERSONAL, "identifies a portal user"),
    (r"^(contact_name|requester_name|author)$",
     PERSONAL, "a customer's name"),
    (r"^(owner|owner_name|assignee|changed_by)$",
     PERSONAL, "a colleague's name"),
    (r"^(body|description|payload|notes?|old_value|new_value|value"
     r"|row_json|detail|error|extra|properties)$",
     CONFIDENTIAL, FREE_TEXT),
    (r"^(list_price|list_price_cents|list price)$",
     PUBLIC, "shown on the portal"),
    (r"(_cents|_pct)$|^(revenue|gross_margin|unit_cost|unit_price|cost"
     r"|amount|units|qty|change_pct)$", CONFIDENTIAL, "money and terms"),
    (r"^(account_name|old_name|relay_name|postcode|segment"
     r"|customer_since(_date)?|cancellation_reason|outcome|term_months"
     r"|(is_)?legacy_terms|(is_)?key_account|notice_date|closed_on"
     r"|closed_date|user_agent|device)$",
     CONFIDENTIAL, "about a customer"),
    (r"^_|^(load_id|load_date|file|file_sha256|sha256|size|status"
     r"|source_format|source|table_name|name|logical_key|note|seen_at"
     r"|arrived_at|layout|format|cost_unit|currency|fingerprint|model"
     r"|relation|materialised|rows.*|source_count|run_id|parked.*"
     r"|rule|reason|scope|ref|entity|record_type|object|field.*"
     r"|source_file|supplier_slug)$",
     PUBLIC, "pipeline metadata"),
    (r"(^|_)(id|key|code|at|date|on|day|month|year|week|quarter|name"
     r"|from|to|seen"
     r"|count|number|no|version|hours_to_.*)$|^(sku|is_.*|channel"
     r"|priority|category|desk|language|region|tag|tier|country"
     r"|timezone|depot|period|date|public|line_count|event_type"
     r"|schema_version|spelling|matched_by|position|score|searches"
     r"|product_views|add_to_carts|checkouts_started|orders_submitted"
     r"|month_end|month_start|week_start|supplier|product name"
     r"|category|launched|gl_name|contracts_in_force|months_.*"
     r"|occurred_at|received_at|record_id)$",
     PUBLIC, "an id, a date or a code"),
]
UNREVIEWED = "no rule: confidential until its owner reviews it"


@dataclass(frozen=True)
class Column:
    layer: str
    relation: str
    column: str
    type: str
    cls: str
    reason: str
    hits: int = 0          # rows holding an email or a phone number


def by_name(column: str) -> tuple[str, str]:
    """The class a column's name gives it, and why."""
    for pattern, cls, reason in RULES:
        if re.search(pattern, column, re.IGNORECASE):
            return cls, reason
    return CONFIDENTIAL, UNREVIEWED


def find(text: str | None, phone: str = PHONE) -> list[tuple[str, str]]:
    """Every email and phone number in one piece of text."""
    if not text:
        return []
    found = [("email", m.group()) for m in re.finditer(EMAIL, text)]
    found += [("phone", m.group()) for m in re.finditer(phone, text)]
    return found


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def scan(con, relation: str, columns: list[str]) -> dict[str, int]:
    """Rows in each text column holding an email or a phone number."""
    if not columns:
        return {}
    pattern = f"({EMAIL})|({PHONE})"
    sql = ", ".join(
        f"count(*) FILTER (WHERE regexp_matches("
        f"CAST({_quote(c)} AS VARCHAR), '{pattern}'))" for c in columns)
    counts = con.execute(f"SELECT {sql} FROM {relation}").fetchone()
    return dict(zip(columns, counts))


def relations(con, schemas=LAYERS) -> list[tuple[str, str]]:
    """(schema, name) of every table and view in those schemas."""
    marks = ", ".join("?" for _ in schemas)
    return con.execute(
        f"SELECT table_schema, table_name FROM information_schema.tables"
        f" WHERE table_schema IN ({marks}) ORDER BY 1, 2",
        list(schemas)).fetchall()


def classify(con, schemas=LAYERS) -> list[Column]:
    """Every column in those schemas, by name and then by content."""
    out = []
    for schema, name in relations(con, schemas):
        relation = f"{schema}.{name}"
        cols = con.execute(
            "SELECT column_name, data_type FROM information_schema.columns"
            " WHERE table_schema = ? AND table_name = ?"
            " ORDER BY ordinal_position", [schema, name]).fetchall()
        text = [c for c, t in cols if t in ("VARCHAR", "JSON")]
        hits = scan(con, relation, text)
        for column, type_ in cols:
            cls, reason = by_name(column)
            n = hits.get(column, 0)
            if n and cls != PERSONAL:
                cls = PERSONAL
                reason = f"holds emails or phone numbers ({reason})"
            elif reason == FREE_TEXT:
                reason = "free text, scanned: nothing found"
            out.append(Column(schema, relation, column, type_, cls,
                              reason, n))
    return out


def save(con, columns: list[Column]) -> int:
    """Write the classification to govern.classification, replacing it."""
    con.execute("CREATE SCHEMA IF NOT EXISTS govern")
    con.execute("""
        CREATE OR REPLACE TABLE govern.classification (
            layer VARCHAR, relation VARCHAR, "column" VARCHAR,
            type VARCHAR, class VARCHAR, reason VARCHAR, hits BIGINT,
            PRIMARY KEY (relation, "column"))""")
    con.executemany(
        "INSERT INTO govern.classification VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(c.layer, c.relation, c.column, c.type, c.cls, c.reason, c.hits)
         for c in columns])
    return len(columns)


def personal(con) -> dict[str, list[str]]:
    """relation -> its personal columns, from govern.classification."""
    out: dict[str, list[str]] = {}
    for relation, column in con.execute(
            "SELECT relation, \"column\" FROM govern.classification"
            " WHERE class = 'personal' ORDER BY 1, 2").fetchall():
        out.setdefault(relation, []).append(column)
    return out
