# Groundwork: the source inventory

Groundwork v0.1. Every system Meridian's data comes from, what it holds, how it reaches
us, and what we noticed when we profiled it. Written in Chapter 3 of *Data Engineering
from Zero to One*; each later chapter that learns something about a source adds it here.

As of Monday 5 January 2026, 06:00 UTC. Numbers are from the profiles in `code/03/`
(`09_profile.py`, `10_profile_everything.py`, `11_grain.py`); rerun them to refresh.

Profile any table or file the same way:

```text
PYTHONPATH=code .venv/bin/python -m groundwork.profile data/meridian-sources/landing/catalogue/catalogue.csv
```

## Summary

| Source | Owner | Arrives as | When | Grain: one row per | Key | Rows today |
|---|---|---|---|---|---|---|
| Tradeline `orders` | orders team | SQLite database, read-only | continuously | order | `id` | 483,980 |
| Tradeline `order_lines` | orders team | same database | continuously | line of an order | `order_id, line_no` | 1,243,789 |
| Relay `accounts.csv` | sales operations | CSV, a full export each night | 23:00 Newark (04:00 UTC in winter) | account, as it stands that night | `account_id` | 6,058 |
| Relay `contracts.csv` | sales operations | same export | same | contract, as it stands that night | `contract_id` | 19,220 |
| Frontline tickets | customer support (a bought product) | JSON API, cursor pages | on request | ticket, as it stands when asked | `id` | 21,668 |
| Supplier price lists | procurement | files in a folder, one per supplier per month | when each supplier sends it | SKU in one month's list | file, SKU | 16 folders |
| Catalogue | merchandising | one CSV, overwritten when edited | when someone saves it | product | `SKU` | 48 |
| Portal events | portal team | gzipped JSON lines, one file an hour | hourly, from 1 July 2025 | event (see Chapter 19) | `event_id` | 4,445 files |
| Ledger | finance | one CSV per closed month | fifth working day of the next month | journal line | `period, region, description` | 237 |
| Regions | kept by hand | CSV | rarely changes | region | `region_id` | 5 |
| Supplier master | procurement | CSV | rarely changes | supplier | `supplier_id` | 20 |

Every grain above was proved by counting rows against distinct keys
(`code/03/11_grain.py`); none failed.

## Tradeline, the orders application

- **Owner:** the orders team. **Reach it by:** `meridian.sources.tradeline.connect()`,
  a read-only connection. Never write to it.
- **Tables:** `orders` (8 columns), `order_lines` (8), `channels` (4 rows: FLD Field
  sales, PTR Partner, TEL Telesales, WEB Web), `replication_log` (the last seven days of
  committed changes; Chapter 7).
- **Time columns:** `placed_at`, `created_at`, `updated_at`, stored as **text**, in
  **UTC**, with no zone written. Orders are quietest at 08:00 in the text's clock, which is
  3 a.m. in Newark. 54 orders are stamped 2026 although trading ended on 31 December 2025.
- **Money:** `unit_price_cents`, integer cents, **already after the line's discount**.
  `discount_pct` records what was taken off. Never apply it again: doing so understates
  November 2025 by 4.53%.
- **Status:** `placed` 478,242, `cancelled` 5,738. Cancelled orders stay in the table.
- **Noticed, and whose it is:**
  - cancelled orders present → excluded from revenue in Chapter 13;
  - UTC text timestamps; an order's day is the depot's local date → Chapter 4;
  - cents after discount → typed as money in Chapter 5, summed as cents throughout.
- **The owner said:** a nightly job runs at 02:00 UTC; read after it (02:05 was
  suggested). Cancelling an order sets its status.

## Relay, the CRM

- **Owner:** sales operations. **Reach it by:** the export folder,
  `landing/relay/<night>/`, which keeps a fortnight (14 nights today, 2025-12-22 to
  2026-01-04). Older nights come from IT's archive (`meridian.sources.relay.export`).
- **Files:** `accounts.csv` (14 columns) and `contracts.csv` (11 columns), UTF-8 with a
  byte-order mark (`ef bb bf`), comma-separated, ISO dates, `true`/`false`.
- **Time columns:** `customer_since`, `closed_on`, `start_date`, `end_date`,
  `notice_date` (dates); `updated_at` (UTC, with `Z`).
- **Current state only.** Each export describes every record as it stands at 23:00 in
  Newark that night. Between the 31 December export and the 4 January export, 79 accounts
  and 49 contracts changed and 377 contracts were added; the files show only the new
  values. Keeping history is our job (Chapter 12).
- **Personal data:** contact names, emails and phone numbers (Chapter 24).
- **Also here:** `audit/field_history_YYYY-MM.csv`, a monthly file of field changes
  (Chapter 12).

## Frontline, the support desk

- **Owner:** customer support; the product is bought, and its API is the only way in.
- **Reach it by:** `GET /api/v2/tickets` with `Authorization: Bearer <token>` (from
  `.env`), paged by `limit` and `after_cursor`; `has_more` says whether to ask again.
  Without the token: 401.
- **Shape:** one JSON object per ticket, with a nested `requester` and a list of
  `custom_fields`. Pages are ordered by `updated_at`, then `id`.
- **Time columns:** `created_at`, `updated_at` (UTC, with `Z`).
- **Count:** 21,668 tickets by paging with the cursor, 21,668 by the API's own total.
- **Noticed:** 721 descriptions typed in capitals, 10,019 in lower case. Free text: a note,
  not a problem.
- **Personal data:** requester names and emails (Chapter 24). Reading it properly, with
  retries and limits, is Chapter 8.

## Supplier price lists

- **Owner:** procurement, who receive them by email and drop them in
  `landing/suppliers/<supplier>/`. A file's modification time is when it arrived.
- **Suppliers:** 16 folders; procurement's master lists 20. The other four sell Meridian
  nothing it stocks (procurement's answer).
- **Layouts:** four, among the newest files:
  - 13 suppliers: `sku,description,unit_cost,currency,effective_from` (dollars);
  - Kestrel: `sku,cost_cents,effective`;
  - Pemberton: `Item Code,Item,Unit Cost USD,Valid From` (dates as DD/MM/YYYY);
  - Aldridge: a spreadsheet (`.xlsx`).
- **Grain:** one row per SKU per monthly list; unique within every file checked (13
  alike, Kestrel, Pemberton).
- **Whose it is:** reading the files once each, and the spreadsheet, is Chapter 9;
  contracts for their columns and units are Chapter 10.

## Catalogue

- **Owner:** merchandising. One file, `landing/catalogue/catalogue.csv`, overwritten when
  edited: yesterday's version is gone.
- **Columns:** `SKU, Product Name, Category, Supplier, List Price, Launched, Notes`.
- **Noticed:**
  - `List Price` is money held as text (`$88.64`);
  - 4 supplier names are not written as procurement writes them: `'Aldridge and Co'`,
    `'Kestrel'`, `'Pemberton'`, `'Voss Industrial '` (a trailing space);
  - 3 SKUs are not written as Tradeline writes them: `'MRD-FAC-031 '`, `'mrd-pac-022'`,
    `'MRD-SAN-043 '`.
  Matching names and codes across teams is Chapter 11.

## Portal events

- **Owner:** the portal team. **Arrives as:** `landing/portal/YYYY/MM/DD/HH.jsonl.gz`,
  one file an hour; 4,445 files (35.1 MB) today, the first
  `portal/2025/07/01/04.jsonl.gz`.
- Each line is one JSON event. Reading them is Chapter 19, and nothing else in
  Groundwork reads them before then.

## Ledger

- **Owner:** finance. **The number of record.**
- **Arrives as:** `landing/ledger/gl_revenue_YYYY-MM.csv`, one file per closed month, on
  the fifth working day of the next; 47 months, 2022-01 to 2025-11. November's arrived
  2025-12-05 22:00 UTC; December's is due on 8 January.
- **Columns:** `period,entity,region,gl_account,gl_name,description,amount`. Some
  descriptions contain commas, so the file quotes them; read it with quoting on.
- **Grain:** one journal line; `period, region, description` is unique (237 of 237).
- Groundwork reconciles to it (Chapter 15). It never overrules it.

## Reference files

- `reference/regions.csv`: 5 regions with depot and time zone (Newark, Atlanta and
  Columbus on New York time; Sacramento on Los Angeles; Dallas on Chicago).
- `reference/suppliers.csv`: procurement's master, 20 suppliers.

## The five questions, for every source

1. What is one row (or file, or event)?
2. What identifies it, and can that identifier change or be reused?
3. Which times does it carry, and on which clock?
4. How does a record change after it is first written, and how would we know?
5. Who do we tell when it looks wrong, and who tells us when it changes?

An owner's answer is a claim; the profile is the evidence. Where they disagree, the
chapter that finds out says so here.
