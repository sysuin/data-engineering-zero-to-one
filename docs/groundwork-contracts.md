# Groundwork: data contracts

Groundwork v0.4. What every raw table promises, who promises it, and what happens when a
batch breaks the promise. Written in Chapter 10 of *Data Engineering from Zero to One*.
The contracts themselves are the YAML files in `code/groundwork/contracts/`; this page
says how to read them, how they are enforced, and how to change one.

## The tables and their contracts

| Raw table | Producer | Policy for bad rows | Checked |
|---|---|---|---|
| `tradeline.orders`, `order_lines` | Tradeline (orders team) | fail | the partition, straight after it is written |
| `tradeline.orders_log`, `order_lines_log` | Tradeline's replication log | fail | the same |
| `frontline.tickets`, `ticket_fields`, `ticket_tags` | Frontline (support ops) | fail | the same |
| `relay.accounts`, `contracts` | Relay, the CRM (sales ops); the old CRM before March 2024 | quarantine, at most 5% | each file, before it lands |
| `relay.field_history`, `migration_map` | Relay; IT | quarantine, at most 5% | each file, before it lands |
| `suppliers.price_lists` | sixteen suppliers, through procurement | quarantine, at most 50% | each file, before it lands |
| `catalogue.products` | merchandising | quarantine, at most 10% | each file, before it lands |
| `ledger.gl_revenue` | finance | fail | each file, before it lands |
| `reference.regions`, `suppliers`, `price_events` | kept by hand | fail | each file, before it lands |

Every contract is owned by data engineering, who answer for it; the producer named in it is
who is asked when it breaks.

## What a contract says

```yaml
table: suppliers.price_lists       # <source>.<table>, the raw table
version: 2                         # raised on every change, with a note in `changes`
producer: {who: ..., contact: ...} # who sends it, and whom to ask
owner: ...                         # who answers for the contract
grain: ...                         # what one row is
key: [file_sha256, sku]            # unique within a batch
formats:                           # headers a file may arrive with (file sources)
  pemberton: [Item Code, Item, Cost (USD), Valid From]
columns:                           # every raw column, in order
  cost:
    type: decimal                  # text, integer, decimal, date, timestamp, boolean
    nullable: false                # the default: empty text counts as empty
    allowed: [...]                 # the only values it may take
    pattern: '...'                 # a regular expression the whole value matches
    min: 0.50                      # range, on the number
    max: 1000.00
    scale: 2                       # most decimals a decimal may have
    format: '%Y-%m-%d'             # how a date or timestamp is written
    unit: USD                      # what the number counts (documentation)
    stamped: false                 # true: added by the loader, not sent
    when:                          # refinements by another column's value
      cost_unit:
        cents: {type: integer, min: 50, max: 100000}
history:                           # a value against the last one for its key
  value: cost
  per: [supplier, sku]
  max_ratio: 3
policy:
  bad_rows: quarantine             # or fail
  max_bad_share: 0.5               # above this, the batch is broken, not its rows
```

Types say what the *text* must look like. Raw keeps what the source sent: a file's values
stay text, and staging (Chapter 11) casts them, by the rules written here. Moments in a
contract are UTC; a timestamp written with a trailing `Z` is read as a `TIMESTAMP`, never a
`TIMESTAMPTZ`.

## What happens to a batch

1. **Header.** For a file, its header is matched to one of `formats`. A column renamed,
   removed or changed, or a header no format allows, is **breaking**. An added column or a
   new order is a note. A reader that read the file as a different format from its
   header's is breaking.
2. **Rows.** Every rule of every column, as one SQL pass, plus the key and the history.
3. **Verdict.** Under `fail`, any bad row breaks the batch. Under `quarantine`, bad rows
   are parked in `audit.quarantine` with every reason they failed, and the rest land; but
   if more than `max_bad_share` of the rows fail, the batch is broken.

A broken file is recorded `failed` in `audit.file_ledger` with the reason, and is read
again on every run until something changes. One sender's broken file does not stop the
others' (Relay's night, which is expected rather than discovered, fails whole). Every
check is a row of `audit.contract_runs`.

```sql
-- what was parked, and why
SELECT table_name, file, row_number, reason FROM audit.quarantine ORDER BY parked_at;
-- every check that did not pass
SELECT * FROM audit.contract_runs WHERE status <> 'passed' ORDER BY checked_at;
```

## Running it

```text
.venv/bin/python -m groundwork.ingest --day 2026-01-04      # every source, one day
.venv/bin/python -m groundwork.contracts.check              # hold raw to the contracts
.venv/bin/python -m groundwork.contracts.check suppliers.price_lists --reopen
```

`--reopen` marks every file that breaks its contract as failed in the file ledger, so the
next ingest for its load date reads it again with today's reader and contract.

## Changing a contract

`contracts.compatible(old, new)` lists each change and who it can break:

- **nobody**: an optional column added; a new header that lands in the same columns.
- **consumers**: anything that lets more through (a new allowed value, a wider range, a
  column that may now be empty). Find who reads the column before you ship it.
- **producer**: anything that lets less through. The producer's next batch may fail;
  agree it with them first.
- **both**: a column's type changed.

Raise `version`, add a line to `changes`, and say in the note who agreed it.

## Changes

| Contract | Version | What changed |
|---|---|---|
| `suppliers.price_lists` | 2 | Pemberton's header from July 2024 (`Unit Cost USD` for `Cost (USD)`), read by `ingest/price_lists.py`; 18 lists reloaded |
| every other | 1 | first written |
