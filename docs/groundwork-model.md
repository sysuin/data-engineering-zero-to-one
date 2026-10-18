# Groundwork's model

The target model for Groundwork's marts: every table analysts, Clarity and Foresight read,
its grain, its keys and its measures, and which dimensions each fact shares. Written in
Chapter 4 of *Data Engineering from Zero to One*, before any of it is loaded. The tables
exist, empty, in `code/groundwork/ddl/marts.sql`; `dim_date` is filled by
`code/groundwork/dates.py`; Chapters 12 and 13 fill the rest, and Chapter 19 adds portal
sessions.

A change to this model changes this file first.

## Rules every mart follows

| Rule | Decision |
|---|---|
| Grain | Every table states it in one sentence, in this file and in the catalogue (`COMMENT ON TABLE`; read it with `SELECT table_name, comment FROM duckdb_tables()`). |
| Surrogate keys | `*_key` is the first 16 hex characters of the MD5 of the natural key (plus `valid_from` for a versioned row). Deterministic: a rebuild or a backfill mints the same key. Chapter 12 argues it. |
| The date key | `date_key` is the date as a number, `20251231`. The one key that carries meaning, because a calendar never changes. |
| Money | `DECIMAL(18,2)` dollars. Raw and staging keep integer cents; the marts convert once. Never `DOUBLE`. |
| Moments | `*_at` is a UTC `TIMESTAMP` without a zone. |
| Dates | `*_date` is the depot's calendar date. A fact joins `dim_date` on the depot's date, never on the UTC date of its timestamp. |
| Revenue | `SUM(qty × unit_price)` over lines of placed, not-cancelled orders, dated by the depot, `unit_price` being the price charged after the line's discount. |
| Versioned dimensions | `valid_from` inclusive, `valid_to` exclusive and `NULL` while current; a fact carries the key of the version valid on its date. |
| Personal data | Contact names, emails and phone numbers stay out of the marts (Chapter 24). |

## Why dates are local

`placed_at` is stored in UTC. Meridian's depots keep their own calendars: Newark, Atlanta
and Columbus on New York time, Sacramento on Los Angeles time, Dallas on Chicago time.
Of 478,242 placed orders, 49,309 (10.3%) have a UTC date that is not the depot's date:
every one placed in the evening, local time. In Sacramento it is 19.9% of orders.
Joined on the UTC date, 6,855 Friday-evening orders land on a Saturday, 1,566 orders land
in the wrong month and 156 in the wrong year (listings `code/04/04_utc_vs_local_date.py`
and `05_date_dimension.py`).

## Dimensions

### `dim_date` — one row per calendar day, 2021-01-01 to 2026-12-31

| | |
|---|---|
| Key | `date_key` (`20251231`); `date` is unique too |
| Source | none: computed by `groundwork/dates.py` (2,191 rows) |
| Attributes | year, quarter, month, month name, day of month and year, ISO day of week and name, `is_weekend`, `is_working_day` (Monday to Friday, as finance counts), `working_day_of_month`, ISO year and week, `week_start`, `month_start`, `month_end`, `is_month_end`, fiscal year, quarter and month |
| Notes | Meridian's financial year is the calendar year (`FISCAL_START_MONTH = 1`); a fiscal year is named for the calendar year it ends in. `iso_year` differs from `year` at the edges: 31 December 2025 is in ISO week 1 of 2026. |

### `dim_region` — one row per region (5)

| | |
|---|---|
| Key | `region_key`; natural key `region_id` |
| Source | `reference/regions.csv`; the ledger's region codes (NE, SE, MW, WE, SW) |
| Attributes | name, code, depot, timezone |

### `dim_supplier` — one row per supplier on procurement's list (20)

| | |
|---|---|
| Key | `supplier_key`; natural key `supplier_id` |
| Source | `reference/suppliers.csv` (procurement's spelling wins over the catalogue's) |
| Attributes | name, country, tier |

### `dim_product` — one row per SKU per cost version (SCD Type 2 on `unit_cost`)

| | |
|---|---|
| Key | `product_key`; natural key `(sku, valid_from)` |
| Source | the catalogue (name, category, supplier, list price) and the supplier price lists (cost, by effective date) |
| Attributes | name, category, `supplier_key`, `list_price`, `unit_cost`, validity |

### `dim_account` — one row per account per version (SCD Type 2 on segment, owner, status)

| | |
|---|---|
| Key | `account_key`; natural key `(account_id, valid_from)` |
| Source | Relay's nightly exports and field audit; ids from before the CRM migration resolved to the Relay account (Chapter 12) |
| Attributes | name, `region_key`, segment, owner, status, key account, customer since, closed on, postcode (text), validity |

## Facts

### `fct_order_lines` — one row per line of a placed, not-cancelled order

The model's centre, and the answer to "what did we sell". Transaction fact.

| | |
|---|---|
| Natural key | `(order_id, line_no)` |
| Dimension keys | `date_key` (the depot's date), `account_key`, `product_key` (versions valid that day), `supplier_key`, `region_key` |
| Degenerate dimensions | `order_id`, `order_number`, `channel` |
| Measures | `qty` (additive), `revenue` = qty × unit_price (additive), `cost` = qty × unit_cost (additive), `gross_margin` = revenue − cost (additive); `unit_price`, `unit_cost`, `discount_pct` (non-additive: average them weighted, never sum) |
| Excludes | cancelled orders |

### `fct_orders` — one row per placed, not-cancelled order

`fct_order_lines` rolled up to the order, for questions about orders rather than lines
(order counts, order value, lines per order). Its revenue must equal the lines' revenue
to the cent.

| | |
|---|---|
| Natural key | `order_id` (and `order_number`) |
| Dimension keys | `date_key`, `account_key`, `region_key` |
| Measures | `line_count`, `units`, `revenue`, `cost`, `gross_margin` (all additive) |

### `fct_tickets` — one row per ticket, from opened to closed (accumulating snapshot)

The row is inserted when a ticket opens and updated as it reaches each milestone.

| | |
|---|---|
| Natural key | `ticket_id` |
| Dimension keys | `opened_date_key`, `first_reply_date_key`, `solved_date_key`, `closed_date_key` (one `dim_date`, four roles), `account_key` (nullable: not every requester is a customer), `region_key`, `product_key` (when the ticket names a SKU) |
| Measures | `comment_count` (additive), `hours_to_first_reply`, `hours_to_solve` (non-additive: average or take percentiles) |

### `fct_contracts` — one row per contract

| | |
|---|---|
| Natural key | `contract_id` |
| Dimension keys | `start_date_key`, `end_date_key`, `notice_date_key` (three roles), `account_key` (version valid at the start), `region_key` |
| Attributes and measures | term, legacy terms, `discount_pct` (non-additive), outcome, cancellation reason |

### `fct_contract_month` — one row per contract per month end it is in force (periodic snapshot)

| | |
|---|---|
| Natural key | `(contract_id, month_end_date_key)` |
| Dimension keys | `month_end_date_key`, `account_key` (version valid that day), `region_key` |
| Measures | `contracts_in_force` (always 1; semi-additive: sum across contracts, never across month ends; average or take the last), `months_in_force`, `months_remaining` |

In 2025 the twelve month ends summed to 55,389 contracts in force; the answer at 31
December was 4,651 (listing `06_additivity.py`).

### `fct_portal_sessions` — one row per portal session

Built by Chapter 19 from the event stream. Its columns here are the target; Chapter 19
may add to them.

| | |
|---|---|
| Natural key | `session_id` |
| Dimension keys | `date_key` (the depot's date of the session start), `account_key`, `region_key` |
| Measures | `event_count`, `searches`, `product_views`, `add_to_carts`, `checkouts_started`, `orders_submitted` (all additive) |

## The bus matrix

Which facts share which conformed dimensions. A number in the date column counts the
roles `dim_date` plays.

| Fact | date | account | region | product | supplier |
|---|:-:|:-:|:-:|:-:|:-:|
| `fct_order_lines` | 1 | ● | ● | ● | ● |
| `fct_orders` | 1 | ● | ● | | |
| `fct_tickets` | 4 | ● | ● | ● | |
| `fct_contracts` | 3 | ● | ● | | |
| `fct_contract_month` | 1 | ● | ● | | |
| `fct_portal_sessions` | 1 | ● | ● | | |

Because every fact uses the same `dim_account`, `dim_region` and `dim_date`, a question
that crosses processes ("revenue and tickets by segment by month") is answered by
aggregating each fact to the shared grain first and joining the results, never by
joining two facts row to row.

## Measures, by how they add up

| Measure | Adds across | Does not add across |
|---|---|---|
| revenue, cost, gross margin, qty, units, line and comment counts | everything | — |
| contracts in force | contracts, accounts, regions | time: take the month end, or the average |
| distinct accounts, distinct orders | nothing | re-count at the grain asked for |
| unit price, discount %, margin %, average order value, hours to solve | nothing | compute from additive parts: `SUM(revenue) / SUM(orders)` |

## What is deliberately not in the marts

- A wide "one big table" of order lines with every attribute copied in. It can be built
  as a view over the star when a tool needs one; it is never the source of truth.
- Cancelled orders. They stay in staging, where Chapter 13 counts them.
- Contact details (Chapter 24).
