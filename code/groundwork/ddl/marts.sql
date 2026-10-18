-- Groundwork's marts: the tables analysts, Clarity and Foresight read.
--
-- Chapter 4 designs them and creates them empty; Chapters 12 and 13 fill
-- them, and Chapter 19 adds portal sessions. docs/groundwork-model.md says
-- why each table has the grain it has. Rules every table here follows:
--
--   * The first comment on each table is its grain, in one sentence.
--   * *_key columns are surrogate keys: the first 16 hex characters of the
--     MD5 of the natural key (plus valid_from for a versioned row), so a
--     rebuild mints the same keys. dim_date's key is the date as yyyymmdd.
--   * Money is DECIMAL(18,2) dollars. Never DOUBLE.
--   * *_at columns are UTC moments without a zone; *_date columns are the
--     depot's calendar dates, and date_key columns point at those dates.
--
-- Every statement is IF NOT EXISTS, so running this file twice is safe.

CREATE SCHEMA IF NOT EXISTS marts;

-- ------------------------------------------------------------ dimensions

-- One row per calendar day, 2021-01-01 to 2026-12-31. Built by
-- groundwork/dates.py; no source sends it.
CREATE TABLE IF NOT EXISTS marts.dim_date (
    date_key              INTEGER PRIMARY KEY,   -- 20251231
    date                  DATE NOT NULL UNIQUE,
    year                  SMALLINT NOT NULL,
    quarter               TINYINT NOT NULL,
    month                 TINYINT NOT NULL,
    month_name            VARCHAR NOT NULL,
    day_of_month          TINYINT NOT NULL,
    day_of_year           SMALLINT NOT NULL,
    day_of_week           TINYINT NOT NULL,      -- 1 Monday ... 7 Sunday
    day_name              VARCHAR NOT NULL,
    is_weekend            BOOLEAN NOT NULL,
    is_working_day        BOOLEAN NOT NULL,      -- Monday to Friday
    working_day_of_month  TINYINT,               -- NULL at weekends
    iso_year              SMALLINT NOT NULL,
    iso_week              TINYINT NOT NULL,
    week_start            DATE NOT NULL,         -- the Monday
    month_start           DATE NOT NULL,
    month_end             DATE NOT NULL,
    is_month_end          BOOLEAN NOT NULL,
    fiscal_year           SMALLINT NOT NULL,     -- named for the year it ends
    fiscal_quarter        TINYINT NOT NULL,
    fiscal_month          TINYINT NOT NULL
);

-- One row per region (5). From reference/regions.csv.
CREATE TABLE IF NOT EXISTS marts.dim_region (
    region_key            VARCHAR PRIMARY KEY,
    region_id             SMALLINT NOT NULL UNIQUE,
    region_name           VARCHAR NOT NULL,
    region_code           VARCHAR NOT NULL,      -- the ledger's code: NE, SE ...
    depot                 VARCHAR NOT NULL,
    timezone              VARCHAR NOT NULL       -- the depot's calendar
);

-- One row per supplier: procurement's master list (20).
CREATE TABLE IF NOT EXISTS marts.dim_supplier (
    supplier_key          VARCHAR PRIMARY KEY,
    supplier_id           SMALLINT NOT NULL UNIQUE,
    supplier_name         VARCHAR NOT NULL,      -- procurement's spelling
    country               VARCHAR NOT NULL,
    tier                  VARCHAR NOT NULL
);

-- One row per SKU per cost version (SCD Type 2 on unit_cost).
CREATE TABLE IF NOT EXISTS marts.dim_product (
    product_key           VARCHAR PRIMARY KEY,
    sku                   VARCHAR NOT NULL,
    product_name          VARCHAR NOT NULL,
    category              VARCHAR NOT NULL,
    supplier_key          VARCHAR NOT NULL,
    list_price            DECIMAL(18,2),
    unit_cost             DECIMAL(18,2),
    valid_from            DATE NOT NULL,
    valid_to              DATE,                  -- NULL: still current
    is_current            BOOLEAN NOT NULL,
    UNIQUE (sku, valid_from)
);

-- One row per account per version (SCD Type 2 on segment, owner and
-- status). Keyed by the Relay account id; legacy ids are resolved to it.
-- Contact details stay in staging (Chapter 24).
CREATE TABLE IF NOT EXISTS marts.dim_account (
    account_key           VARCHAR PRIMARY KEY,
    account_id            INTEGER NOT NULL,
    account_name          VARCHAR NOT NULL,
    region_key            VARCHAR NOT NULL,
    segment               VARCHAR NOT NULL,
    owner                 VARCHAR,
    status                VARCHAR NOT NULL,
    key_account           BOOLEAN NOT NULL,
    customer_since        DATE,
    closed_on             DATE,
    postcode              VARCHAR,               -- text: it has leading zeros
    valid_from            DATE NOT NULL,
    valid_to              DATE,                  -- NULL: still current
    is_current            BOOLEAN NOT NULL,
    UNIQUE (account_id, valid_from)
);

-- ------------------------------------------------------------ facts

-- One row per line of a placed, not-cancelled order.
CREATE TABLE IF NOT EXISTS marts.fct_order_lines (
    order_id              BIGINT NOT NULL,
    line_no               SMALLINT NOT NULL,
    order_number          BIGINT NOT NULL,       -- degenerate dimension
    date_key              INTEGER NOT NULL,      -- the depot's date
    account_key           VARCHAR NOT NULL,      -- version of that day
    product_key           VARCHAR NOT NULL,      -- cost of that day
    supplier_key          VARCHAR NOT NULL,
    region_key            VARCHAR NOT NULL,
    channel               VARCHAR NOT NULL,
    placed_at             TIMESTAMP NOT NULL,    -- UTC
    order_date            DATE NOT NULL,         -- the depot's date
    qty                   INTEGER NOT NULL,
    discount_pct          SMALLINT NOT NULL,
    unit_price            DECIMAL(18,2) NOT NULL,  -- after discount
    unit_cost             DECIMAL(18,2),
    revenue               DECIMAL(18,2) NOT NULL,  -- qty * unit_price
    cost                  DECIMAL(18,2),           -- qty * unit_cost
    gross_margin          DECIMAL(18,2),           -- revenue - cost
    PRIMARY KEY (order_id, line_no)
);

-- One row per placed, not-cancelled order: fct_order_lines rolled up.
CREATE TABLE IF NOT EXISTS marts.fct_orders (
    order_id              BIGINT PRIMARY KEY,
    order_number          BIGINT NOT NULL UNIQUE,
    date_key              INTEGER NOT NULL,
    account_key           VARCHAR NOT NULL,
    region_key            VARCHAR NOT NULL,
    channel               VARCHAR NOT NULL,
    placed_at             TIMESTAMP NOT NULL,
    order_date            DATE NOT NULL,
    line_count            SMALLINT NOT NULL,
    units                 INTEGER NOT NULL,
    revenue               DECIMAL(18,2) NOT NULL,
    cost                  DECIMAL(18,2),
    gross_margin          DECIMAL(18,2)
);

-- One row per ticket, its life from opened to closed (accumulating
-- snapshot: the row is updated as each milestone is reached).
CREATE TABLE IF NOT EXISTS marts.fct_tickets (
    ticket_id             VARCHAR PRIMARY KEY,
    account_key           VARCHAR,               -- NULL: not a customer
    region_key            VARCHAR,
    product_key           VARCHAR,               -- when a SKU is named
    opened_date_key       INTEGER NOT NULL,
    first_reply_date_key  INTEGER,
    solved_date_key       INTEGER,
    closed_date_key       INTEGER,
    opened_at             TIMESTAMP NOT NULL,
    first_reply_at        TIMESTAMP,
    solved_at             TIMESTAMP,
    closed_at             TIMESTAMP,
    channel               VARCHAR NOT NULL,
    category              VARCHAR,
    priority              VARCHAR,
    desk                  VARCHAR,
    status                VARCHAR NOT NULL,      -- as of the last load
    comment_count         SMALLINT NOT NULL,
    hours_to_first_reply  DECIMAL(10,2),
    hours_to_solve        DECIMAL(10,2)
);

-- One row per contract.
CREATE TABLE IF NOT EXISTS marts.fct_contracts (
    contract_id           INTEGER PRIMARY KEY,
    account_key           VARCHAR NOT NULL,      -- version valid at start
    region_key            VARCHAR NOT NULL,
    start_date_key        INTEGER NOT NULL,
    end_date_key          INTEGER NOT NULL,
    notice_date_key       INTEGER,
    start_date            DATE NOT NULL,
    end_date              DATE NOT NULL,
    term_months           SMALLINT NOT NULL,
    legacy_terms          BOOLEAN NOT NULL,
    discount_pct          SMALLINT,
    outcome               VARCHAR,               -- NULL: not yet ended
    cancellation_reason   VARCHAR
);

-- One row per contract per month end it is in force (periodic
-- snapshot). Its measure is semi-additive: sum it across contracts,
-- never across months.
CREATE TABLE IF NOT EXISTS marts.fct_contract_month (
    contract_id           INTEGER NOT NULL,
    month_end_date_key    INTEGER NOT NULL,
    account_key           VARCHAR NOT NULL,      -- version valid that day
    region_key            VARCHAR NOT NULL,
    month_end             DATE NOT NULL,
    months_in_force       SMALLINT NOT NULL,     -- 1 in its first month
    months_remaining      SMALLINT NOT NULL,     -- 0 in its last month
    discount_pct          SMALLINT,
    contracts_in_force    TINYINT NOT NULL DEFAULT 1,
    PRIMARY KEY (contract_id, month_end_date_key)
);

-- One row per portal session (Chapter 19 builds it from the stream).
CREATE TABLE IF NOT EXISTS marts.fct_portal_sessions (
    session_id            VARCHAR PRIMARY KEY,
    account_key           VARCHAR,
    region_key            VARCHAR,
    date_key              INTEGER NOT NULL,      -- the depot's date
    started_at            TIMESTAMP NOT NULL,
    ended_at              TIMESTAMP NOT NULL,
    device                VARCHAR,
    event_count           INTEGER NOT NULL,
    searches              INTEGER NOT NULL,
    product_views         INTEGER NOT NULL,
    add_to_carts          INTEGER NOT NULL,
    checkouts_started     INTEGER NOT NULL,
    orders_submitted      INTEGER NOT NULL
);

-- ------------------------------------------------------------ grains
-- Each table carries its grain in the catalogue, where a query can read
-- it: SELECT table_name, comment FROM duckdb_tables().

COMMENT ON TABLE marts.dim_date IS
    'One row per calendar day, 2021-01-01 to 2026-12-31.';
COMMENT ON TABLE marts.dim_region IS 'One row per region.';
COMMENT ON TABLE marts.dim_supplier IS
    'One row per supplier on procurement''s list.';
COMMENT ON TABLE marts.dim_product IS
    'One row per SKU per cost version.';
COMMENT ON TABLE marts.dim_account IS
    'One row per account per version.';
COMMENT ON TABLE marts.fct_order_lines IS
    'One row per line of a placed, not-cancelled order.';
COMMENT ON TABLE marts.fct_orders IS
    'One row per placed, not-cancelled order.';
COMMENT ON TABLE marts.fct_tickets IS
    'One row per ticket, from opened to closed.';
COMMENT ON TABLE marts.fct_contracts IS 'One row per contract.';
COMMENT ON TABLE marts.fct_contract_month IS
    'One row per contract per month end it is in force.';
COMMENT ON TABLE marts.fct_portal_sessions IS
    'One row per portal session.';
