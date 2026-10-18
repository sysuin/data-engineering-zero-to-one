-- What roles.py writes down, as PostgreSQL would enforce it. Not run:
-- DuckDB has no users to grant to.

CREATE ROLE analyst NOLOGIN;
CREATE ROLE manager_southwest NOLOGIN;
GRANT analyst TO tomas;                    -- a person gets a role

-- Schema and table grants: the analyst reads the marts and the masked
-- tables, and nothing in raw or staging.
GRANT USAGE ON SCHEMA marts, masked TO analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA marts, masked TO analyst;
REVOKE ALL ON SCHEMA raw, staging FROM analyst;

-- Column grants: everything in the portal's sessions but session_id.
REVOKE SELECT ON marts.fct_portal_sessions FROM analyst;
GRANT SELECT (account_key, region_key, date_key, started_at, ended_at,
              device, event_count, orders_submitted)
    ON marts.fct_portal_sessions TO analyst;

-- Row-level security: the Southwest manager's queries see Southwest's
-- rows, whatever the query says.
ALTER TABLE marts.fct_orders ENABLE ROW LEVEL SECURITY;
CREATE POLICY southwest_only ON marts.fct_orders
    FOR SELECT TO manager_southwest
    USING (region_key = (SELECT region_key FROM marts.dim_region
                         WHERE region_name = 'Southwest'));
GRANT SELECT ON marts.fct_orders TO manager_southwest;
