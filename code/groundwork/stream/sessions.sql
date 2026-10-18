-- Chapter 19's additions to marts.fct_portal_sessions. Chapter 4
-- designed the table, and ddl/marts.sql stays as that chapter printed.
--
--   account_id      the portal's account. account_key stays NULL
--                   until the stream can look up the version of
--                   dim_account that Chapter 12 builds
--   is_bot          more events than a person makes: at least 100,
--                   30 or more a minute. Kept, flagged, and left out
--                   of the funnel
--   late_events     events that arrived after the row was written
--   clock_offset_s  seconds added to the device's times because its
--                   clock was more than a minute out (almost always 0)
--
-- ADD COLUMN IF NOT EXISTS, so running this twice is safe.
ALTER TABLE marts.fct_portal_sessions
    ADD COLUMN IF NOT EXISTS account_id INTEGER;
ALTER TABLE marts.fct_portal_sessions
    ADD COLUMN IF NOT EXISTS is_bot BOOLEAN;
ALTER TABLE marts.fct_portal_sessions
    ADD COLUMN IF NOT EXISTS late_events INTEGER;
ALTER TABLE marts.fct_portal_sessions
    ADD COLUMN IF NOT EXISTS clock_offset_s INTEGER;
