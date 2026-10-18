-- Chapter 11's conventions, checked: no column above raw is a
-- floating-point DOUBLE or REAL, every *_cents column is a BIGINT,
-- and every *_at column a TIMESTAMP without a zone (UTC, by rule).
-- The columns that break them.
{{ config(tags=['quality']) }}
SELECT table_schema, table_name, column_name, data_type
FROM information_schema.columns
WHERE table_catalog = '{{ target.database }}'
  AND table_schema IN ('staging', 'intermediate', 'marts')
  AND (data_type IN ('DOUBLE', 'FLOAT', 'REAL')
       OR (column_name LIKE '%\_cents' ESCAPE '\'
           AND data_type <> 'BIGINT')
       OR (column_name LIKE '%\_at' ESCAPE '\'
           AND data_type <> 'TIMESTAMP'))
