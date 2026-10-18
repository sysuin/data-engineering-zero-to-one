-- Book 2's training-table features, rewritten as SQL over Book 2's
-- warehouse: a second, independent computation to reconcile with.
CREATE OR REPLACE TEMP TABLE book2 AS
WITH tidy AS (                 -- Book 2's rule for the old CRM's ids
    SELECT account_id, crm_source, postcode, since,
           regexp_replace(regexp_replace(lower(trim(regexp_replace(
               name, '\s+', ' ', 'g'))), ' \(old\)$', ''),
               ' inc$', '') AS name
    FROM b2.accounts
),
ids AS (
    SELECT o.account_id AS legacy, n.account_id
    FROM tidy AS o JOIN tidy AS n
      ON n.postcode = o.postcode AND n.since = o.since
     AND n.name = o.name AND n.crm_source = 'Meridian CRM'
    WHERE o.crm_source = 'Legacy CRM'
),
rows AS (
    SELECT contract_id, account_id, CAST(end_date AS DATE) - 90 AS mark,
           outcome = 'not_renewed' AS not_renewed
    FROM b2.contracts
    WHERE end_date BETWEEN '2023-01-01' AND '2025-12-31'
      AND outcome IS NOT NULL
),
ord AS (
    SELECT coalesce(i.account_id, o.account_id) AS account_id,
           CAST(o.order_date AS DATE) AS day,
           sum(l.qty * l.unit_price) AS value
    FROM b2.orders AS o JOIN b2.order_lines AS l USING (order_id)
    LEFT JOIN ids AS i ON i.legacy = o.account_id
    GROUP BY o.order_id, 1, 2
),
tix AS (
    SELECT coalesce(i.account_id, t.account_id) AS account_id,
           CAST(substr(t.opened_at, 1, 10) AS DATE) AS day
    FROM b2.tickets AS t LEFT JOIN ids AS i ON i.legacy = t.account_id
),
o AS (
    SELECT r.contract_id, min(r.mark - x.day) AS since,
           count(*) FILTER (r.mark - x.day <= 90) AS n90,
           count(*) FILTER (r.mark - x.day BETWEEN 91 AND 180) AS p90,
           round(sum(x.value) FILTER (r.mark - x.day <= 365), 2)
               AS spend
    FROM rows AS r
    JOIN ord AS x ON x.account_id = r.account_id AND x.day < r.mark
    GROUP BY 1
),
t AS (
    SELECT r.contract_id, count(*) AS t90
    FROM rows AS r
    JOIN tix AS x ON x.account_id = r.account_id AND x.day < r.mark
                 AND x.day >= r.mark - 90
    GROUP BY 1
),
seg AS (                       -- the history row in force before it
    SELECT r.contract_id, arg_max(h.segment, h.valid_from) AS segment
    FROM rows AS r JOIN b2.account_history AS h
      ON h.account_id = r.account_id
     AND CAST(h.valid_from AS DATE) < r.mark
    GROUP BY 1
)
SELECT r.contract_id, r.account_id, o.since, coalesce(o.n90, 0) AS n90,
       coalesce(o.p90, 0) AS p90, coalesce(o.spend, 0) AS spend,
       coalesce(t.t90, 0) AS t90,
       r.mark - CAST(a.since AS DATE) AS tenure, seg.segment,
       r.not_renewed
FROM rows AS r
LEFT JOIN o USING (contract_id)
LEFT JOIN t USING (contract_id)
LEFT JOIN seg USING (contract_id)
LEFT JOIN b2.accounts AS a ON a.account_id = r.account_id
