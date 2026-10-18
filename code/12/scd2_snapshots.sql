-- SCD Type 2 from nightly snapshots: compare each night with the night
-- before, and start a version wherever an account's values differ.
WITH marked AS (
    SELECT night, account_id, segment, owner, status,
           (segment, owner, status) IS DISTINCT FROM
           lag((segment, owner, status)) OVER (
               PARTITION BY account_id ORDER BY night) AS changed
    FROM snapshots
)
SELECT account_id,
       night                                        AS valid_from,
       lead(night) OVER (PARTITION BY account_id
                         ORDER BY night)            AS valid_to,
       segment, owner, status
FROM marked
WHERE changed
