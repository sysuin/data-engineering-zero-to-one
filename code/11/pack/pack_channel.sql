-- pack_channel: sales by channel and month, section 1 of the pack.
-- materialised: table
SELECT month, channel, count(*) AS lines,
       sum(value_cents) AS value_cents
FROM {{ ref('int_pack__lines') }}
GROUP BY month, channel
