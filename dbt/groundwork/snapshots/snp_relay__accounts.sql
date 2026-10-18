-- snp_relay__accounts: one row per account per version of its segment,
-- owner and status, as dbt's snapshots record them: each run compares
-- tonight's export with the versions it holds, closes the ones that
-- changed and opens new ones. It knows nothing from before its first
-- run. var('night') picks the export (default: the latest) and, through
-- macros/snapshot_get_time.sql, the moment the versions are stamped.
{% snapshot snp_relay__accounts %}
{{ config(
    unique_key='account_id',
    strategy='check',
    check_cols=['segment', 'owner_name', 'status'],
    invalidate_hard_deletes=True
) }}
SELECT account_id, account_name, region_name, segment, owner_name,
       status, snapshot_date
FROM {{ ref('stg_relay__accounts') }}
WHERE snapshot_date = {% if var('night') -%}
    DATE '{{ var("night") }}'
{%- else -%}
    (SELECT max(snapshot_date) FROM {{ ref('stg_relay__accounts') }})
{%- endif %}
{% endsnapshot %}
