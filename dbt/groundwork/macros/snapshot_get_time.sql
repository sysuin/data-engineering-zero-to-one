-- The moment a snapshot stamps on the versions it opens and closes.
-- dbt-duckdb uses the clock; Groundwork uses the night of the export
-- the snapshot read, when one is given, so a replay of past nights
-- dates each version by its night and not by the day of the replay.
{% macro duckdb__snapshot_get_time() -%}
    {%- if var('night') -%}
        CAST(DATE '{{ var("night") }}' AS TIMESTAMP)
    {%- else -%}
        {{ current_timestamp() }}::TIMESTAMP
    {%- endif -%}
{%- endmacro %}
