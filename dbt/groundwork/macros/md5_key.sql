-- Groundwork's surrogate key (DECISIONS §2): the first 16 hex
-- characters of the MD5 of the natural key, and for a version of an
-- SCD2 row, '|' and the day the version begins.
{% macro md5_key(natural_key, valid_from=none) -%}
    substr(md5(CAST({{ natural_key }} AS VARCHAR)
    {%- if valid_from is not none %} || '|'
               || strftime({{ valid_from }}, '%Y-%m-%d')
    {%- endif %}), 1, 16)
{%- endmacro %}
