-- By default dbt builds a model with +schema: staging into a schema
-- called <target schema>_staging, so that two people's builds do not
-- collide. Groundwork's dev target is a separate file, so the schema
-- is the folder's name, as it was for the runner.
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name or target.schema }}
{%- endmacro %}
