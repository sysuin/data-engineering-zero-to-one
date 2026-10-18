-- A generic test of a range: the rows whose value lies outside
-- [min_value, max_value]. Either end may be left out. A NULL is not
-- out of range here; not_null says whether one is allowed.
{% test between(model, column_name, min_value=none, max_value=none) %}
SELECT {{ column_name }} AS value
FROM {{ model }}
WHERE {{ column_name }} IS NOT NULL
  AND (FALSE
  {%- if min_value is not none %}
       OR {{ column_name }} < {{ min_value }}
  {%- endif %}
  {%- if max_value is not none %}
       OR {{ column_name }} > {{ max_value }}
  {%- endif %})
{% endtest %}
