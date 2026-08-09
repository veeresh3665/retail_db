{% macro amount_format(column_name) %}

ROUND({{ column_name }},2)

{% endmacro %}