{% macro null_handler(column_name) %}

COALESCE({{ column_name }}, 'UNKNOWN')

{% endmacro %}