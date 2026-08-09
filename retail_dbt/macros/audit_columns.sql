{% macro audit_columns() %}

CURRENT_TIMESTAMP() AS CREATED_AT,
CURRENT_TIMESTAMP() AS UPDATED_AT

{% endmacro %}