{% macro generate_alias(column_name, suffics) %}
    {{ column_name ~ '_' ~ suffics }}
{% endmacro %}