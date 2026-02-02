{% macro select_column(column_name, apply_upper=false) %}
    {% if apply_upper %}
        {{ column_name | upper}}
    {% else %}
        {{column_name}}
    {% endif %}
{% endmacro %}