{% macro transform_title(title) %}
    {% do return(title | trim | replace('raw_','stg_') | replace('_data','')) %}
{% endmacro %}