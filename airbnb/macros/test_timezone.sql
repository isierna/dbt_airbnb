{% macro test_timezone() %}
    {% set result = run_query("SELECT CURRENT_TIMESTAMP() as ts") %}
    {{ log(result.columns[0].values()[0], info=True) }}
{% endmacro %}