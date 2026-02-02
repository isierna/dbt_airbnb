{% macro learn_variables() %}
    {% set your_name_jinja = "Ira" %}
    {{ log("Hello " ~ your_name_jinja, info=True) }}

    {{ log("Hello dbt user " ~ var("user_name","NO user_name set!") ~ "!", info=True) }}
{% endmacro %}