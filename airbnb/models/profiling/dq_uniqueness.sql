{{
    config(
        materialized='view',
        schema='profiling'
    )
}}

{% set tables = [
    {'name': 'customers', 'schema': 'RAW', 'identifier': 'CUSTOMERS'},
    {'name': 'orders', 'schema': 'RAW', 'identifier': 'ORDERS'}
]%}

{% for table in tables %}

    {% set columns_query %}
        select column_name, data_type
        from {{ target.database }}.information_schema.columns
        where table_schema = '{{ table.schema }}'
        and table_name = '{{ table.identifier }}'
    {% endset %}

    {% set columns = run_query(columns_query) %}

    {% for column in columns %}
        select
            '{{ table.identifier }}' as table_name,
            '{{ column[0] }}' as column_name,
            count(*) as total_records,
            count(distinct {{ column[0] }}) as unique_records,
            count({{ column[0] }}) - count(distinct {{ column[0] }}) as duplicates
        from {{ source('airbnb', table.name) }}

        {% if not loop.last %} union all {% endif %}
    {% endfor %}

            {% if not loop.last %} union all {% endif %}
{% endfor %}