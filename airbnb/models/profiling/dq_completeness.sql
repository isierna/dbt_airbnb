{{
    config(
        materialized='view',
        schema='profiling'
    )
}}

{% set tables = [
    {'name': 'customers', 'schema': 'RAW', 'identifier': 'CUSTOMERS'},
    {'name': 'orders', 'schema': 'RAW', 'identifier': 'ORDERS'}
] %}

{% for table in tables %}

    {% set columns_query %}
        select column_name, data_type
        from {{ target.database }}.information_schema.columns
        where table_schema = '{{ table.schema }}'
        and table_name = '{{ table.identifier }}'
    {% endset %}

    {% set columns = run_query(columns_query) %}

    {% for col in columns %}
        select
            '{{ table.identifier }}' as table_name,
            '{{ col[0] }}' as column_name,
            sum (CASE WHEN {{ col[0] }} is NULL THEN 1 ELSE 0 END) as nulls_count,
            {% if col[1] in ('TEXT', 'VARCHAR', 'STRING', 'CHAR') %}
                sum (case when {{ col[0] }} = '' then 1 else 0 end) as empty_values,
                sum (case when {{ col[0] }} in ('N/A', 'unknown', 'TBD', 'none', '-') then 1 else 0 end) as invalid_placeholders
            {% else %}
                0 as empty_values,
                0 as invalid_placeholders
            {% endif %}
        from {{ source('airbnb', table.name) }}

        {% if not loop.last %}union all{% endif %}
    {% endfor %}

    {% if not loop.last %}union all{% endif %}

{% endfor %}