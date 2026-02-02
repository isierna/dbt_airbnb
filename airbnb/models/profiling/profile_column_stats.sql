{{ config(
    materialized='incremental',
    schema='profiling',
    tags=['profiling']
) }}


{# {% set tables = [
    source('airbnb', 'orders'),
    source('airbnb', 'customers')
] %} #}

{% set tables = [
    {'name': 'orders', 'schema': 'RAW', 'identifier': 'ORDERS'},
    {'name': 'customers', 'schema': 'RAW', 'identifier': 'CUSTOMERS'}
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
    '{{ table.name }}' as table_name,
    '{{ col[0] }}' as column_name,
    '{{ col[1] }}' as data_type,
    count(*) as total_rows,
    count({{ col[0] }}) as non_null_count,
    sum(case when {{ col[0] }} is null then 1 else 0 end) as null_count,
    round(null_count * 100.0 / total_rows, 2) as null_percent,
    count(distinct {{ col[0] }}) as distinct_count,
    {% if col[1] in ('NUMBER', 'DATE', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ') %}
    min({{ col[0] }})::varchar as min_value,
    max({{ col[0] }})::varchar as max_value
    {% else %}
    null as min_value,
    null as max_value
    {% endif %},
    current_timestamp() as profiled_at
from {{ source('airbnb', table.name) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}

{% if not loop.last %}union all{% endif %}
{% endfor %}