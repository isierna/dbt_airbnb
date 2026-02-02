{{ config(
    materialized='incremental',
    schema='profiling',
    tags=['profiling']
) }}

{% set columns = [
    {'table': 'orders', 'column': 'status'},
    {'table': 'orders', 'column': 'currency'},
    {'table': 'customers', 'column': 'country'}
] %}

{% for col in columns %}
select
    '{{ col.table }}' as table_name,
    '{{ col.column }}' as column_name,
    {{ col.column }}::varchar as value,
    count(*) as row_count,
    round(
        count(*) * 100.0 / (select count(*) from {{ source('airbnb', col.table) }}), 
        2
    ) as percent,
    current_timestamp() as profiled_at
from {{ source('airbnb', col.table) }}
group by {{ col.column }}
{% if not loop.last %}union all{% endif %}
{% endfor %}