{{ config(
    materialized='incremental',
    schema='profiling',
    tags=['profiling']
) }}

{% set tables = [
    {'name': 'orders', 'schema': 'RAW', 'identifier': 'CUSTOMER_ORDERS'},
    {'name': 'customers', 'schema': 'RAW', 'identifier': 'CUSTOMERS'}
] %}

{% for table in tables %}
select
    '{{ table.name }}' as table_name,
    (select count(*) from {{ source('airbnb', table.name) }}) as row_count,
    (select count(*) 
     from information_schema.columns 
     where table_schema = '{{ table.schema }}' 
     and table_name = '{{ table.identifier }}') as column_count,
    (select max(created_at) from {{ source('airbnb', table.name) }}) as last_data_at,
    current_timestamp() as profiled_at
{% if not loop.last %}union all{% endif %}
{% endfor %}