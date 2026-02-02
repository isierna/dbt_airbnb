{{ config(
    materialized='incremental',
    schema='profiling',
    tags=['profiling']
) }}

-- Orphan records: orders with non-existent customer_id
select
    'orders' as table_name,
    'orphan_records' as pattern_type,
    'customer_id not in customers table' as description,
    count(*) as issue_count,
    current_timestamp() as profiled_at
from {{ source('airbnb', 'orders') }} o
where not exists (
    select 1 from {{ source('airbnb', 'customers') }} c
    where c.customer_id = o.customer_id
)

union all

-- Date inconsistency: order_date after created_at
select
    'orders' as table_name,
    'date_inconsistency' as pattern_type,
    'order_date is after created_at' as description,
    count(*) as issue_count,
    current_timestamp() as profiled_at
from {{ source('airbnb', 'orders') }}
where order_date > created_at::date

union all

-- Outliers: amount outside typical range (using IQR method simplified)
select
    'orders' as table_name,
    'amount_outliers' as pattern_type,
    'amount is zero or negative' as description,
    count(*) as issue_count,
    current_timestamp() as profiled_at
from {{ source('airbnb', 'orders') }}
where amount <= 0

union all

-- Potential duplicates: same customer, date, amount
select
    'orders' as table_name,
    'potential_duplicates' as pattern_type,
    'same customer_id, order_date, amount' as description,
    count(*) - count(distinct customer_id || order_date || amount) as issue_count,
    current_timestamp() as profiled_at
from {{ source('airbnb', 'orders') }}

union all

-- Empty strings in customer names
select
    'customers' as table_name,
    'empty_strings' as pattern_type,
    'first_name or last_name is empty string' as description,
    count(*) as issue_count,
    current_timestamp() as profiled_at
from {{ source('airbnb', 'customers') }}
where trim(first_name) = '' or trim(last_name) = ''

union all

-- Invalid email format
select
    'customers' as table_name,
    'invalid_email' as pattern_type,
    'email missing @ or domain' as description,
    count(*) as issue_count,
    current_timestamp() as profiled_at
from {{ source('airbnb', 'customers') }}
where email not like '%@%.%'