{{ config(
    materialized = 'view',
    tags = ['metrics']
) }}

with raw as (
    select
        line:"results" as results_array
    from {{ source('airbnb', 'RAW_DBT_RUN_RESULTS') }}
    qualify row_number() over (order by load_ts desc) = 1
),

flattened as (
    select
        value as result
    from raw,
    lateral flatten(input => results_array)
)

select
    result:"unique_id"::string      as unique_id,
    result:"status"::string         as status,          -- 'pass', 'fail', 'warn', 'error'
    result:"execution_time"::float  as execution_time,
    result:"timing"[0]:"started_at"::timestamp as started_at,
    result:"timing"[array_size(result:"timing")-1]:"completed_at"::timestamp as completed_at
from flattened
where result:"unique_id"::string like 'test.%'