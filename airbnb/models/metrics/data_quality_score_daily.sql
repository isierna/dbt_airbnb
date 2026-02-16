{{
    config(
        materialized='incremental',
        unique_key='invocation_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with base as (
    select 
        invocation_id
    from {{ ref('elementary_test_results') }}
    group by invocation_id 
    order by max(to_timestamp_ntz(detected_at)) desc
    limit 1
)

select
    invocation_id,
    max(detected_at) as last_detected_at,
    round(sum(case when status = 'pass' then 1 else 0 end) * 100.0 / count(*), 1) as quality_score,
    count(*) as total_tests,
    sum(case when status = 'pass' then 1 else 0 end) as passed_tests,
    sum(case when status != 'pass' then 1 else 0 end) as failed_tests
from {{ ref('elementary_test_results') }} where invocation_id  in (select invocation_id from base)
group by invocation_id
