{{
    config(
        materialized='incremental',
        unique_key='score_date',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns'
    )
}}

with base as (
    select
        date_trunc('day', detected_at) as score_date,
        detected_at,
        status
    from dev_elementary.elementary_test_results
    {% if is_incremental() %}
      -- reprocess last 2 days (tweak to 1–3 based on lateness)
      where detected_at >= dateadd(day, -2, (select max(last_detected_at) from {{ this }}))
    {% endif %}
)

select
    score_date,
    max(detected_at) as last_detected_at,
    round(sum(case when status = 'pass' then 1 else 0 end) * 100.0 / count(*), 1) as quality_score,
    count(*) as total_tests,
    sum(case when status = 'pass' then 1 else 0 end) as passed_tests,
    sum(case when status != 'pass' then 1 else 0 end) as failed_tests
from base
group by 1
order by score_date desc