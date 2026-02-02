{{
  config(
    materialized = 'incremental',
    unique_key = ['source_name', 'snapshotted_at']
    )
}}

with freshness_results as (
    select
        unique_id as source_name,
        max_loaded_at,
        snapshotted_at,
        datediff('hour', max_loaded_at, snapshotted_at) as hours_since_update,
        status  -- 'pass', 'warn', 'error'
    from {{ ref('elementary', 'dbt_source_freshness_results') }}
    qualify row_number() over (
        partition by source_name
        order by snapshotted_at desc
    ) = 1
)

select
    source_name,
    max_loaded_at as last_updated_at,
    hours_since_update,
    status as freshness_status,
    snapshotted_at,
    case
        when hours_since_update <= 12 then 'green'
        when hours_since_update <= 24 then 'yellow'
        else 'red'
    end as freshness_color
from freshness_results