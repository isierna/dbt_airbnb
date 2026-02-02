{{ config(materialized='table') }}

SELECT 
    CURRENT_TIMESTAMP() as current_ts,
    '{{ run_started_at }}' as dbt_run_started_at