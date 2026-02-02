{{ config(
    materialized='table',
    schema='profiling',
    tags='profiling'
)}}

select 
    count(distinct(table_name)) as tables,
    sum(row_count) as rows_cnt,
    sum(column_count) as column_cnt
from {{ref('profile_table_overview')}}
where profiled_at = (
  select max(profiled_at)
  from {{ ref('profile_table_overview') }})
