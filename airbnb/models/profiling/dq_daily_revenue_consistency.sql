{{
    config(
        materialized='view',
        schema='profiling'
    )
}}

with daily_metrics as (select 
                            order_date,
                            count(*) as order_count,
                            sum(order_amount) as daily_revenue,
                            avg(order_amount) as average_daily_order,
                            sum(case when order_amount<0 then 1 else 0 end) as negative_order_count
                        from airbnb.dev.iqr_orders
                        group by order_date),
q1_q3 as (
   select
        percentile_cont(0.25) within group (order by daily_revenue) as q1,
        percentile_cont(0.75) within group (order by daily_revenue) as q3
   from daily_metrics
),

iqr as (
    select
        q1,
        q3,
        q3 - q1 as iqr,
        q1 - (1.5 * iqr) as lower_fence,
        q3 + (1.5 * iqr) as upper_fence
    from q1_q3
)

select 
    dm.order_date,
    dm.order_count,
    dm.daily_revenue,
    dm.average_daily_order,
    dm.negative_order_count,
    iqr.lower_fence,
    iqr.upper_fence,
    case when dm.daily_revenue < iqr.lower_fence then 'LOW_ANOMALY'
        when dm.daily_revenue > iqr.upper_fence then 'HIGH_ANOMALY'
        else '-' 
    end as status
from daily_metrics dm
cross join iqr
order by dm.order_date