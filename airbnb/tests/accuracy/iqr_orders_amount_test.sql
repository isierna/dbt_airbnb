{{
    config(
        severity = 'warn',
        tags = ['accuracy'],
        meta = {
            'impact':'high',
            'description':'Check if there are order_amounts that are > upper_iqr_fence and < lower_iqr_fence. All findings are subject for review.',
            'model':'iqr_orders'
        }
    )
}}

with iqr as (select percentile_cont(0.25) within group (order by order_amount) as q1,
                    percentile_cont(0.75) within group (order by order_amount) as q3,
                    q3 - q1 as iqr,
                    q1 - (1.5 * iqr) as lower_fence,
                    q3 + (1.5 * iqr) as upper_fence
            from {{ ref('iqr_orders') }})

select 
    order_id, order_amount, lower_fence, upper_fence 
from {{ ref('iqr_orders') }}
cross join iqr 
where order_amount < (select lower_fence from iqr) or order_amount > (select upper_fence from iqr)