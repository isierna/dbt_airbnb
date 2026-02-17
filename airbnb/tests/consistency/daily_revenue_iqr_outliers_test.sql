{{
   config(
      severity = 'warn',
      tags = ['consistency'],
      meta = {
      'impact':'high',
      'description':'We test where daily_revenue is an IQR outlier in comparison to other days.',
      'model':'iqr_orders'
      }
    )
}}

select 
    *
from {{ ref('dq_daily_revenue_consistency') }}
where status in ('LOW_ANOMALY', 'HIGH_ANOMALY')