{{
   config(
      tags = ['uniqueness'],
      meta = {
      'impact':'high',
      'description':'No duplicate orders (same customer, date, amount, status, currency)'
      }
      )
}}

select
    customer_id,
    order_date,
    amount,
    status,
    currency,
    count(*) as duplicate_count
from {{ source('airbnb', 'orders') }}
group by customer_id, order_date, amount, status, currency
having count(*) > 1