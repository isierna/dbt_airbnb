{{
   config(
      tags = ['consistency'],
      meta = {
      'impact':'high',
      'description':'Customer record should be created before or at the same time as their first order. A customer cannot place an order before they exist in the system.',
      'model':'orders'
      }
      )
}}

select
    c.customer_id,
    c.created_at as customer_created_at,
    min(o.created_at) as first_order_at
from {{ source('airbnb', 'customers') }} c
join {{ source('airbnb', 'orders') }} o
    on c.customer_id = o.customer_id
group by c.customer_id, c.created_at
having c.created_at > min(o.created_at)