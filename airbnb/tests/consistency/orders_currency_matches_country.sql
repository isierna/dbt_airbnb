{{
   config(
      tags = ['consistency'],
      meta = {
      'impact':'high',
      'description':'Currency should match customer\'s country. US customers should use USD, DE customers should use EUR, etc.',
      'model':'orders'
      }
      )
}}

select
    o.order_id,
    o.currency,
    c.country,
    c.customer_id
from {{ source('airbnb', 'orders') }} o
join {{ source('airbnb', 'customers') }} c
    on o.customer_id = c.customer_id
where not (
    (c.country = 'US' and o.currency = 'USD') or
    (c.country = 'GB' and o.currency = 'GBP') or
    (c.country in ('FR', 'ES', 'IT', 'NL', 'BE', 'AT','DE') and o.currency = 'EUR')
)