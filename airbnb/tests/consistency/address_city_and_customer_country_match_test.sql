{{
   config(
      tags = ['consistency'],
      meta = {
      'impact':'high',
      'description':'Address city should match customer country. A customer cannot be in a country that is not the same as the city of their address.',
      'model':'customers'
      }
      )
}}

with customers_with_city as (
    select
        customer_id,
        address,
        country,
        {{ extract_city('address') }} as extracted_city
    from {{ source('airbnb', 'customers') }}
)

select
    cf.customer_id,
    cf.customer_address,
    cf.customer_country,
    cwc.extracted_city,
    cf.customer_ccode,
    cf.nom_country,
    cf.nom_city,
    cf.nom_ccode
from airbnb.dev.customer_address_country_mismatches cf
join customers_with_city cwc
    on cf.customer_id = cwc.customer_id

