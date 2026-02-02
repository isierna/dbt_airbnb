{{
   config(
      tags = ['consistency'],
      meta = {
      'impact':'high',
      'description':'Make sure that review date is not earlier than created_at of a listing',
      'model':'fct_reviews'
      }
      )
}}

SELECT r.listing_id
FROM {{ ref('dim_listings_cleansed') }} as lc
INNER JOIN {{ ref('fct_reviews') }} as r
   ON r.listing_id = lc.listing_id
WHERE r.review_date < lc.created_at
LIMIT 10