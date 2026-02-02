{{
    config(
    tags = ['validity'],
    meta = {
        'impact':'high',
        'description':'Minimum nights must be grater than 0.'
    }
    )
}}


SELECT
    *
FROM
    {{ ref('dim_listings_cleansed') }}
WHERE minimum_nights < 1
    LIMIT 10