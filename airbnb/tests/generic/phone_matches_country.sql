{% test phone_matches_country(model, column_name, country_column) %}

SELECT 
    m.{{ column_name }},
    m.{{ country_column }},
    cc.phone_prefix AS expected_prefix
FROM {{ model }} m
LEFT JOIN {{ ref('country_phone_codes') }} cc 
    ON m.{{ country_column }} = cc.country_code
WHERE m.{{ column_name }} IS NOT NULL
  AND cc.phone_prefix IS NOT NULL
  AND NOT STARTSWITH(m.{{ column_name }}, cc.phone_prefix)

{% endtest %}