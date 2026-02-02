{% set price_string = '99.50' | float%}
{% set quantity_string = '10' | int%}
{% set table_name = (' raw_customers_data ') |trim | replace('raw_','stg_') | replace('_data','') %}
{% set table_name1 = transform_title(' raw_customers_data1 ')%}
{% set columns = ['email', 'customer_id', 'first_name', 'last_name', 'email', 'customer_id']%}

{% if execute %}
    {% set query %}
        SELECT MAX(customer_id) FROM {{ source('airbnb','customers') }} 
    {% endset %}
    {% set result = run_query(query )%}
    {% set max_id = result.columns[0].values()[0] %}
{% else %}
    {% set max_id = -1 %}
{% endif %}

SELECT 
    {{ columns | unique | sort | join(', ') }}
FROM 
    {{ source('airbnb', 'customers') }}
WHERE
    customer_id = {{ max_id }}
--AND revenue > {{(price_string * quantity_string)}}  
