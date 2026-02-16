{% macro extract_city(address_column) %}
    trim(
        case trim(split_part({{ address_column }}, ',', 2))
            when 'NYC' then 'New York'
            when 'LA' then 'Los Angeles'
            else split_part({{ address_column }}, ',', 2)
        end
    )
{% endmacro %}