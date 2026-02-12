{{ config(
    materialized='incremental',
    schema='profiling',
    tags=['profiling']
) }}


{# {% set tables = [
    source('airbnb', 'orders'),
    source('airbnb', 'customers')
] %} #}

{% set tables = [
    {'name': 'orders', 'schema': 'RAW', 'identifier': 'ORDERS'},
    {'name': 'customers', 'schema': 'RAW', 'identifier': 'CUSTOMERS'}
] %}

{% for table in tables %}

{% set columns_query %}
    select column_name, data_type
    from {{ target.database }}.information_schema.columns
    where table_schema = '{{ table.schema }}'
    and table_name = '{{ table.identifier }}'
{% endset %}

{% set columns = run_query(columns_query) %}

{% for col in columns %}
select
    '{{ table.name }}' as table_name,
    '{{ col[0] }}' as column_name,
    '{{ col[1] }}' as data_type,
    count(*) as total_rows,
    count({{ col[0] }}) as non_null_count,
    sum(case when {{ col[0] }} is null then 1 else 0 end) as null_count,
    round(null_count * 100.0 / total_rows, 2) as null_percent,
    {% if col[1] in ('TEXT', 'VARCHAR', 'STRING', 'CHAR') %}
        sum (case when {{ col[0] }} = '' then 1 else 0 end) as empty_values,
        sum (case when lower({{ col[0] }}) in ('n/a', 'unknown', 'tbd', 'none', 'null', 'na', 'not available', '-', '–', '—', '.', '...', 'empty') then 1 else 0 end) as invalid_placeholders
    {% else %}
        0 as empty_values,
        0 as invalid_placeholders
    {% endif %},
    count(distinct {{ col[0] }}) as distinct_count,
    round(count(distinct {{ col[0] }}) * 100 / count({{ col[0] }}), 2) as cardinality_prcnt,
    {% if col[1] in ('NUMBER', 'DATE', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ', 'TIMESTAMP_TZ') %}
        min({{ col[0] }})::varchar as min_value,
        max({{ col[0] }})::varchar as max_value
    {% else %}
        null::varchar as min_value,
        null::varchar as max_value
    {% endif %},
    current_timestamp() as profiled_at,
    {# validity checks #}
    {% if col[1] in ('NUMBER', 'FLOAT', 'INTEGER', 'DECIMAL') %}
    sum(case when {{ col[0] }} < 0 then 1 else 0 end) as negative_values,
    sum(case when {{ col[0] }} = 0 then 1 else 0 end) as zero_values,
    sum(case when {{ col[0] }} > 1000000 then 1 else 0 end) as extreme_high_values,
    {% else %}
        0 as negative_values,
        0 as zero_values,
        0 as extreme_high_values,
    {% endif %}
    {% if col[1] in ('DATE', 'TIMESTAMP', 'TIMESTAMP_NTZ', 'TIMESTAMP_LTZ') %}
    -- Future dates (where they shouldn't be)
    sum(case when {{ col[0] }} > current_date() then 1 else 0 end) as future_dates,
    
    -- Too old dates (unrealistic)
    sum(case when {{ col[0] }} < '1900-01-01' then 1 else 0 end) as ancient_dates,
    
    -- Weekend dates (if business dates only)
    sum(case when dayofweek({{ col[0] }}) in (0, 6) then 1 else 0 end) as weekend_dates,
    {% else %}
        0::integer as future_dates,
        0::integer as ancient_dates,
        0::integer as weekend_dates,
    {% endif %}
    {% if col[1] in ('NUMBER', 'FLOAT', 'INTEGER', 'DECIMAL') %}
        -- ID fields should not be negative or zero
    sum(case 
        when '{{ col[0] }}' ilike '%_id' 
        and {{ col[0] }} <= 0 
        then 1 else 0 
    end) as invalid_id_values,

    -- Amount/Price fields should be non-negative
    sum(case 
        when '{{ col[0] }}' ilike any ('%amount%', '%price%', '%cost%', '%total%')
        and {{ col[0] }} < 0 
        then 1 else 0 
    end) as negative_financial_values,

    -- Percentage fields should be 0-100
    sum(case 
        when '{{ col[0] }}' ilike any ('%percent%', '%rate%', '%pct%')
        and ({{ col[0] }} < 0 or {{ col[0] }} > 100)
        then 1 else 0 
    end) as invalid_percentage_values,

    -- Quantity fields should be positive
    sum(case 
        when '{{ col[0] }}' ilike '%quantity%'
        and {{ col[0] }} <= 0 
        then 1 else 0 
    end) as invalid_quantity_values,
    {% else %}
    0 as invalid_id_values,
    0 as negative_financial_values,
    0 as invalid_percentage_values,
    0 as invalid_quantity_values,
    {% endif %}
    -- For string length consistency
    {% if col[1] in ('TEXT', 'VARCHAR', 'STRING') %}
        min(length({{ col[0] }})) as min_length,
        max(length({{ col[0] }})) as max_length,
        avg(length({{ col[0] }}))::int as avg_length,
        stddev(length({{ col[0] }}))::int as length_stddev,
    {% else %}
    0 as min_length,
    0 as max_length,
    0 as avg_length,
    0 as length_stddev,
    {% endif %}
    -- Check if text column contains only numbers (might be wrong type)
    {% if col[1] in ('TEXT', 'VARCHAR', 'STRING') %}
        sum(case 
            when {{ col[0] }} rlike '^[0-9]+$' 
            then 1 else 0 
        end) as looks_like_number,
        
        sum(case 
            when try_to_date({{ col[0] }}) is not null 
            then 1 else 0 
        end) as looks_like_date
    {% else %}
        0 as looks_like_number,
        0 as looks_like_date
    {% endif %}
from {{ source('airbnb', table.name) }}
{% if not loop.last %}union all{% endif %}
{% endfor %}

{% if not loop.last %}union all{% endif %}
{% endfor %}