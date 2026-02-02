{% test not_repeated_digit_phone(model, column_name, min_len=5) %}
with base as (
  select
    {{ column_name }} as raw_phone,
    -- keep only digits
    regexp_replace(cast({{ column_name }} as {{ dbt.type_string() }}), '[^0-9]', '') as digits
  from {{ model }}
),
invalid as (
  select *
  from base
  where digits is not null
    and length(digits) >= {{ min_len }}
    -- all digits the same (0000..., 1111..., etc.)
    and digits = repeat(substr(digits, 1, 1), length(digits))
)
select * from invalid
{% endtest %}