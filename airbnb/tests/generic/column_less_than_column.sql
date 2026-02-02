{% test column_less_than_column(model, column_name, compare_column) %}

select *
from {{ model }}
where {{ column_name }} IS NOT NULL 
  and {{ compare_column }} IS NOT NULL
  and try_to_date({{ column_name }}) >= {{ compare_column }}

{% endtest %}