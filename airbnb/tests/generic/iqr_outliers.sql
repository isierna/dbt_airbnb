{% test iqr_outliers(model, column_name, group_by=[], where=None, multiplier=1.5, min_n=20) %}

with base as (
    select
        {% if group_by and group_by|length > 0 %}
            {{ group_by | join(', ') }},
        {% endif %}
        {{ column_name }} as value
    from {{ model }}
    where {{ column_name }} is not null
    {% if where %}
      and ({{ where }})
    {% endif %}
),

stats as (
    select
        {% if group_by and group_by|length > 0 %}
            {{ group_by | join(', ') }},
        {% endif %}
        count(*) as n,
        percentile_cont(0.25) within group (order by value) as q1,
        percentile_cont(0.75) within group (order by value) as q3
    from base
    {% if group_by and group_by|length > 0 %}
      group by {{ group_by | join(', ') }}
    {% endif %}
),

bounds as (
    select
        {% if group_by and group_by|length > 0 %}
            {{ group_by | join(', ') }},
        {% endif %}
        n,
        q1,
        q3,
        (q3 - q1) as iqr,
        (q1 - {{ multiplier }} * (q3 - q1)) as lower_bound,
        (q3 + {{ multiplier }} * (q3 - q1)) as upper_bound
    from stats
    where n >= {{ min_n }}
)

select
    b.*
from base b
join bounds x
  {% if group_by and group_by|length > 0 %}
    on {% for g in group_by %} b.{{ g }} = x.{{ g }}{% if not loop.last %} and {% endif %}{% endfor %}
  {% else %}
    on 1=1
  {% endif %}
where b.value < x.lower_bound
   or b.value > x.upper_bound

{% endtest %}
