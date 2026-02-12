{% test no_placeholder_values(model, column_name, additional_placeholders=[]) %}
    {% set default_placeholders= ['n/a', 'unknown', 'tbd', 'none', 'null', 'na', 'not available', '-', '–', '—', '.', '...', 'empty'] %}

    {% set all_placeholders = default_placeholders + additional_placeholders %}

    select {{ column_name }}
    from {{ model }}
    where lower(trim({{ column_name }})) in ('{{ all_placeholders | join("', '")}}')

{% endtest %}