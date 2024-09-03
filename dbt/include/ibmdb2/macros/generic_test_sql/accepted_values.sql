{% macro ibmdb2__test_accepted_values(model, column_name, values, quote=True) %}

    select
        {{ column_name }} as value_field,
        count(*) as n_records

    from {{ model }}
    where {{ column_name }} not in (
    {% for value in values -%}
        {% if quote -%}
        '{{ value }}'
        {%- else -%}
        {{ value }}
        {%- endif -%}
        {%- if not loop.last -%},{%- endif %}
    {%- endfor %}
    )
    group by {{ column_name }}
{% endmacro %}
