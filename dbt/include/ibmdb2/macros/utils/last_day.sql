{% macro ibmdb2__last_day(date, datepart) -%}

    {%- if datepart == 'quarter' -%}
        cast(last_day date({{ date }}) + (2 - mod(month({{ date }}) - 1, 3)) months) as date)
    {%- elif datepart == 'month' -%}
        cast(last_day({{ date }}) as date)
    {%- elif datepart == 'year' -%}
        cast(date(year({{ date }}) || '-12-31') as date)
    {%- else -%}
        {{ exceptions.raise_compiler_error("Unsupported datepart for macro last_day in ibmdb2: {!r}".format(datepart)) }}
    {%- endif -%}

{%- endmacro %}
