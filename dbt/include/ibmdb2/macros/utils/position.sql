{% macro ibmdb2__position(substring_text, string_text) %}

    locate({{ substring_text }}, {{ string_text }})
    
{%- endmacro -%}