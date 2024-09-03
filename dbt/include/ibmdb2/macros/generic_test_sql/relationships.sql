{% macro ibmdb2__test_relationships(model, column_name, to, field) %}

select
    child.{{ column_name }} as from_field
from {{ model }} as child
left join {{ to }} as parent
    on child.{{ column_name }} = parent.{{ field }}                   
where child.{{ column_name }} is not null and parent.{{ field }} is null

{% endmacro %}
