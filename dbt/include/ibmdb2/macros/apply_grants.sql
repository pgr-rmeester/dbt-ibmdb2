{% macro ibmdb2__get_show_grant_sql(relation) %}

  {%- set database = case_relation_part(relation.quote_policy['database'], relation.database) -%}
  {%- set schema = case_relation_part(relation.quote_policy['schema'], relation.schema) -%}
  {%- set identifier = case_relation_part(relation.quote_policy['identifier'], relation.identifier) -%}

select distinct
    grantee AS "grantee",
    case 
        when selectauth = 'Y' then 'SELECT'
        when insertauth = 'Y' then 'INSERT'
        when updateauth = 'Y' then 'UPDATE'
        when deleteauth = 'Y' then 'DELETE'
        when alterauth = 'Y' then 'ALTER'
        when indexauth = 'Y' then 'INDEX'
    end as "privilege_type"
from sysibm.systabauth
where grantee != current sqlid
    and dbname = '{{ database }}'
    and ttname = '{{ identifier }}'
    and tcreator = '{{ schema }}'

{% endmacro %}

{% macro ibmdb2__call_dcl_statements(dcl_statement_list) %}
     {% for dcl_statement in dcl_statement_list %}
        {% do run_query(dcl_statement) %}
     {% endfor %}
{% endmacro %}