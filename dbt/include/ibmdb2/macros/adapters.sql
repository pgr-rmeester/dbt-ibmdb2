
{% macro case_relation_part(quoting, relation_part) %}
  {% if quoting == False %}
    {%- set relation_part = relation_part|upper -%}
  {% endif %}
  {{ return(relation_part) }}
{% endmacro %}


{% macro ibmdb2__check_schema_exists(information_schema, schema) -%}

  {%- set database = case_relation_part(information_schema.quote_policy['database'], 'not_defined') -%}
  {# This schema will ignore quoting and therefore also upper vs lowercase #}
  {%- set schema = case_relation_part(information_schema.quote_policy['schema'], schema) -%}

  {% set sql -%}

    select count(*) 
    from sysibm.systables 
    where 
      dbname = '{{ database }}' and
      creator = '{{ schema }}'

  {%- endset %}

  {{ return(run_query(sql)) }}

{% endmacro %}


{% macro ibmdb2__create_schema(relation) -%}

  {%- call statement('create_schema') -%}

    {%- set database = case_relation_part(relation.quote_policy['database'], relation.without_identifier()) -%}
    {%- set schema = case_relation_part(relation.quote_policy['schema'], relation.without_identifier()) -%}

    {% set sql -%}
      select count(*)
      from sysibm.systables
      where dbname = '{{ database }}' and creator = '{{ schema }}';)
    {%- endset %}

    {%- if run_query(sql) == 0 -%}
      begin
        prepare stmt from 'create schema {{ database }}.{{ schema }}';
        execute stmt;
      end;
    {%- endif -%}  
  {%- endcall -%}
{% endmacro %}



{% macro ibmdb2__drop_schema(relation) -%}

  {%- call statement('drop_schema') -%}

  {%- set database = case_relation_part(relation.quote_policy['database'], relation.schema) -%}
  {%- set schema = case_relation_part(relation.quote_policy['schema'], relation.schema) -%}

begin
	for t as
    select 
      name as tabname, 
      creator as tabschema,
      (case when type='T' then 'TABLE' else 'VIEW' end) as type 
    from sysibm.systables 
    where creator = '{{ schema }}'
    
		do
			prepare stmt from 'drop '||t.type||' '||t.tabschema||'.'||t.tabname;
			execute stmt;
	end for;
  if exists (
    select distinct 
      creator as schemaname 
    from sysibm.systables 
    where dbname = '{{ database }}' and creator = '{{ schema }}'
  ) then
    prepare stmt from 'drop schema {{ schema }} restrict';
    execute stmt;
  end if;
end;

  {% endcall %}
{% endmacro %}


{% macro ibmdb2__create_table_as(temporary, relation, sql) -%}

  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set table_space = config.get('table_space', none) -%}
  {%- set organize_by = config.get('organize_by', none) -%}
  {%- set distribute_by = config.get('distribute_by', none) -%}

  {{ sql_header if sql_header is not none }}

  {# Ignore temporary table type #}
create table {{ relation }} as (
  {{ sql }}
)
with data

  {%- if table_space is not none -%}
    {{ ' ' }}
in {{ table_space | upper  }}
  {%- endif -%}

  {%- if organize_by is not none -%}
    {{ ' ' }}
organize by {{ organize_by | upper }}
  {%- endif -%}

  {%- if distribute_by is not none -%}
    {%- set distribute_by_type = distribute_by['type'] | lower -%}
    {{ ' ' }}
distribute by {{ distribute_by_type | upper }}
    {%- if distribute_by_type == 'hash' -%}
      {%- set distribute_by_columns = distribute_by['columns'] -%}
(
      {% for column in distribute_by_columns %}
{{ column }}
        {% if not loop.last %},{% endif %}
      {% endfor %}
)
    {%- endif -%}
  {%- endif -%}

{%- endmacro %}


{% macro ibmdb2__create_view_as(relation, sql) -%}
  {%- set sql_header = config.get('sql_header', none) -%}

  {{ sql_header if sql_header is not none }}
create view {{ relation }} as 
  {{ sql }}

{% endmacro %}


{% macro ibmdb2__get_columns_in_relation(relation) -%}

  {% call statement('get_columns_in_relation', fetch_result=True) %}

  {%- set database = case_relation_part(relation.quote_policy['database'], relation.database) -%}
  {%- set schema = case_relation_part(relation.quote_policy['schema'], relation.schema) -%}
  {%- set identifier = case_relation_part(relation.quote_policy['identifier'], relation.identifier) -%}
select
  trim(syscolumns.name) as "name",
  trim(syscolumns.typename) as "type",
  syscolumns.length as "character_maximum_length",
  syscolumns.length as "numeric_precision",
  syscolumns.scale as "numeric_scale"
from sysibm.syscolumns as syscolumns
inner join 
  sysibm.systables systables 
on 
  syscolumns.tbname = systables.name 
  and syscolumns.tbcreator = systables.creator
where 
  systables.dbname = '{{ database | upper }}' and
  syscolumns.tbcreator = '{{ schema | upper }}' and
  syscolumns.name = '{{ identifier | upper }}' and
  syscolumns.hidden in ('', 'N')
order by colno

  {% endcall %}
  {% set table = load_result('get_columns_in_relation').table %}
  {{ return(sql_convert_columns_in_relation(table)) }}
{% endmacro %}

{% macro ibmdb2__list_relations_without_caching(schema_relation) %}

  {%- set database = case_relation_part(schema_relation.quote_policy['database'], schema_relation.database) -%}
  {%- set schema = case_relation_part(schema_relation.quote_policy['schema'], schema_relation.schema) -%}
  
  {% call statement('list_relations_without_caching', auto_begin=False, fetch_result=True) -%}
    select
      dbname as "database",
      trim(creator) as "schema",
      trim(name) as "name",
      case
        when type = 'T' then 'table'
        when type = 'V' then 'view'
      end as "table_type"
    from sysibm.systables
    where
      dbname = '{{ database | upper }}' and
      creator = '{{ schema | upper }}' and
      type in ('T', 'V')
      with ur
  {%- endcall %}
  
  {{ return(load_result('list_relations_without_caching').table) }}
{% endmacro %}


{% macro ibmdb2__rename_relation(from_relation, to_relation) -%}

  {% call statement('rename_relation') -%}

  {%- if from_relation.is_table -%}
    rename table {{ from_relation }} to {{ to_relation.replace_path(schema=none) }}
  {%- endif -%}

  {% if from_relation.is_view %}
    {% do exceptions.raise_compiler_error('IBMDB2 Adapter error: Renaming of views is not supported.') %}
  {% endif %}

  {%- endcall %}
{% endmacro %}


{% macro ibmdb2__list_schemas(database) %}

  {% call statement('list_schemas', fetch_result=True, auto_begin=False) -%}

  select distinct 
    trim(creator) as "schema"
  from sysibm.systables
  where dbname = '{{ database | upper }}'

  {%- endcall %}
  {{ return(load_result('list_schemas').table) }}
{% endmacro %}

{% macro ibmdb2__drop_relation(relation) -%}

  {% call statement('drop_relation', auto_begin=False) -%}

  {%- set database = case_relation_part(relation.quote_policy['database'], relation.database) -%}
  {%- set schema = case_relation_part(relation.quote_policy['schema'], relation.schema) -%}
  {%- set identifier = case_relation_part(relation.quote_policy['identifier'], relation.identifier) -%}

  {%- set sql -%}
    select count(*)
    from sysibm.systables
    where
      dbname = '{{ database | upper }}' and
      creator = '{{ schema | upper }}' and
        name = '{{ identifier | upper }}' and
        type = (case
          when '{{ relation.type }}' = 'view' then 'V' else 'T'
        end);
    {%- endset -%}

    {%- if run_query(sql) == 0 -%}
      prepare stmt from 'drop {{ relation.type | upper }} {{ database | upper }}.{{ relation | upper }}';
      execute stmt;
      commit;
      end;
    {%- endif -%}  
  {%- endcall %}
{% endmacro %}

{% macro ibmdb2__get_columns_in_query(select_sql) %}

  {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}

select * from (
    {{ select_sql }}
) as dbt_sbq
where 0=1
fetch first 0 rows only

  {% endcall %}
  {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}


{% macro ibmdb2__truncate_relation(relation) %}

  {% call statement('truncate_relation') -%}
truncate table {{ relation }}
immediate
  {%- endcall %}
{% endmacro %}


{% macro ibmdb2__get_binding_char() %}
  {{ return('?') }}
{% endmacro %}


{% macro ibmdb2__snapshot_hash_arguments(args) -%}
    hash({%- for arg in args -%}
        coalesce(cast({{ arg }} as varchar ), '')
        {% if not loop.last %} || '|' || {% endif %}
    {%- endfor -%})
{%- endmacro %}


{% macro ibmdb2__post_snapshot(staging_relation) %}
    {% do adapter.truncate_relation(staging_relation) %}
    {% do adapter.drop_relation(staging_relation) %}
{% endmacro %}
