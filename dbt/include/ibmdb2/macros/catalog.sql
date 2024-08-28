{% macro ibmdb2__get_catalog(information_schema, schemas) -%}

  {%- call statement('catalog', fetch_result=True) -%}
  
    with db as (
      select trim(name) as dbname, creator as schema 
      from sysibm.sysdatabase
      where ( -- moved this where clause up to eliminate unnecessary joins
        {%- for schema in schemas -%}
        creator = UPPER('{{ schema }}') {%- if not loop.last %} OR {% endif -%}
        {%- endfor -%}
      )
    ),
    tables as (
      select
        dbname, -- should be replaced by db  from profile
        creator as schema,
        trim(name) as name,
        createdby as owner, -- user id who the object
        case
          when type = 'T' then 'TABLE' -- Upcase here to work with tests
          when type = 'V' then 'VIEW'  -- Upcase here to work with tests
        end as type
      from sysibm.systables
      where type in ('T', 'V')
    ),
    columns as (
      select
        trim(name) as name,
        trim(coltype) as coltype,
        tbname,
        tbcreator as schema,
        colno
      from sysibm.syscolumns
    )
    select
      db.dbname as "table_database",
      db.schema as "table_schema",
      tables.name as "table_name",
      tables.type as "table_type",
      '' as "table_comment",
      columns.name as "column_name",
      columns.colno as "column_index",
      columns.coltype as "column_type",
      '' as "column_comment",
      tables.owner as "table_owner"
    from db 
    inner join tables
      on db.dbname = tables.dbname
        and db.schema = tables.schema
    inner join columns 
      on columns.tbname = tables.name 
          and columns.schema = tables.schema
    order by
      tables.schema,
      tables.name,
      columns.colno

  {%- endcall -%}

  {{ return(load_result('catalog').table) }}
{%- endmacro %}
