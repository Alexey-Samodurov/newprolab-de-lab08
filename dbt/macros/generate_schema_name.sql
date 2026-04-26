{# 
  Override стандартного `generate_schema_name` из dbt-core.

  По умолчанию dbt конкатенирует target.schema + custom_schema:
    profile.schema=silver, model +schema=gold  =>  итог: "silver_gold".

  Это путает: в HMS/Trino появляется схема `silver_gold`, хотя в коде
  модели лежат в каталогах silver/ и gold/ и +location_root указывает
  на s3a://lake/silver и s3a://lake/gold соответственно.

  Здесь меняем поведение: если у модели задан `+schema` (custom_schema_name)
  — используем именно его как имя схемы. Если не задан — fallback на
  target.schema (значение из profiles.yml).
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
