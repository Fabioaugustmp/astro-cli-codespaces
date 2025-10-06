{% macro generate_schema_name(custom_schema_name=none, node=none) -%}

  {# If the model defines a schema in its config(), use it directly #}
  {%- if node is not none -%}
    {%- set model_schema = node.config.get('schema') -%}
  {%- else -%}
    {%- set model_schema = none -%}
  {%- endif -%}

  {%- if model_schema is not none and model_schema | trim != '' -%}
    {{ return(model_schema) }}
  {%- endif -%}

  {# Fallback to dbt's default behavior when no schema set on model #}
  {%- if custom_schema_name is none -%}
    {{ return(target.schema) }}
  {%- else -%}
    {{ return(target.schema ~ '_' ~ (custom_schema_name | trim)) }}
  {%- endif -%}

{%- endmacro %}
