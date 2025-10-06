{% macro drop_schema(schema_name) %}
    {% do run_query("DROP SCHEMA IF EXISTS " ~ schema_name ~ " CASCADE") %}
{% endmacro %}