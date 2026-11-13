{#
  Returns a literal ("timestamptz '...'" or a number) instead of a subquery,
  so DuckDB's postgres extension can push the WHERE down to Postgres.
  A `(select max(...) from this)` subquery is NOT pushed down => full table scan.
#}
{% macro incremental_since(column, lookback_days=none) -%}
    {%- set sql -%}
        select max({{ column }}){% if lookback_days is not none %} - interval {{ lookback_days }} day{% endif %} from {{ this }}
    {%- endset -%}
    {%- set value = run_query(sql).columns[0].values()[0] if execute else none -%}
    {%- if value is none -%}
        null
    {%- elif lookback_days is none -%}
        {{ value }}
    {%- else -%}
        timestamptz '{{ value }}'
    {%- endif -%}
{%- endmacro %}
