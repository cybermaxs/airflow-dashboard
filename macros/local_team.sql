{#-
  Team names from the AIRFLOW_DAG_TEAMS env var (set in the git-ignored .env), so real team names never
  land in the repo:   AIRFLOW_DAG_TEAMS=fin:Finance,mkt:Marketing,ops:Operations
  Returns a SQL CASE on the prefix expression, or null when the variable is empty.
-#}
{% macro local_team(prefix_sql) -%}
    {%- set pairs = [] -%}
    {%- for item in env_var('AIRFLOW_DAG_TEAMS', '').split(',') if ':' in item -%}
        {%- do pairs.append(item.split(':', 1)) -%}
    {%- endfor -%}
    {%- if pairs -%}
        case {{ prefix_sql }}
        {%- for p in pairs %} when '{{ p[0] | trim | replace("'", "''") }}' then '{{ p[1] | trim | replace("'", "''") }}'{% endfor %} end
    {%- else -%}
        cast(null as varchar)
    {%- endif -%}
{%- endmacro %}
