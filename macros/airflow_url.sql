{#-
  Link into the Airflow 3 web UI. Base URL from AIRFLOW_UI_URL (.env); no URL set = no link (null).
  airflow_url('dag_id')                                  -> the DAG's grid
  airflow_url('dag_id', 'run_id')                        -> one run
  airflow_url('dag_id', 'run_id', 'task_id', 'map_index') -> one task instance (logs)
-#}
{% macro airflow_url(dag_id, run_id=none, task_id=none, map_index=none) -%}
{%- set base = env_var('AIRFLOW_UI_URL', '') -%}
{%- if base -%}
  (rtrim('{{ base }}', '/') || '/dags/' || url_encode({{ dag_id }})
  {%- if run_id %} || coalesce('/runs/' || url_encode({{ run_id }}), '') {%- endif %}
  {%- if task_id %} || coalesce('/tasks/' || url_encode({{ task_id }}), '') {%- endif %}
  {%- if map_index %} || case when {{ map_index }} >= 0 then '/mapped/' || {{ map_index }} else '' end {%- endif %})
{%- else -%}
  cast(null as varchar)
{%- endif -%}
{%- endmacro %}
