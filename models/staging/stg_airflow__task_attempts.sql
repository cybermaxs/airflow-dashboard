{{ config(materialized='incremental', unique_key='attempt_id', incremental_strategy='delete+insert', on_schema_change='sync_all_columns') }}

{#
  Airflow 3 gives every try its own UUID:
  - task_instance         = the latest try of each (dag, task, run, map_index)
  - task_instance_history = earlier tries (moved there on retry/clear, id kept as task_instance_id)
  Union = one row per try. Rows deleted by `airflow db clean` stay in the local copy.
#}

{% set cols %} dag_id, task_id, run_id, map_index, try_number, state, start_date, end_date, duration, updated_at {% endset %}
{% set since %}
    {% if is_incremental() %} where updated_at > {{ incremental_since('updated_at', var('lookback_days')) }} {% endif %}
{% endset %}

with tries as (
    select id::varchar as attempt_id, {{ cols }} from {{ source('airflow', 'task_instance') }} {{ since }}
    union all
    select task_instance_id::varchar as attempt_id, {{ cols }} from {{ source('airflow', 'task_instance_history') }} {{ since }}
)

select * from tries
