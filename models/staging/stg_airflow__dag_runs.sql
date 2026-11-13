{{ config(materialized='incremental', unique_key='dag_run_id', incremental_strategy='delete+insert', on_schema_change='sync_all_columns') }}

select
    r.id                                                                as dag_run_id,
    r.dag_id,
    r.run_id,
    r.run_type,
    r.state,
    r.run_after,     -- when the run was due
    r.start_date,
    r.end_date,
    r.clear_number,
    r.updated_at
from {{ source('airflow', 'dag_run') }} r
{% if is_incremental() %}
-- a literal, not a subquery: DuckDB pushes it down to Postgres (seconds instead of a full scan)
where r.updated_at > {{ incremental_since('updated_at', var('lookback_days')) }}
{% endif %}
