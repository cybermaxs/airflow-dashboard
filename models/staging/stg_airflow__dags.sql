{{ config(materialized='table') }}

-- current state of each DAG (small table, full reload)
select
    d.dag_id,
    d.is_paused,
    d.is_stale,             -- its file is gone from the DAG bundle (Airflow 3; was `not is_active`)
    d.has_import_errors,
    d.timetable_summary
from {{ source('airflow', 'dag') }} d
