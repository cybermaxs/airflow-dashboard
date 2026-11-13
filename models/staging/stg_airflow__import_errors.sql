{{ config(materialized='table') }}

-- DAG files Airflow cannot parse right now (small table, full reload). Their DAGs stop being scheduled.
select
    id                                                                          as import_error_id,
    "timestamp"                                                                 as failed_at,
    filename,
    list_filter(split(trim(stacktrace), chr(10)), lambda l: trim(l) != '')[-1]  as error   -- the last line says what broke
from {{ source('airflow', 'import_error') }}
