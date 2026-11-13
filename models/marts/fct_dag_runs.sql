{#
  One row per DAG run.
  Clearing a run resets dag_run.start_date to the re-run, and a fixed run ends 'success':
  the task tries (task_instance_history keeps the earlier ones) tell when it really started and that it failed.
#}

with tries as (
    select
        dag_id,
        run_id,
        count(*) filter (state = 'failed')    as failed_attempt_count,
        min(start_date)                       as first_task_start
    from {{ ref('stg_airflow__task_attempts') }}
    group by all
),

runs as (
    select
        r.*,
        d.team,
        coalesce(t.failed_attempt_count, 0)                                     as failed_attempt_count,
        least(r.start_date, coalesce(t.first_task_start, r.start_date))         as first_start_date,
        r.clear_number > 0                                                      as was_cleared,
        r.state = 'failed' or (r.clear_number > 0 and coalesce(t.failed_attempt_count, 0) > 0) as had_failure
    from {{ ref('stg_airflow__dag_runs') }} r
    left join {{ ref('dim_dags') }} d using (dag_id)
    left join tries t using (dag_id, run_id)
)

select
    dag_run_id,
    dag_id,
    run_id,
    team,
    run_type,
    state,
    state = 'success'                                    as is_success,
    state in ('success', 'failed')                       as is_finished,
    was_cleared,
    had_failure,
    case when state = 'failed' then 'still failed'
         when had_failure      then 'fixed by re-run' end as failure_outcome,
    failed_attempt_count,
    run_after,
    first_start_date,
    first_start_date::date                               as run_date,
    end_date,
    epoch(end_date - start_date)                         as duration_s,          -- after a clear: the re-run only
    epoch(end_date - run_after)                          as completion_delay_s,  -- due -> data landed
    updated_at
from runs
