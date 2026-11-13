-- one row per task instance (dag, task, run, map_index), all its tries rolled up
with tis as (
    select
        dag_id,
        task_id,
        run_id,
        map_index,
        arg_max(state, try_number)                                as final_state,
        count(*)                                                  as attempt_count,
        count(*) filter (state = 'failed')                        as failed_attempt_count,
        arg_max(state, try_number) = 'success' and count(*) > 1   as is_flaky,   -- succeeded only after a retry
        min(start_date)                                           as first_start_date,
        max(end_date)                                             as last_end_date,
        min(start_date)::date                                     as run_date,
        arg_max(duration, try_number)                             as final_duration_s,
        sum(duration)                                             as compute_s,  -- every try: what the workers spent
        sum(duration) filter (try_number < max_try)               as retried_s   -- tries thrown away by a retry
    from (
        select *, max(try_number) over (partition by dag_id, task_id, run_id, map_index) as max_try
        from {{ ref('stg_airflow__task_attempts') }}
    )
    group by dag_id, task_id, run_id, map_index
)

select t.*, d.team
from tis t
left join {{ ref('dim_dags') }} d using (dag_id)
