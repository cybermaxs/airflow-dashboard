{#
  One row per scheduled DAG that is not paused: is it OK right now, and if not, since when.
  status, first match wins:
    removed      its file is gone from the DAG bundle, but it was never paused
    failing      its latest finished scheduled run failed
    running long a run is going for over 2x its usual p95 (and 15 min more than it)
    late         the next run should have landed by now: last delivered interval + schedule interval
                 + its p90 landing time (30 days) + 15 min
    running / on time
  attention: 'now' = broke recently, 'neglected' = removed, or failing / late for over max(7 days, 2 intervals).
  as_of = when dbt built this table (the local copy is complete up to then).
#}

with as_of as (select now() as ts),

sched as (
    select * from {{ ref('fct_dag_runs') }} where run_type = 'scheduled'
),

-- the current failing streak: finished scheduled runs after the latest success
streak as (
    select dag_id,
           count(*)                    as failed_in_a_row,
           min(end_date)               as failing_since,
           arg_max(run_id, run_after)  as latest_failed_run_id
    from (
        select *, count(*) filter (where state = 'success')
                    over (partition by dag_id order by run_after desc rows unbounded preceding) as successes_after
        from sched
        where is_finished
    )
    where successes_after = 0
    group by all
),

failed_tasks as (
    select t.dag_id,
           string_agg(t.task_id, ', ' order by t.task_id)  as failed_tasks
    from (select distinct dag_id, task_id, run_id from {{ ref('fct_task_instances') }} where final_state = 'failed') t
    join streak s on s.dag_id = t.dag_id and s.latest_failed_run_id = t.run_id
    group by all
),

runs as (
    select
        dag_id,
        max(run_after) filter (is_success)                              as last_delivered_due,
        max(end_date) filter (is_success)                               as last_landed,
        max(run_after)                                                  as last_due,
        arg_max(state, run_after) filter (is_finished)                  as latest_finished_state,
        median(completion_delay_s) filter (is_success and not was_cleared
          and run_after > (select ts from as_of) - interval 30 day) / 3600   as p50_landing_h,
        quantile_cont(completion_delay_s, 0.9) filter (is_success and not was_cleared
          and run_after > (select ts from as_of) - interval 30 day) / 3600   as p90_landing_h,
        quantile_cont(duration_s, 0.95) filter (is_success and not was_cleared
          and run_after > (select ts from as_of) - interval 30 day) / 60     as p95_run_min
    from sched
    group by all
),

-- any run type: a stuck manual run blocks the DAG too
running as (
    select dag_id,
           min(first_start_date)                  as running_since,
           arg_min(run_id, first_start_date)      as running_run_id
    from {{ ref('fct_dag_runs') }}
    where state = 'running' and first_start_date is not null
    group by all
),

s as (
    select
        d.dag_id,
        d.team,
        d.is_stale,
        d.schedule_interval_h,
        r.*,
        k.failed_in_a_row,
        k.failing_since,
        k.latest_failed_run_id,
        f.failed_tasks,
        g.running_since,
        g.running_run_id,
        epoch((select ts from as_of) - g.running_since) / 60                       as running_min,
        r.last_delivered_due
          + to_microseconds((coalesce(d.schedule_interval_h, 0) * 3600e6)::bigint)
          + to_microseconds((coalesce(r.p90_landing_h, 0) * 3600e6)::bigint)
          + interval 15 minute                                                      as expected_by,
        greatest(interval 7 day, to_microseconds((2 * coalesce(d.schedule_interval_h, 0) * 3600e6)::bigint)) as neglect_after,
        (select ts from as_of)                                                      as as_of
    from {{ ref('dim_dags') }} d
    join runs r using (dag_id)
    left join streak k using (dag_id)
    left join failed_tasks f using (dag_id)
    left join running g using (dag_id)
    where not d.is_paused
      and d.is_scheduled
      and (d.schedule_interval_h is not null or d.is_stale)
),

statused as (
    select *,
        case
            when is_stale                                                       then 'removed'
            when latest_finished_state = 'failed'                               then 'failing'
            when running_min > greatest(2 * p95_run_min, p95_run_min + 15)      then 'running long'
            when last_delivered_due is null or as_of > expected_by              then 'late'
            when running_since is not null                                      then 'running'
            else 'on time'
        end as status
    from s
),

dated as (
    select *,
        case status
            when 'removed'      then last_due
            when 'failing'      then failing_since
            when 'running long' then running_since
            when 'late'         then expected_by
        end as since
    from statused
)

select
    dag_id,
    team,
    case
        when schedule_interval_h is null then '?'
        when schedule_interval_h < 1     then round(schedule_interval_h * 60)::int || ' min'
        when schedule_interval_h < 24    then round(schedule_interval_h)::int || ' h'
        when schedule_interval_h < 168   then round(schedule_interval_h / 24)::int || ' d'
        when schedule_interval_h < 700   then round(schedule_interval_h / 168)::int || ' wk'
        else 'monthly'
    end                                                                             as every,
    status,
    case
        when status = 'removed'                                                     then 'neglected'
        when status in ('failing', 'late') and since < as_of - neglect_after       then 'neglected'
        when status in ('failing', 'late', 'running long')                          then 'now'
    end                                                                             as attention,
    case status
        when 'removed'      then 'gone from the code: pause or delete it'
        when 'failing'      then 'fix it, or pause it'
        when 'late'         then 'not delivering: check it, or pause it'
    end                                                                             as action,
    since,
    strftime(timezone('UTC', since), '%Y-%m-%d %H:%M')                              as since_utc,
    round(epoch(as_of - since) / 3600, 1)                                           as hours_since,
    case when status = 'failing' then failed_in_a_row end                           as failed_in_a_row,
    case when status = 'failing' then failed_tasks end                              as failed_tasks,
    strftime(timezone('UTC', last_landed), '%Y-%m-%d %H:%M')                        as last_landed,
    round(epoch(as_of - last_landed) / 3600, 1)                                     as hours_since_landed,
    round(p50_landing_h, 1)                                                         as usual_landing_h,
    round(p90_landing_h, 1)                                                         as p90_landing_h,
    round(greatest(epoch(as_of - expected_by), 0) / 3600, 1)                        as hours_late,
    round(running_min)                                                              as running_min,
    round(p95_run_min)                                                              as p95_run_min,
    {{ airflow_url('dag_id') }}                                                     as dag_url,
    {{ airflow_url('dag_id', "case status when 'failing' then latest_failed_run_id when 'running long' then running_run_id end") }} as run_url,
    as_of
from dated
order by case attention when 'now' then 0 when 'neglected' then 2 else 1 end,
         case status when 'failing' then 0 when 'running long' then 1 when 'late' then 2 when 'removed' then 3
                     when 'running' then 4 else 5 end,
         since, dag_id
