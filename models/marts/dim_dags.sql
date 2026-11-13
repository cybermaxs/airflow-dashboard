-- one row per DAG: team (from the dag_id prefix) and its real schedule interval (from the run history)
with intervals as (
    select dag_id, median(gap_s) / 3600 as schedule_interval_h
    from (
        select dag_id, epoch(run_after - lag(run_after) over (partition by dag_id order by run_after)) as gap_s
        from {{ ref('stg_airflow__dag_runs') }}
        where run_type = 'scheduled'
    )
    where gap_s > 0
    group by all
)

select
    d.dag_id,
    split_part(d.dag_id, '_', 1)                                                  as dag_prefix,
    coalesce({{ local_team('split_part(d.dag_id, \'_\', 1)') }}, t.team, 'Unknown') as team,
    d.is_paused,
    d.is_stale,
    d.has_import_errors,
    d.timetable_summary,
    coalesce(d.timetable_summary, 'None') != 'None'                               as is_scheduled,
    i.schedule_interval_h
from {{ ref('stg_airflow__dags') }} d
left join {{ ref('dag_teams') }} t on t.dag_prefix = split_part(d.dag_id, '_', 1)
left join intervals i using (dag_id)
