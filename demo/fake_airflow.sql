-- A made-up Airflow 3 metadata database, for `make demo`: the data team of an imaginary company,
-- 39 DAGs, 45 days of runs ending now, flaky APIs, retries, runs fixed by a clear, and a few things
-- going wrong right now. Only the tables and columns the dbt models read, with Airflow 3's names.
--
--   duckdb demo/fake_airflow.duckdb < demo/fake_airflow.sql
--
-- Random but repeatable: every draw is a hash of what it is for (DAG, run, task, try).

set TimeZone = 'UTC';
create schema if not exists public;

create or replace temp macro u(k) as (hash(k::varchar) % 1000003) / 1000003.0;                       -- uniform [0, 1)
create or replace temp macro gauss(k) as sqrt(-2 * ln(greatest(u(k || ':a'), 1e-9))) * cos(2 * pi() * u(k || ':b'));
create or replace temp macro secs(x) as to_microseconds((x * 1e6)::bigint);

create or replace temp table p as select date_trunc('minute', now()::timestamp) as now_ts;

-- every DAG: what it does, when, how heavy (scale), and the story it tells (scenario)
create or replace temp table spec0 as
select * from (values
    ('sales_ingestion_salesforce_daily',         'ingestion',      'daily',   2,    0,    null::int, ['accounts', 'opportunities', 'contacts', 'leads'],      1.0,  null::varchar,   null::varchar),
    ('sales_ingestion_gong_daily',               'ingestion',      'daily',   3,    0,    null, ['calls', 'transcripts'],                                   1.6,  null,            null),
    ('sales_transformation_revenue_daily',       'transformation', 'daily',   4,    0,    null, ['revenue', 'bookings', 'pipeline'],                        1.3,  'slower',        null),
    ('sales_export_crm_sync_daily',              'export',         'daily',   6,    30,   null, ['accounts', 'forecasts'],                                  0.7,  null,            null),
    ('sales_reporting_pipeline_weekly',          'reporting',      'weekly',  6,    0,    1,    ['pipeline', 'quota_attainment'],                           1.0,  null,            null),
    ('mkt_ingestion_google_ads_daily',           'ingestion',      'daily',   1,    0,    null, ['campaigns', 'ad_groups', 'keywords', 'clicks'],           1.2,  null,            null),
    ('mkt_ingestion_meta_ads_daily',             'ingestion',      'daily',   1,    15,   null, ['campaigns', 'ads', 'spend'],                              0.9,  null,            null),
    ('mkt_ingestion_linkedin_ads_daily',         'ingestion',      'daily',   1,    30,   null, ['campaigns', 'spend'],                                     0.6,  'removed',       null),
    ('mkt_ingestion_twitter_ads_daily',          'ingestion',      'daily',   1,    45,   null, ['campaigns', 'spend'],                                     0.5,  'paused',        null),
    ('mkt_transformation_attribution_daily',     'transformation', 'daily',   3,    30,   null, ['touchpoints', 'attribution', 'channels'],                 1.4,  'failing now',   'dbt_test_touchpoints'),
    ('mkt_export_ad_audiences_daily',            'export',         'daily',   null, null, null, ['audiences', 'lookalikes'],                                1.5,  'running long',  null),
    ('mkt_training_churn_model_weekly',          'training',       'weekly',  7,    0,    1,    []::varchar[],                                              1.0,  null,            null),
    ('fin_ingestion_stripe_daily',               'ingestion',      'daily',   0,    30,   null, ['charges', 'refunds', 'payouts', 'disputes', 'invoices'],  1.1,  null,            null),
    ('fin_ingestion_fx_rates_hourly',            'ingestion',      'hourly',  null, 5,    null, ['rates'],                                                  0.15, 'import error',  null),
    ('fin_transformation_ledger_daily',          'transformation', 'daily',   3,    0,    null, ['ledger', 'revenue_recognition', 'accruals', 'balances'],  1.8,  null,            null),
    ('fin_transformation_budget_vs_actual_daily','transformation', 'daily',   5,    15,   null, ['budget', 'actuals', 'variance'],                          0.8,  null,            null),
    ('fin_reporting_month_close_monthly',        'reporting',      'monthly', 6,    0,    null, ['pnl', 'balance_sheet', 'cash_flow'],                      2.5,  null,            null),
    ('fin_export_erp_daily',                     'export',         'daily',   7,    0,    null, ['journal_entries', 'vendors'],                             0.8,  null,            null),
    ('crm_ingestion_hubspot_hourly',             'ingestion',      'hourly',  null, 15,   null, ['contacts', 'deals', 'companies'],                         0.25, null,            null),
    ('crm_ingestion_zendesk_daily',              'ingestion',      'daily',   2,    45,   null, ['tickets', 'satisfaction'],                                0.8,  null,            null),
    ('crm_transformation_customer_360_daily',    'transformation', 'daily',   4,    30,   null, ['customers', 'segments', 'lifetime_value', 'health_scores'], 1.5, null,           null),
    ('crm_export_braze_hourly',                  'export',         'hourly',  null, null, null, ['user_attributes', 'events'],                              0.25, 'running',       null),
    ('pay_ingestion_adyen_daily',                'ingestion',      'daily',   1,    30,   null, ['transactions', 'settlements', 'chargebacks'],             1.3,  null,            null),
    ('pay_ingestion_paypal_daily',               'ingestion',      'daily',   1,    50,   null, ['transactions', 'balances'],                               0.7,  null,            null),
    ('pay_transformation_reconciliation_daily',  'transformation', 'daily',   3,    45,   null, ['matches', 'discrepancies'],                               1.2,  'failing 2',     'dbt_run_discrepancies'),
    ('product_ingestion_events_hourly',          'ingestion',      'hourly',  null, 0,    null, ['page_views', 'clicks', 'sessions'],                       0.35, null,            null),
    ('product_ingestion_app_store_daily',        'ingestion',      'daily',   4,    0,    null, ['reviews', 'downloads'],                                   0.5,  'late',          null),
    ('product_transformation_funnels_daily',     'transformation', 'daily',   5,    30,   null, ['funnels', 'retention', 'feature_usage'],                  1.6,  null,            null),
    ('product_transformation_experiments_daily', 'transformation', 'daily',   6,    0,    null, ['assignments', 'metrics', 'results'],                      1.1,  null,            null),
    ('product_export_metrics_api_hourly',        'export',         'hourly',  null, 30,   null, ['kpis'],                                                   0.2,  null,            null),
    ('ops_ingestion_jira_daily',                 'ingestion',      'daily',   2,    30,   null, ['issues', 'sprints'],                                      0.6,  null,            null),
    ('ops_maintenance_cleanup_daily',            'maintenance',    'daily',   23,   0,    null, ['logs', 'tmp_tables', 'xcom'],                             0.8,  null,            null),
    ('ops_maintenance_vacuum_weekly',            'maintenance',    'weekly',  2,    0,    7,    ['warehouse', 'staging'],                                   4.0,  null,            null),
    ('ops_reporting_data_quality_daily',         'reporting',      'daily',   8,    0,    null, ['freshness', 'volumes', 'schema_changes'],                 0.6,  null,            null),
    ('ops_export_tableau_extracts_daily',        'export',         'daily',   6,    0,    null, ['extracts'],                                               1.0,  'paused 2',      null),
    ('ops_backfill_events_manual',               'ingestion',      'manual',  null, null, null, ['events'],                                                 3.0,  null,            null),
    ('risk_ingestion_fraud_signals_hourly',      'ingestion',      'hourly',  null, 20,   null, ['devices', 'ip_scores'],                                   0.2,  null,            null),
    ('risk_scoring_transactions_daily',          'scoring',        'daily',   2,    15,   null, ['transactions'],                                           1.2,  null,            null),
    ('risk_training_fraud_model_weekly',         'training',       'weekly',  4,    0,    3,    []::varchar[],                                              1.5,  'failing weeks', 'train_model')
) t(dag_id, kind, every, at_hour, at_min, at_dow, subjects, scale, scenario, fail_task);

-- the two DAGs running right now are scheduled relative to now, so the demo always catches them mid-run
create or replace temp table spec as
select s.* replace (
    coalesce(s.scenario, 'ok') as scenario,
    case s.scenario when 'running long' then hour(now_ts - interval 190 minute) else s.at_hour end as at_hour,
    case s.scenario when 'running long' then minute(now_ts - interval 190 minute)
                    when 'running' then minute(now_ts - interval 3 minute) else s.at_min end as at_min)
from spec0 s, p;

-- tasks of each kind of DAG: stage (tasks of a stage run in parallel), name ({s} = one task per subject),
-- typical minutes, spread
create or replace temp table templates as
select * from (values
    ('ingestion',      0, 'check_source_available', 0.3,  0.3),
    ('ingestion',      1, 'extract_{s}',            6.0,  0.2),
    ('ingestion',      2, 'load_{s}',               2.5,  0.15),
    ('ingestion',      3, 'dbt_run_staging',        4.0,  0.15),
    ('ingestion',      4, 'dbt_test_staging',       1.2,  0.1),
    ('transformation', 0, 'wait_for_upstream',      6.0,  0.8),
    ('transformation', 1, 'dbt_run_{s}',            8.0,  0.15),
    ('transformation', 2, 'dbt_test_{s}',           1.5,  0.1),
    ('transformation', 3, 'refresh_dashboards',     2.0,  0.2),
    ('export',         0, 'wait_for_upstream',      4.0,  0.8),
    ('export',         1, 'query_{s}',              5.0,  0.2),
    ('export',         2, 'upload_{s}',             4.0,  0.25),
    ('export',         3, 'notify_slack',           0.1,  0.1),
    ('reporting',      0, 'wait_for_upstream',      8.0,  0.8),
    ('reporting',      1, 'compute_{s}',            6.0,  0.15),
    ('reporting',      2, 'render_report',          3.0,  0.1),
    ('reporting',      3, 'send_email',             0.2,  0.1),
    ('training',       0, 'build_features',         25.0, 0.15),
    ('training',       1, 'train_model',            55.0, 0.2),
    ('training',       2, 'evaluate_model',         8.0,  0.1),
    ('training',       3, 'register_model',         0.5,  0.1),
    ('maintenance',    0, 'cleanup_{s}',            5.0,  0.3),
    ('maintenance',    1, 'report_freed_space',     0.3,  0.1),
    ('scoring',        0, 'wait_for_upstream',      5.0,  0.8),
    ('scoring',        1, 'load_model',             1.0,  0.1),
    ('scoring',        2, 'score_{s}',              12.0, 0.15),
    ('scoring',        3, 'write_scores',           3.0,  0.15)
) t(kind, stage, pattern, base_min, sigma);

-- the usual suspects: chance that a try fails (rate limits, timeouts)
create or replace temp table flaky as
select * from (values
    ('mkt_ingestion_google_ads_daily',      'extract_keywords',      0.15),
    ('pay_ingestion_adyen_daily',           'extract_settlements',   0.10),
    ('sales_transformation_revenue_daily',  'dbt_run_bookings',      0.12),   -- warehouse timeouts, worse as it slows down
    ('crm_ingestion_hubspot_hourly',        'extract_contacts',      0.08),
    ('sales_ingestion_salesforce_daily',    'extract_opportunities', 0.08),
    ('crm_export_braze_hourly',             'upload_events',         0.05),
    ('risk_ingestion_fraud_signals_hourly', 'extract_ip_scores',     0.04),
    ('product_ingestion_events_hourly',     'load_sessions',         0.03)
) t(dag_id, task_id, p);

create or replace temp table tasks as
select
    s.dag_id,
    tp.stage,
    tp.sigma,
    x.task_id,
    tp.base_min * 60 * s.scale * (0.6 + 0.8 * u(s.dag_id || x.task_id))     as base_s,
    coalesce(f.p, case when x.task_id like 'wait_for%' or x.task_id like 'check_%' then 0
                       when x.task_id like 'extract_%' or x.task_id like 'upload_%' then 0.006
                       else 0.002 end)                                       as p,
    case when f.p is not null then 0.2 else 0.5 end                          as p2   -- a flaky API usually works on retry
from spec s
join templates tp using (kind),
     unnest(case when tp.pattern like '%{s}%' then list_transform(s.subjects, lambda x: replace(tp.pattern, '{s}', x))
                 else [tp.pattern] end) as x(task_id)
left join flaky f on f.dag_id = s.dag_id and f.task_id = x.task_id;

-- runs: every schedule tick of the last 45 days (6 months for the monthly one), up to now
create or replace temp table runs as
with ticks as (
    select s.*, ra as run_after
    from spec s, p,
         unnest(case s.every
             when 'hourly'  then generate_series(date_trunc('hour', now_ts - interval 45 day) + to_minutes(s.at_min), now_ts, interval 1 hour)
             when 'daily'   then generate_series(date_trunc('day', now_ts - interval 45 day) + to_hours(s.at_hour) + to_minutes(s.at_min), now_ts, interval 1 day)
             when 'weekly'  then generate_series(date_trunc('week', now_ts - interval 45 day) + to_days(s.at_dow - 1) + to_hours(s.at_hour), now_ts, interval 7 day)
             when 'monthly' then generate_series(date_trunc('month', now_ts - interval 180 day) + to_hours(s.at_hour), now_ts, interval 1 month)
             else [now_ts - interval 30 day - interval 3 hour, now_ts - interval 16 day - interval 1 hour, now_ts - interval 3 day - interval 5 hour]
         end) as t(ra)
    where case s.scenario
        when 'removed'      then ra < now_ts - interval 18 day    -- its file was deleted, nobody paused it
        when 'paused'       then ra < now_ts - interval 38 day
        when 'paused 2'     then ra < now_ts - interval 11 day
        when 'import error' then ra < now_ts - interval 5 hour    -- a broken import: no new runs since
        else true end
)
select
    *,
    dag_id || '|' || run_after                                                          as run_key,
    case when every = 'manual' then 'manual' else 'scheduled' end                       as run_type,
    row_number() over (partition by dag_id order by run_after desc)                     as rn_desc,
    row_number() over (partition by dag_id order by run_after) / count(*) over (partition by dag_id) as progress,
    run_after + secs(10 + 40 * u(dag_id || '|' || run_after || ':lag'))                 as run_start
from ticks;

-- one row per task per run (the 2 latest runs of the late DAG are stuck in the queue: no tasks yet)
create or replace temp table ti as
with base as (
    select
        r.dag_id, r.run_after, r.run_key, r.rn_desc, r.scenario, r.fail_task, r.run_start,
        t.task_id, t.stage, t.p, t.p2,
        r.run_key || '|' || t.task_id                                                   as k,
        case when r.scenario = 'running long' and r.rn_desc = 1 and t.task_id = 'upload_audiences' then 1e7  -- stuck
             else t.base_s
                * exp(t.sigma * gauss(r.run_key || '|' || t.task_id))
                * case when t.task_id like 'wait_for%' or t.task_id like 'check_%' then 1
                       when isodow(r.run_after) = 1 then 1.25            -- Monday: the weekend's data
                       when isodow(r.run_after) >= 6 then 0.75 else 1 end
                * case when r.scenario = 'slower' and t.task_id like 'dbt_run_%' then 1 + 2.5 * pow(r.progress, 2) else 1 end
        end                                                                             as dur,
        coalesce(t.task_id = r.fail_task, false) and (
               (r.scenario = 'failing now'   and r.rn_desc = 1)
            or (r.scenario = 'failing 2'     and r.rn_desc <= 2)
            or (r.scenario = 'failing weeks' and r.rn_desc <= 4))                       as forced
    from runs r
    join tasks t using (dag_id)
    where not (r.scenario = 'late' and r.rn_desc <= 2)
),
draws as (
    select *,
        u(k || ':f1') < p                                                               as o1,
        u(k || ':f1') < p and u(k || ':f2') < p2                                        as o2,
        u(k || ':f1') < p and u(k || ':f2') < p2 and u(k || ':f3') < 0.4 and rn_desc > 2 as o3  -- no surprise failure in the latest runs
    from base
)
select *,
    forced or o3                                                                        as f3,
    case when forced or o3 then 3 when o2 then 2 when o1 then 1 else 0 end              as n_failed
from draws;

-- a failed run is often cleared by hand a few hours later, and then succeeds
create or replace temp table run_fix as
select
    run_key,
    bool_or(f3) and not bool_or(forced) and any_value(run_after) < (select now_ts from p) - interval 1 day
        and u(run_key || ':clear') < 0.6                                                as cleared,
    1800 + 10800 * u(run_key || ':gap')                                                 as clear_gap
from ti
group by run_key;

create or replace temp table tries as
with t as (
    select ti.*, f.cleared, f.clear_gap, i as try_number,
        i <= n_failed                                                                   as try_failed,
        case when i <= n_failed then dur * (0.1 + 0.6 * u(k || ':d' || i)) else dur end as try_dur,
        case when i = 1 then 0 when i <= 3 then 300 else f.clear_gap end                as gap   -- retry delay 5 min
    from ti
    join run_fix f using (run_key),
         unnest(generate_series(1, case when f3 then 3 + f.cleared::int else n_failed + 1 end)) as g(i)
)
select *,
    sum(gap + try_dur) over (partition by k order by try_number) - try_dur              as start_off
from t;

-- stages run one after the other; a task that failed for good stops the stages after it
create or replace temp table stages as
with s as (
    select run_key, stage,
           max(start_off + try_dur) + 3                                                 as stage_s,
           bool_or(f3 and not cleared)                                                  as stage_failed
    from tries
    group by all
)
select *,
    coalesce(sum(stage_s) over (partition by run_key order by stage rows between unbounded preceding and 1 preceding), 0) as stage_off,
    min(stage) filter (stage_failed) over (partition by run_key)                        as fail_stage
from s;

create or replace temp table timed as
select t.*, s.fail_stage,
    t.run_start + secs(s.stage_off + 1 + 4 * u(t.k || ':j') + t.start_off)             as try_start,
    t.run_start + secs(s.stage_off + 1 + 4 * u(t.k || ':j') + t.start_off + t.try_dur) as try_end
from tries t
join stages s using (run_key, stage)
where s.fail_stage is null or t.stage <= s.fail_stage;

-- what Airflow would show right now: tries not started yet don't exist, tries under way are running
create or replace temp table seen as
select t.*,
    case when try_end <= now_ts then case when try_failed then 'failed' else 'success' end else 'running' end as state,
    case when try_end <= now_ts then try_end end                                        as end_date,
    case when try_end <= now_ts then try_dur end                                        as duration,
    row_number() over (partition by k order by try_number desc)                         as latest
from timed t, p
where try_start <= now_ts;

create or replace temp table run_fail as
select run_key, max(end_date) filter (f3 and not cleared and stage = fail_stage) as failed_at
from seen group by all;

-- tasks with no try yet: waiting for their turn, or never run because something upstream failed
create or replace temp table not_started as
select ti.*, case when fl.failed_at is not null and ti.stage > st.fail_stage then 'upstream_failed' end as state, fl.failed_at
from ti
left join (select distinct run_key, fail_stage from stages) st using (run_key)
left join run_fail fl using (run_key)
where ti.k not in (select k from seen);

create or replace table public.task_instance as
select md5(k || ':' || try_number) as id, dag_id, task_id, 'scheduled__' || strftime(run_after, '%Y-%m-%dT%H:%M:%S') || '+00:00' as run_id,
       -1 as map_index, try_number, state, try_start::timestamptz as start_date, end_date::timestamptz as end_date, duration,
       coalesce(end_date, try_start)::timestamptz as updated_at
from seen where latest = 1
union all
select md5(k || ':0'), dag_id, task_id, 'scheduled__' || strftime(run_after, '%Y-%m-%dT%H:%M:%S') || '+00:00',
       -1, 0, state, null, null, null, coalesce(failed_at, run_start)::timestamptz
from not_started;

create or replace table public.task_instance_history as
select md5(k || ':' || try_number) as task_instance_id, dag_id, task_id, 'scheduled__' || strftime(run_after, '%Y-%m-%dT%H:%M:%S') || '+00:00' as run_id,
       -1 as map_index, try_number, state, try_start::timestamptz as start_date, end_date::timestamptz as end_date, duration,
       end_date::timestamptz as updated_at
from seen where latest > 1;

create or replace table public.dag_run as
with ti_state as (
    select run_key,
           count(*) filter (state not in ('success', 'failed', 'upstream_failed') or state is null) as unfinished,
           max(end_date)                                                                as last_end,
           min(try_start) filter (try_number = 4)                                      as cleared_at
    from (select run_key, state, end_date, try_start, try_number from seen where latest = 1
          union all
          select run_key, state, null, null, 0 from not_started)
    group by all
)
select
    row_number() over (order by r.run_after, r.dag_id)                                  as id,
    r.dag_id,
    r.run_type || '__' || strftime(r.run_after, '%Y-%m-%dT%H:%M:%S') || '+00:00'        as run_id,
    r.run_type,
    case when s.run_key is null then 'queued'
         when s.unfinished > 0 then 'running'
         when f.failed_at is not null then 'failed'
         else 'success' end                                                             as state,
    r.run_after::timestamptz                                                            as run_after,
    case when s.run_key is null then null else coalesce(s.cleared_at, r.run_start) end::timestamptz as start_date,
    case when s.run_key is not null and s.unfinished = 0 then s.last_end end::timestamptz as end_date,
    case when s.cleared_at is not null then 1 else 0 end                                as clear_number,
    coalesce(case when s.unfinished = 0 then s.last_end end, p.now_ts)::timestamptz     as updated_at
from runs r
cross join p
left join ti_state s using (run_key)
left join run_fail f using (run_key);

-- the manual DAG's runs were triggered by hand
update public.task_instance set run_id = replace(run_id, 'scheduled__', 'manual__') where dag_id like '%_manual';
update public.task_instance_history set run_id = replace(run_id, 'scheduled__', 'manual__') where dag_id like '%_manual';

create or replace table public.dag as
select
    dag_id,
    scenario like 'paused%'                                                             as is_paused,
    scenario = 'removed'                                                                as is_stale,
    scenario = 'import error'                                                           as has_import_errors,
    case every
        when 'hourly'  then at_min || ' * * * *'
        when 'daily'   then at_min || ' ' || at_hour || ' * * *'
        when 'weekly'  then '0 ' || at_hour || ' * * ' || (at_dow % 7)
        when 'monthly' then '0 ' || at_hour || ' 1 * *'
    end                                                                                 as timetable_summary
from spec;

create or replace table public.import_error as
select
    1                                                                                   as id,
    (now_ts - interval 5 hour - interval 12 minute)::timestamptz                        as "timestamp",
    'dags/finance/fx_rates.py'                                                          as filename,
    'Traceback (most recent call last):
  File "/opt/airflow/dags/finance/fx_rates.py", line 9, in <module>
    from forex_python.converter import CurrencyRates
ModuleNotFoundError: No module named ''forex_python'''                                  as stacktrace
from p;

select 'fake Airflow ready: ' || (select count(*) from public.dag) || ' DAGs, '
       || (select count(*) from public.dag_run) || ' runs, '
       || (select count(*) from public.task_instance) + (select count(*) from public.task_instance_history) || ' task tries' as done;
