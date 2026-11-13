-- DAGs whose prefix has no team: add it to AIRFLOW_DAG_TEAMS in .env (or seeds/dag_teams.csv)
{{ config(severity='warn') }}
select dag_id, dag_prefix from {{ ref('dim_dags') }} where team = 'Unknown'
