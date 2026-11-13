-- one row: the Airflow web UI base URL (AIRFLOW_UI_URL in .env), so boards can link runs and task logs. null = no links.
select nullif(rtrim('{{ env_var("AIRFLOW_UI_URL", "") }}', '/'), '') as base_url
