from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG


DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="hourly_data_quality",
    description="Build dbt models, run dbt tests, then scan serving tables with Soda.",
    schedule="7 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["quality", "dbt", "soda"],
) as hourly_quality:
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command="cd /opt/platform/dbt && dbt build --profiles-dir .",
        execution_timeout=timedelta(minutes=20),
    )
    soda_scan = BashOperator(
        task_id="soda_scan",
        bash_command="curl -fsS -X POST --max-time 660 http://quality-api:8000/scan",
        execution_timeout=timedelta(minutes=10),
    )
    dbt_build >> soda_scan


with DAG(
    dag_id="daily_cdc_reconciliation",
    description="Compare source and current-state row counts with a bounded streaming lag.",
    schedule="20 2 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["cdc", "reconciliation"],
) as reconciliation:
    BashOperator(
        task_id="compare_source_and_serving_counts",
        bash_command="python /opt/platform/services/python/reconcile.py",
        execution_timeout=timedelta(minutes=10),
        env={
            "MYSQL_HOST": "mysql",
            "MYSQL_DATABASE": "{{ var.value.get('mysql_database', 'inventory') }}",
            "MYSQL_USER": "{{ var.value.get('mysql_user', 'app_user') }}",
            "MYSQL_PASSWORD": "{{ var.value.get('mysql_password', 'local_app_password') }}",
            "POSTGRES_HOST": "postgres-source",
            "POSTGRES_DATABASE": "{{ var.value.get('postgres_database', 'appdb') }}",
            "POSTGRES_USER": "{{ var.value.get('postgres_user', 'app_user') }}",
            "POSTGRES_PASSWORD": "{{ var.value.get('postgres_password', 'local_app_password') }}",
            "STARROCKS_HOST": "starrocks",
            "STARROCKS_PORT": "9030",
            "STARROCKS_READER_USER": "analytics_reader",
            "STARROCKS_READER_PASSWORD": "local_analytics_reader",
        },
    )


with DAG(
    dag_id="daily_rag_context_refresh",
    description="Refresh the local platform context index when the AI profile is running.",
    schedule="40 3 * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["ai", "rag", "weaviate"],
) as rag_refresh:
    BashOperator(
        task_id="index_platform_context",
        bash_command=(
            "if curl -fsS --max-time 5 http://rag-api:8000/health >/dev/null; then "
            "curl -fsS -X POST --max-time 1200 http://rag-api:8000/index; "
            "else echo 'AI profile is not running; context refresh skipped'; fi"
        ),
        execution_timeout=timedelta(minutes=20),
    )
