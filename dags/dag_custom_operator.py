from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from custom_operator import PostgresInsertOperator

with DAG(
    dag_id="dag_custom_operator",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:

    insert_task = PostgresInsertOperator(
        task_id="insert_data_from_staging",
        source="public.huge_table",
        target="public.huge_table_parallel",
        key_cols=["id"],  # Not used for INSERT, but required by signature
        postgres_conn_id="postgres_conn_id",
        execution_timeout=timedelta(seconds=30)
    )
