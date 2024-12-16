from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.triggers.external_task import ExternalTaskTrigger
from datetime import datetime

dag = DAG(
    dag_id="dag_consumer",
    start_date=datetime(2024, 12, 15),
    schedule_interval=None,
    catchup=False,
)

def process_data():
    print("Processing data after producer task completes.")


# Task using ExternalTaskTrigger to wait for producer_task1
wait_for_producer_task = PythonOperator(
    task_id="wait_for_producer_task",
    python_callable=lambda: print("Waiting for producer_task1 to complete"),
    dag=dag,
    trigger_rule="all_success",
    task_defer={
        "trigger": ExternalTaskTrigger(
            external_dag_id="dag_external_trigger_producer",  # DAG ID of the producer
            external_task_id="producer_task1",  # Task ID to wait for
            execution_date_fn=None,  # Optional: custom logic for execution date mapping
            timeout=600,  # Timeout in seconds
        ),
        "timeout": 600,  # Total time to wait before failing
    },
)

# Task to process data after waiting for producer_task1
process_data_task = PythonOperator(
    task_id="process_data",
    python_callable=process_data,
    dag=dag,
)

wait_for_producer_task >> process_data_task
