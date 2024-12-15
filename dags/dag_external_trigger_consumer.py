from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.triggers.external_task import TaskStateTrigger
from datetime import datetime, timedelta

dag = DAG(
    dag_id="dag_external_trigger_consumer",
    start_date=datetime(2024, 12, 15),
    schedule_interval=None,
    catchup=False,
)


def process_data():
    print("Processing data after producer task completes.")


# Task using TaskStateTrigger with PythonOperator.defer
wait_for_producer_task = PythonOperator.defer(
    task_id="wait_for_producer_task",
    python_callable=lambda: print("Waiting for producer task..."),
    trigger=TaskStateTrigger(
        dag_id="dag_external_trigger_producer",  # Producer DAG ID
        task_id="producer_task1",  # Producer task ID
        states=["success"],  # Task state to wait for
        execution_dates=[datetime(2024, 12, 15)],  # Specify execution date
    ),
    dag=dag,
)

# Task to process data after waiting for producer_task1
process_data_task = PythonOperator(
    task_id="process_data",
    python_callable=process_data,
    dag=dag,
)

# Define the dependency
wait_for_producer_task >> process_data_task
