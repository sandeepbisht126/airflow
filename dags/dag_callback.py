from time import sleep

from airflow.models import TaskInstance
from airflow.utils.state import State
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging


# The function you want to run when the state of any task changes.
def state_change_listener(context):
    task_instance = context['task_instance']
    state = task_instance.state
    task_id = task_instance.task_id
    execution_date = task_instance.execution_date

    # Check if the task is marked as failed externally
    if state == State.FAILED:
        logging.error(f"Task {task_id} has been externally marked as failed at {execution_date}.")
        print(f"Performing cleanup for externally failed task {task_id}...")

    else:
        logging.info(f"Task {task_id} finished with state {state} at {execution_date}.")


# Define the main task function
def hello_world():
    count = 1
    while True:
        print(f"Hello, world! - {count}")
        sleep(5)
        count += 1
        if count > 5:
            break

    raise ValueError("Simulated task failure.")


# Define the DAG
with DAG(
        dag_id='dag_callback',
        start_date=datetime(2025, 1, 1),
        schedule_interval=None,  # Run manually
        catchup=False,
) as dag:
    hello_task = PythonOperator(
        task_id='task_callback',
        python_callable=hello_world,
        # on_failure_callback=on_failure_callback
    )

    # Hook up the listener for state changes
    dag.on_task_instance_changed = state_change_listener

    hello_task  # Add the task to the DAG
