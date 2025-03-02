from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime


# def skip_task():
#     condition = False  # Change this based on your actual condition
#     if not condition:
#         raise AirflowSkipException("Skipping this task as condition is False")
#     print("Task is running")


def printres():
    print("Actual Task is running")


def print_skip():
    print("Skip Task is running")


skip_task = True
dag = DAG(
    "dag_skip_task",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False
)

if not skip_task:
    actual_task = PythonOperator(
        task_id="actual_task",
        python_callable=printres,
        dag=dag
    )
else:
    actual_task = PythonOperator(
        task_id="actual_task",
        python_callable=print_skip,
        dag=dag
    )

next_task = BashOperator(
    task_id="next_task",
    bash_command="echo 'Next task executed'",
    dag=dag
)

actual_task >> next_task
