from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    dag_id="dag_external_trigger_producer",
    start_date=datetime(2024, 12, 15),
    schedule_interval="@once",
    catchup=False,
)

task1 = BashOperator(
    task_id="producer_task1",
    bash_command='echo "Task in producer completed"',
    dag=dag,
)
