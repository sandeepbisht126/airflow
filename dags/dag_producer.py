import os
import subprocess

from airflow import DAG
from airflow.datasets import Dataset
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="dag_producer",
    description="usage of airflow outlet/ inlet",
    max_active_runs=1,
    schedule_interval="@once",
    start_date=datetime.now() - timedelta(days=1)
)

bash_script_path = os.path.join(os.environ['AIRFLOW_HOME'], 'dags', 'script', 'airflow_macro.sh ')
py_script_path = os.path.join(os.environ['AIRFLOW_HOME'], 'dags', 'script', 'airflow_macro.py')


def print_log(dt):
    print(f'from python operator - yesterday day was - {dt} and type: {type(dt)}')
    print(f'script path is - {bash_script_path}')


def run_pyhon_script(dt):
    dt = subprocess.run(["python3", py_script_path, dt], check=True)
    print(f"dt is : {dt}")


workflow1 = BashOperator(
    task_id='echo_bash',
    bash_command='echo "today day is - {{ ds }}"',
    dag=dag,
    outlets=[Dataset("echo_bash_done")]
)

workflow3 = PythonOperator(
    task_id='print_python',
    python_callable=print_log,
    op_args=[" {{ ds }}"],
    dag=dag,
    outlets=[Dataset("print_python_done")]
)

workflow4 = PythonOperator(
    task_id='script_python',
    python_callable=run_pyhon_script,
    op_args=[" {{ ds }}"],
    # provide_context=True,
    dag=dag,
    outlets=[Dataset("script_python_done")]
)

workflow1 >> workflow3 >> workflow4
