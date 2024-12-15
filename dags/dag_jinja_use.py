import os
import subprocess

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

dag = DAG(
    dag_id="dag_jinja_use",
    description="usage of airflow macro",
    max_active_runs=1,
    schedule_interval="@once",
    start_date=datetime.now() - timedelta(days=5)
)

bash_script_path = os.path.join(os.environ['AIRFLOW_HOME'], 'dags', 'script', 'airflow_macro.sh ')
py_script_path = os.path.join(os.environ['AIRFLOW_HOME'], 'dags', 'script', 'airflow_macro.py')


def print_log(dt, jinja_ds):
    print(f'from python operator - yesterday day was - {dt} and type: {type(dt)}')
    print(f'from param_dict - jinja_ds was - {jinja_ds} and type: {type(jinja_ds)}')
    print(f'script path is - {bash_script_path}')


def run_pyhon_script(dt):
    dt = subprocess.run(["python3", py_script_path, dt], check=True)
    print(f"dt is : {dt}")


workflow1 = BashOperator(
    task_id='echo_bash',
    bash_command='echo "today day is - {{ ds }}"',
    dag=dag
)

workflow2 = BashOperator(
    task_id='script_bash',
    bash_command=f"{bash_script_path}" + "{{ ds }}",
    dag=dag
)

workflow4 = PythonOperator(
    task_id='script_python',
    python_callable=run_pyhon_script,
    op_args=[" {{ ds }}"],
    # provide_context=True,
    dag=dag
)

workflow5 = BashOperator(
    task_id='python_bash',
    bash_command=f"python {py_script_path} " + "{{ds}}",
    dag=dag
)

workflow1 >> workflow2 >> workflow4 >> workflow5
