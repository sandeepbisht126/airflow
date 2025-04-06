from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from script.utils import run_tasks


def run_task(**kwargs):
    dag_id = kwargs["params"].get("target_dag_id")
    task_id = kwargs["params"].get("target_task_id")

    run_tasks(dag_id, task_id)


with DAG(
    dag_id='dag_clear_tasks',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['maintenance', 'manual_trigger']
) as dag:

    run_script = PythonOperator(
        task_id="run_task",
        python_callable=run_task,
        params={
            "target_dag_id": "dag_postgres_parallel_load",  # Change this as needed
            "target_task_id": "insert_for_SG"  # Change this as needed
        }
    )
