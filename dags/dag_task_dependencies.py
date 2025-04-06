from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.baseoperator import chain
from datetime import datetime

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'catchup': False
}

# Initialize DAG
dag = DAG(
    'dag_parallel_tasks_dependency',
    default_args=default_args,
    schedule_interval=None
)

# Define tasks
T_ref = DummyOperator(task_id='T_ref', dag=dag)
T_cleanup = DummyOperator(task_id='T_cleanup', dag=dag)

# Single join point after T_ref and T_cleanup
join_task = DummyOperator(task_id='join_task', dag=dag)

# Define parallel tasks
task_list = [DummyOperator(task_id=f'T{i}', dag=dag) for i in range(1, 11)]

# Ensure T1-T10 start only after both T_ref and T_cleanup are complete
[T_ref, T_cleanup] >> join_task >> task_list  # This sets both as dependencies
