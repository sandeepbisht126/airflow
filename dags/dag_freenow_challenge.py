from airflow import DAG
from datetime import datetime
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

dag = DAG(
    dag_id="dag_freenow_challenge",
    description="a default dag",
    start_date=datetime(2021, 7, 15),
    max_active_runs=1,
    schedule_interval="@once",
)

# TODO Task 3: Write an Airflow job to trigger the spark process


def print_log():
    print("workflow")


start_workflow = PythonOperator(
    task_id='start_workflow',
    python_callable=print_log,
    dag = dag
)

end_workflow = PythonOperator(
    task_id='end_workflow',
    python_callable=print_log,
    dag = dag
)

spark_config = {
    'conn_id': 'spark_local',
    'num_executors': 1,
    'executor_cores': 1,
    'executor_memory': '2g',
    'driver_memory': '1g'
}

task_1_newbookings = SparkSubmitOperator(
    task_id='task_1_newbookings',
    application='./data/passengerRunner.py',
    name='task_1_newbookings',
    dag=dag,
    **spark_config    
)

task_2_sessions = SparkSubmitOperator(
    task_id='task_2_sessions',
    application='./data/bookingRunner.py',
    name='task_2_sessions',
    dag=dag,
    **spark_config    
)

start_workflow >> [task_1_newbookings, task_2_sessions] >> end_workflow
