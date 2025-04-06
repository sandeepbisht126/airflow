from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor

# Define the consumer DAG
consumer_dag = DAG(
    dag_id="dag_consumer",
    description="Consumer DAG that relies on producer outlets",
    schedule=[
        Dataset("echo_bash_done"),
        Dataset("print_python_done"),
        Dataset("script_python_done"),
    ],
    start_date=datetime(2024, 12, 15),
    max_active_runs=1,
    catchup=False,
)


def process_data(dataset):
    print(f"Processing data from updated dataset: {dataset}")


wait_for_producer_task = ExternalTaskSensor(
    task_id="Sensor_to_dag_producer_echo_bash",
    external_dag_id="dag_producer",  # ID of the producer DAG
    external_task_id="echo_bash",  # ID of the task in the producer DAG
    poke_interval=0,  # Not needed for deferrable sensors
    mode="reschedule",  # Use 'reschedule' mode for deferrable execution
    execution_date_fn=lambda dt: dt.replace(hour=0, minute=0, second=0, microsecond=0),
    timeout=600,  # Time to wait before failing
    # deferrable=True,
    dag=consumer_dag,
)


# Define tasks for each dataset
task1 = PythonOperator(
    task_id="process_echo_bash",
    python_callable=process_data,
    op_args=["Dataset: echo_bash_done"],
    dag=consumer_dag,
)

task2 = PythonOperator(
    task_id="process_print_python",
    python_callable=process_data,
    op_args=["Dataset: print_python_done"],
    dag=consumer_dag,
)

task3 = PythonOperator(
    task_id="process_script_python",
    python_callable=process_data,
    op_args=["Dataset: script_python_done"],
    dag=consumer_dag,
)

wait_for_producer_task >> task1
