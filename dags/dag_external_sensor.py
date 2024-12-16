from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset

dag = DAG(
    dag_id="dag_external_sensor",
    description="Consumer DAG that relies on producer outlets",
    schedule=[
        Dataset("script_python_done"),
    ],
    start_date=datetime(2024, 12, 15),
    max_active_runs=1,
    catchup=False,
)

# Deferrable ExternalTaskSensor
wait_for_producer_task = ExternalTaskSensor(
    task_id="wait_for_producer_task",
    external_dag_id="dag_producer",  # ID of the producer DAG
    external_task_id="echo_bash",  # ID of the task in the producer DAG
    poke_interval=0,  # Not needed for deferrable sensors
    mode="reschedule",  # Use 'reschedule' mode for deferrable execution
    timeout=600,  # Time to wait before failing
    deferrable=True,
    dag=dag,
)

# Downstream task
process_data = EmptyOperator(
    task_id="process_data",
    dag=dag,
)

# Set dependencies
wait_for_producer_task >> process_data
