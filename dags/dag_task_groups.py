from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime


# Define a simple Python function for the task
def process_country(country):
    print(f"Processing data for {country}")


# Initialize DAG
default_args = {'start_date': datetime(2025, 1, 1)}
with DAG(
        "dag_task_groups",
        default_args=default_args,
        schedule_interval="@daily",
) as dag:
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Start processing"),
    )

    # Task group for incremental processing
    with TaskGroup("inc_country_processing_group", ui_color="#FFCC80") as inc_country_group:
        for country in ["USA", "India", "Germany", "Japan"]:
            PythonOperator(
                task_id=f"process_inc_{country}",
                python_callable=process_country,
                op_args=[country],
            )

    # Task group for full processing
    with TaskGroup("full_country_processing_group", ui_color="#80D8FF") as full_country_group:
        for country in ["USA", "India", "Germany", "Japan"]:
            PythonOperator(
                task_id=f"process_full_{country}",
                python_callable=process_country,
                op_args=[country],
            )

    # Define dependencies
    start >> inc_country_group >> full_country_group >> PythonOperator(
        task_id="end",
        python_callable=lambda: print("Processing completed"),
    )
