from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task_group
from datetime import datetime


# Define a simple Python function for the task
def process_country(country):
    print(f"Processing data for {country}")


# Define task group for incremental processing
@task_group(group_id="inc_country_processing_group", ui_color="#FFCC80")
def inc_country_tasks():
    tasks = []
    for country in ["USA", "India", "Germany", "Japan"]:
        task = PythonOperator(
            task_id=f"process_inc_{country}",
            python_callable=process_country,
            op_args=[country],
        )
        tasks.append(task)
    return tasks


# Define task group for full processing
@task_group(group_id="full_country_processing_group", ui_color="#80D8FF")
def full_country_tasks():
    tasks = []
    for country in ["USA", "India", "Germany", "Japan"]:
        task = PythonOperator(
            task_id=f"process_full_{country}",
            python_callable=process_country,
            op_args=[country],
        )
        tasks.append(task)
    return tasks


# Initialize DAG
default_args = {'start_date': datetime(2025, 1, 1)}
with DAG(
        "dag_task_groups_decorator",
        default_args=default_args,
        schedule_interval="@daily",
) as dag:
    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Start processing"),
    )

    # Call task groups
    inc_group = inc_country_tasks()
    full_group = full_country_tasks()

    for inc_task, full_task in zip(inc_group, full_group):
        inc_task >> full_task

    end = PythonOperator(
        task_id="end",
        python_callable=lambda: print("Processing completed"),
    )

    # Define start and end dependencies
    start >> inc_group
    full_group >> end
