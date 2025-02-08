from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

all_dag_params = Variable.get("dag_parameters", default_var="{}", deserialize_json=True)
dag_params = all_dag_params.get("dag_airflow_variables")
concurrency = dag_params.get("concurrency")
countries = dag_params.get("countries")
end_date_str = dag_params.get("end_date")
end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else None
start_date_str = dag_params.get("start_date")
start_date = datetime.strptime(start_date_str, "%Y-%m-%d") if start_date_str else None


def print_values():
    print(f"dag_params = {dag_params}")
    print(f"dag_params.concurrency = {concurrency}")
    print(f"dag_params.countries = {countries}")


with DAG(
    dag_id='dag_airflow_variables',
    start_date=start_date,
    end_date=end_date,
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Start processing"),
    )

    task = PythonOperator(
        task_id=f'airflow_variable_values',
        python_callable=print_values,

    )

    start >> task
