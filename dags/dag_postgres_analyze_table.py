from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from datetime import datetime

all_dag_params = Variable.get("dag_parameters", default_var="{}", deserialize_json=True)
dag_params = all_dag_params.get("dag_postgres_analyze_table")
end_date_str = dag_params.get("end_date")
end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else None
start_date_str = dag_params.get("start_date")
start_date = datetime.strptime(start_date_str, "%Y-%m-%d") if start_date_str else None

with DAG(
    dag_id='dag_postgres_analyze_table',
    start_date=start_date,
    end_date=end_date,
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Start processing"),
    )

    task = PostgresOperator(
        task_id=f'analyze_table',
        postgres_conn_id='postgres_conn_id_read',
        sql=f"select analyze_table('huge_table_parallel')",
    )

    start >> task
