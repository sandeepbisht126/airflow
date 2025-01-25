from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from script.data_quality_check import DataQualityChecks


# Database configuration
DB_CONFIG = {
    "host": "host.docker.internal",
    "database": "postgres",
    "user": "postgres",
    "password": "postgres",
    "port": 5432
}


# Wrapper function for the Airflow task
def run_data_quality_checks(**kwargs):
    params = kwargs["params"]
    dq = DataQualityChecks(DB_CONFIG)  # Instantiate the class with DB config
    dq.run_checks(params)  # Run the checks


# Define the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_data_quality_check",
    default_args=default_args,
    description="Run data quality checks",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    data_quality_task = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_data_quality_checks,
        params={
            "RcountCheck": {
                "tableName": "session_tgt",
                "key_by_val": {"inserted_dt": {"gte": "2025-01-01", "lt": "2025-02-01"}, "data": "data1"},
                "threshold": {"lower": 1},
                "criticality": True
            },
            "DuplicateCheck": {
                "tableName": "session_tgt",
                "key_by_val": {"inserted_dt": {"gte": "2025-01-01", "lt": "2025-02-01"}},
                "key_col": "data",
                "threshold": {"upper": 0},
                "criticality": True
            }
        },
        provide_context=True,
    )

    data_quality_task
