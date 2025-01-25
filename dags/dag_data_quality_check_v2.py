from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from script.data_quality_check_v2 import DataQualityChecks


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
    params_list = kwargs["params"].get("dq_checks")
    for params in params_list:
        dq = DataQualityChecks(DB_CONFIG, params)  # Instantiate the class with DB config
        dq.run_checks()  # Run the checks


with DAG(
    "dag_data_quality_check_v2",
    description="Run data quality checks",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    data_quality_task = PythonOperator(
        task_id="data_quality_check",
        python_callable=run_data_quality_checks,
        params={
            "dq_checks": [
                {
                    "metric_name": "RcountCheck",
                    "table_name": "session_tgt",
                    "key_by_val": {"inserted_dt": {"gte": "2025-01-01", "lt": "2025-02-01"}, "data": "data1"},
                    "threshold": {"lower": 1},
                    "criticality": True,
                },
                {
                    "metric_name": "DuplicateCheck",
                    "table_name": "session_tgt",
                    "key_by_val": {"inserted_dt": {"gte": "2025-01-01", "lt": "2025-02-01"}},
                    "key_cols": ["data"],
                    "threshold": {"upper": 0},
                    "criticality": True,
                }
            ]
        },
        provide_context=True,
    )

    data_quality_task
