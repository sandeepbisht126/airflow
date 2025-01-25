from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
with DAG(
    dag_id='dag_airflow_db_connect',
    default_args=default_args,
    description='A sample DAG to query the Airflow metadata database',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Manual trigger
    catchup=False,
) as dag:

    # Define the PostgresOperator task
    query_postgres_db = PostgresOperator(
        task_id='query_postgres_db',
        postgres_conn_id='postgres_conn_id',  # Connection ID from Airflow > Admin > Connections
        sql="""
            INSERT INTO session_tgt (id, data, expiry, session_id)
            SELECT id, data, expiry, session_id 
            FROM session_src 
            ORDER BY session_id DESC;
        """,
    )

    query_airflow_db = PostgresOperator(
        task_id='query_airflow_db',
        postgres_conn_id='airflow_db',  # Connection ID from Airflow > Admin > Connections
        sql="""
                SELECT * from dag;
            """,
    )

    # Task execution
    query_postgres_db
    query_airflow_db
