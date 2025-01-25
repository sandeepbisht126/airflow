from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='dag_postgres_example',
    default_args=default_args,
    description='Example DAG to interact with Cloud SQL PostgreSQL',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Task to create a table
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='postgres_conn_id',  # Connection ID in Airflow
        sql="""
            CREATE TABLE IF NOT EXISTS example_table (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,
    )

    # Task to insert data
    insert_data = PostgresOperator(
        task_id='insert_data',
        postgres_conn_id='cloudsql_postgres',
        sql="INSERT INTO example_table (name) VALUES ('Airflow');",
    )

    # Task to select data
    select_data = PostgresOperator(
        task_id='select_data',
        postgres_conn_id='postgres_conn_id',
        sql="SELECT * FROM example_table;",
    )

    # Task dependencies
    create_table >> insert_data >> select_data
