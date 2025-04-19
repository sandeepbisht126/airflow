from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import task_group
from airflow.operators.dummy import DummyOperator


def parallel_load(cc):
    @task_group(group_id=f"tg_insert_for_{cc}", ui_color="#80D8FF")
    def load_tasks():
        tasks = []
        parallel_thread = 6
        for id in range(parallel_thread):
            insert_parallel = PostgresOperator(
                task_id=f'insert_for_{cc}_chunk_{id}',
                postgres_conn_id='postgres_conn_id',  # Connection ID from Airflow > Admin > Connections
                sql=f"""
                    INSERT INTO huge_table_parallel
                    SELECT * from huge_table
                    WHERE country = '{cc}'
                    AND mod(id, {parallel_thread}) = {id} limit 1000
                """,
            )
            tasks.append(insert_parallel)

        # Completion marker outside the TaskGroup
        all_chunk_done = DummyOperator(task_id=f"tg_insert_for_{cc}_completed")
        for chunk_task in tasks:
            start >> chunk_task
            chunk_task >> all_chunk_done

    load_tasks()


def sequencial_load(cc):
    insert_parallel_tb = PostgresOperator(
        task_id=f'insert_for_{cc}',
        postgres_conn_id='postgres_conn_id',
        sql=f"""
                INSERT INTO huge_table_parallel
                SELECT * FROM huge_table
                WHERE country = '{cc}' limit 100
            """,
    )
    start >> insert_parallel_tb


with DAG(
    dag_id='dag_postgres_parallel_load',
    description='A sample DAG to test parallel load in postgresql',
    start_date=datetime(2025, 1, 1),
    schedule_interval='00 16 * * *',
    catchup=False,
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Start processing"),
    )

    all_tasks = []
    for cc in ['IN', 'ID', 'HK', 'PK', 'PH', 'MY']:
        if cc == 'IN':
            parallel_load(cc)
        else:
            sequencial_load(cc)
