from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.decorators import task_group


def parallel_load(cc):
    @task_group(group_id=f"insert_for_{cc}", ui_color="#80D8FF")
    def load_tasks():
        tasks = []
        parallel_thread = 6
        for id in range(parallel_thread):
            insert_parallel = PostgresOperator(
                task_id=f'insert_chunk_{id}',
                postgres_conn_id='postgres_conn_id',  # Connection ID from Airflow > Admin > Connections
                sql=f"""
                    INSERT INTO huge_table_parallel
                    SELECT * from huge_table
                    WHERE country = '{cc}'
                    AND mod(id, {parallel_thread}) = {id};
                """,
            )
            tasks.append(insert_parallel)
        return tasks
    return load_tasks()


with DAG(
    dag_id='dag_postgres_parallel_load',
    description='A sample DAG to test parallel load in postgresql',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    start = PythonOperator(
        task_id="start",
        python_callable=lambda: print("Start processing"),
    )

    all_tasks = []
    for cc in ['IN', 'SG']:
        insert_parallelTb = parallel_load(cc) if cc == 'IN' else PostgresOperator(
            task_id=f'insert_for_{cc}',
            postgres_conn_id='postgres_conn_id',
            sql=f"""
                    INSERT INTO huge_table_parallel
                    SELECT * from huge_table
                    where country = '{cc}'
                    """,
        )

    start >> insert_parallelTb


