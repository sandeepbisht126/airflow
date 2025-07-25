from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from custom_operator import PostgresInsertOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from step_custom_operator import PostgresStepInsertOperator

delta = 2
aod = f"{{{{ macros.ds_add(ds, -{delta})}}}}"

with open("dags/jinja_test.sql", 'r') as file:
    source_query = file.read()

# template = Template(source_query)

with DAG(
    dag_id="dag_jinja_custom_operator",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False
) as dag:

    inbuilt_operator = PostgresOperator(
        task_id='inbuilt_operator',
        postgres_conn_id='postgres_conn_id',
        sql=f"INSERT INTO public.huge_table_parallel select * from public.huge_table where created_at = {repr(aod)}:: date limit 10",
    )

    custom_operator = PostgresInsertOperator(
        task_id="custom_operator",
        # source="public.huge_table",
        source_query=source_query.format(aod=repr(aod), cc=repr('IN')),
        # source_query= source_query,
        target="public.huge_table_parallel",
        delete_by_key_val={"created_at": f"cast({repr(aod)} as date)"},
        key_cols=["id"],  # Not used for INSERT, but required by signature
        # custom_filter=f"where created_at = {repr(aod)}:: date limit 10",
        postgres_conn_id="postgres_conn_id",
        execution_timeout=timedelta(seconds=50),
        parameters={"aod": aod,
                    "country_code": 'cc'}
    )

    step_custom_operator = PostgresStepInsertOperator(
        task_id="step_custom_operator",
        steps=[{
            # source="public.huge_table",
            'source_query': source_query.format(aod=repr(aod), cc=repr('SG')),
            # 'source_query': source_query,
            'target': "public.huge_table_parallel",
            'key_cols': ["id"],  # Not used for INSERT, but required by signature
            # custom_filter=f"where created_at = {repr(aod)}:: date limit 10",
            'postgres_conn_id': "postgres_conn_id"
            }
        ],
    )

    # step_both_custom_operator = PostgresStepInsertOperator(
    #     task_id="step_both_custom_operator",
    #     steps=[{
    #         # source="public.huge_table",
    #         'source_query': source_query,
    #         'target': "public.huge_table_parallel",
    #         'key_cols': ["id"],  # Not used for INSERT, but required by signature
    #         # custom_filter=f"where created_at = {repr(aod)}:: date limit 10",
    #         'postgres_conn_id': "postgres_conn_id",
    #         'params': {"limit": 10}
    #         }
    #     ],
    # )