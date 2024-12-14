from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from script.utils import get_param_value, convert_str_to_datetime
from airflow.models import Variable

pipeline_name = "dag_jinja_to_utility"


def print_log(dt, jinja_ds="dummy"):
    print(f'From Python operator - yesterday was: {dt}, type: {type(dt)}')
    print(f'From param_dict - Jinja ds was: {convert_str_to_datetime(jinja_ds)}, type: {type(convert_str_to_datetime(jinja_ds))}')


dag = DAG(
    dag_id="dag_jinja_to_utility",
    description="Usage of Airflow Jinja variable",
    max_active_runs=1,
    schedule_interval="@once",
    start_date=days_ago(1),
)


def generate_aod_param(**context):
    # Generate `aod` dynamically based on `{{ ds }}` at runtime
    aod = context["ds"]
    params = get_param_value(pipeline_name=pipeline_name,
                             historical_load_params="hist_load_param",
                             cc_list=eval(Variable.get("cc_list")),
                             aod_start_range=7,
                             aod_end_range=7,
                             aod=aod                # additional args
                             )

    return params  # JSON serializable dictionary


generate_aod_param = PythonOperator(
    task_id="generate_aod_param",
    python_callable=generate_aod_param,
    provide_context=True,
    dag=dag,
)


task1 = PythonOperator(
    task_id='print_python',
    python_callable=print_log,
    op_args=["{{ task_instance.xcom_pull(task_ids='generate_aod_param') }}",
             "{{ task_instance.xcom_pull(task_ids='generate_aod_param')['aod_start'] }}"],
    dag=dag
)

generate_aod_param >> task1
