import os

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from script.utils import get_param_value, convert_str_to_datetime, daterange, iterate_str_dates

pipeline_name = "dag_jinja_simple"
historical_load_params = Variable.get("historical_load_params")
params = get_param_value(pipeline_name=pipeline_name,
                         historical_load_params=historical_load_params,
                         cc_list=eval(Variable.get("cc_list")),
                         aod_start_range=7,
                         aod_end_range=7
                         )

if not params.get("hist_flag"):  # for daily load
    params = {
        "aod_start": "{{ macros.ds_add(ds, -5) }}",
        "aod_end": "{{ macros.ds_add(ds) }}",
        "aod": "{{ macros.ds_add(ds, -7) }}",
        "cc_list": eval(Variable.get("cc_list")),
        "hist_flag": params.get("hist_flag")
    }

dag = DAG(
    dag_id="dag_jinja_simple",
    description="usage of airflow macro",
    max_active_runs=1,
    schedule_interval="00 16 * * *",
    start_date=datetime(2024, 12, 10, 16, 00, 00),
    catchup=True
)


def print_log(dt):
    print(f'from python operator - date - {dt} and type: {type(dt)}')


workflow3 = PythonOperator(
    task_id='print_python',
    python_callable=print_log,
    op_args=[aod for aod in iterate_str_dates(params.get("aod_start"), params.get("aod_end"))],
    dag=dag
)

workflow3
