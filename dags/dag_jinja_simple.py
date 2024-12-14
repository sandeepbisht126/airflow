import os

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from script.utils import get_param_value, convert_str_to_datetime, daterange

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
        "aod_start": params.get("aod_start"),
        "aod_end": params.get("aod_end"),
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


def print_log(dt, jinja_ds=None):
    print(f'from python operator - yesterday day was - {dt} and type: {type(dt)}')
    print(f'from param_dict - jinja_ds was - {jinja_ds} and type: {type(jinja_ds)}')


workflow3 = PythonOperator(
    task_id='print_python',
    python_callable=print_log,
    op_args=[(aod.strftime('%Y-%m-%d'), 'hist')
             for aod, _ in daterange(convert_str_to_datetime(params.get("aod_start")),
                                     convert_str_to_datetime(params.get("aod_end")))
             if params.get("hist_flag")] or [params.get("aod"), 'daily'],
    dag=dag
)

workflow3
