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
                         aod_end_range=1
                         )

if params.get("load_type") == 'daily':
    # params["aod"] = [f"{{{{ macros.ds_add(ds, -{i}) }}}}" for i in range(5)]       # for multi-day processing for Daily load
    params["aod"] = ["{{ macros.ds_add(ds, -7) }}"]                                  # for single day processing for Daily load


dag = DAG(
    dag_id="dag_jinja_str_dates",
    description="usage of airflow macro",
    max_active_runs=1,
    schedule_interval="00 16 * * *",
    start_date=datetime(2024, 12, 10, 16, 00, 00),
    catchup=True
)


def print_log(aod_start=None, aod_end=None):
    print(f'from python operator - date - {aod_start} and type: {type(aod_start)}')
    print(f'from python operator - date - {aod_end} and type: {type(aod_end)}')


tasks_list = []
for i, aod in enumerate(params.get("aod")):
    tsk = PythonOperator(
        task_id=f'print_python_{i}',
        python_callable=print_log,
        op_args=[aod],
        dag=dag
    )
    tasks_list.append(tsk)


tasks_list
