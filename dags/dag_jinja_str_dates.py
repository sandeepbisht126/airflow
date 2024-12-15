from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

from script.utils import get_param_value, convert_str_to_datetime, daterange, iterate_str_dates

pipeline_name = "dag_jinja_str_dates"
historical_load_params = Variable.get("historical_load_params")
params = get_param_value(pipeline_name=pipeline_name,
                         historical_load_params=historical_load_params,
                         cc_list=eval(Variable.get("cc_list")),
                         aod_start_range=7,
                         aod_end_range=1
                         )

if params.get("load_type") == 'daily':
    dates_to_process = [f"{{{{ macros.ds_add(ds, -{i}) }}}}" for i in range(5)]  # for multi-day processing for Daily load, like Beacon PDM
    # dates_to_process = ["{{ macros.ds_add(ds, -7) }}"]                         # for single day processing for Daily load, like Commercial common
elif params.get("load_type") == 'historical':
    dates_to_process = [aod.strftime("%Y-%m-%d")
                        for aod, _ in daterange(convert_str_to_datetime(params.get("aod_start")),
                                                convert_str_to_datetime(params.get("aod_end")))]
else:
    raise ValueError("Invalid load_type, should be daily or historical")

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
for i, aod in enumerate(dates_to_process):
    tsk = PythonOperator(
        task_id=f'print_python_{i}',
        python_callable=print_log,
        op_args=[aod],
        dag=dag
    )
    tasks_list.append(tsk)

tasks_list
