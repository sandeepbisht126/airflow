from time import sleep

from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
import pendulum
from airflow.utils.dates import parse_execution_date

from script.utils import get_param_value, convert_str_to_datetime, daterange, iterate_str_dates

pipeline_name = "dag_jinja_str_dates"
historical_load_params = Variable.get("historical_load_params")
params = get_param_value(pipeline_name=pipeline_name,
                         historical_load_params=historical_load_params,
                         cc_list=eval(Variable.get("cc_list")),
                         aod_start_range=7,
                         aod_end_range=1
                         )

aod_delta = 4
if params.get("load_type") == 'daily':
    dates_to_process = [f"{{{{ macros.ds_add(ds, -{i}) }}}}" for i in range(9)]  # for multi-day processing for Daily load, like Beacon PDM
    #dates_to_process = [f"{{{{ macros.ds_add(ds, -{aod_delta}) }}}}"]           # for single day processing for Daily load, like Commercial common
elif params.get("load_type") == 'historical':
    dates_to_process = [aod.strftime("%Y-%m-%d")
                        for aod, _ in daterange(convert_str_to_datetime(params.get("aod_start")),
                                                convert_str_to_datetime(params.get("aod_end")))]
else:
    raise ValueError("Invalid load_type, should be daily or historical")

all_dag_params = Variable.get("dag_parameters", default_var="{}", deserialize_json=True)
dag_params = all_dag_params.get("dag_jinja_str_dates")
end_date_str = dag_params.get("end_date")
end_date = datetime.strptime(end_date_str, "%Y-%m-%dT%H:%M:%S") if end_date_str else None
start_date_str = dag_params.get("start_date")
start_date = datetime.strptime(start_date_str, "%Y-%m-%dT%H:%M:%S") if start_date_str else None
catchup = dag_params.get("catchup", False)

dag = DAG(
    dag_id="dag_jinja_str_dates",
    description="usage of airflow macro",
    max_active_runs=1,
    schedule_interval="00 16 * * *",
    start_date=parse_execution_date(start_date_str),
    end_date=parse_execution_date(end_date_str),
    catchup=catchup
)


def print_log(aod_start=None, aod_end=None):
    sleep(10)
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

workflow1 = BashOperator(
    task_id='echo_bash',
    bash_command='echo "today day is - {{ ds }}"',
    dag=dag
)

workflow1 >> tasks_list
