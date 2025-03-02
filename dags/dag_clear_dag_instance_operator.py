from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import DagRun
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.utils.dates import parse_execution_date
from datetime import datetime
from airflow.utils.session import provide_session
from airflow.models import Variable
import re
from airflow.operators.dummy import DummyOperator


@provide_session
def clear_and_trigger_dag_run(dag_id, execution_date_str, session=None):
    """
    Clears (triggers) a specific DAG run for the given execution date.
    """
    if not execution_date_str or not dag_id:
        raise ValueError(f"{execution_date_str} and {dag_id} are must !")

    execution_date = parse_execution_date(execution_date_str)

    # Clear the existing DAG run for the given execution date
    dag_runs = DagRun.find(dag_id=dag_id, execution_date=execution_date)
    if dag_runs:
        print(f"Found {len(dag_runs)} existing runs - deleting them")
        for run in dag_runs:
            session.delete(run)
        session.commit()

    trigger_dag(
        dag_id=dag_id,
        run_id=f"manual__{execution_date_str}",
        execution_date=execution_date,
        conf={},
        replace_microseconds=False,
    )
    print(f"Triggered DAG run for {dag_id} with execution date {execution_date} ...")


# dags_to_run = ['dag_jinja_str_dates']
# execution_dates_list = ['2025-01-10T16:00:00', '2025-01-11T16:00:00', '2025-01-12T16:00:00']

all_dag_params = Variable.get("dag_parameters", default_var="{}", deserialize_json=True)
dag_params = all_dag_params.get("dag_clear_dag_instance_operator")
dags_to_run = dag_params.get("dags_to_run")
execution_dates_list = dag_params.get("execution_dates_list")
dependency = dag_params.get("dependency", "nparallel")

dag = DAG(
    dag_id='dag_clear_dag_instance_operator',
    schedule_interval=None,
    start_date=datetime(2024, 12, 15),
)

start = DummyOperator(task_id="start")

tasks = []
for dag_nm in dags_to_run:
    for exec_dt in execution_dates_list:
        task_id = f"{dag_nm}_clear_and_trigger_{exec_dt}"
        trigger_dag_run = PythonOperator(
            task_id=re.sub(r"[^a-zA-Z0-9_-]", "-", task_id),
            python_callable=clear_and_trigger_dag_run,
            op_kwargs={
                'dag_id': dag_nm,
                'execution_date_str': exec_dt,
            },
            dag=dag,
        )
        tasks.append(trigger_dag_run)

if dependency == "parallel":
    start >> tasks
else:
    start >> tasks[0]
    for i in range(1, len(tasks)):
        tasks[i - 1] >> tasks[i]
