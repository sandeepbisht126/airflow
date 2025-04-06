import re
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.models import DagRun
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import parse_execution_date
from airflow.utils.session import provide_session
from datetime import datetime, timedelta


@provide_session
def clear_dag_run(dag_id, execution_date_str, session=None):
    """Clears existing DAG runs before triggering a new one."""

    if not execution_date_str or not dag_id:
        raise ValueError(f"{execution_date_str} and {dag_id} are must !")

    execution_date = parse_execution_date(execution_date_str)
    dag_runs = session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun.execution_date == execution_date
    ).all()

    if dag_runs:
        for run in dag_runs:
            session.delete(run)
        session.commit()
        print(f"Deleted existing DAG run for {dag_id} at {execution_date_str}")


# Fetch parameters from Airflow Variable
all_dag_params = Variable.get("dag_backfill_params", default_var="{}", deserialize_json=True)
print(f"type of all_dag_params is: {type(all_dag_params)}")
# dag_params = all_dag_params.get("dag_clear_dag_instance_operator", {})
# all_dag_params = eval(Variable.get("dag_backfill_params", default_var="{}", deserialize_json=True))
dags_to_run = all_dag_params.get("dags_to_run", [])
execution_dates_list = all_dag_params.get("execution_dates_list", [])
dependency = all_dag_params.get("dependency", "parallel")
execution_date_start = all_dag_params.get("execution_date_start")
execution_date_end = all_dag_params.get("execution_date_end")
flag = all_dag_params.get("flag")

# Generate list of execution dates
if execution_date_start and execution_date_end:
    start_date = parse_execution_date(execution_date_start)
    end_date = parse_execution_date(execution_date_end)
    current_date = start_date
    while current_date <= end_date:
        execution_dates_list.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)

dag = DAG(
    dag_id='dag_clear_dag_instance_operator_v2',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
)

previous_task=None
tasks = []
for dag_nm in dags_to_run:
    for exec_dt in execution_dates_list:
        task_id = f"{dag_nm}_{exec_dt}"
        sanitized_task_id = re.sub(r"[^a-zA-Z0-9_-]", "-", task_id)

        # Step 1: delete existing DAG run
        clear_task = PythonOperator(
            task_id=f"clear_{sanitized_task_id}",
            python_callable=clear_dag_run,
            op_kwargs={
                'dag_id': dag_nm,
                'execution_date_str': exec_dt,
            },
            dag=dag,
        )

        # Step 2: trigger DAG run
        trigger_dag_run = TriggerDagRunOperator(
            task_id=sanitized_task_id,
            trigger_dag_id=dag_nm,
            execution_date=exec_dt,
            conf={},
            dag=dag,
        )

        if dependency == "parallel":
            clear_task >> trigger_dag_run
        else:
            if previous_task:
                previous_task >> clear_task
            clear_task >> trigger_dag_run
            previous_task = trigger_dag_run
