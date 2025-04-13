import re
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import parse_execution_date
from airflow.utils.session import provide_session
from datetime import datetime, timedelta
from airflow.models import DagRun, DagBag
from airflow.utils.state import State

# Fetch parameters from Airflow Variable
all_dag_params = Variable.get("dag_backfill_params", default_var="{}", deserialize_json=True)
print(f"type of all_dag_params is: {type(all_dag_params)}")
dags_to_run = all_dag_params.get("dags_to_run", [])
execution_dates_list = all_dag_params.get("execution_dates_list", [])
dependency = all_dag_params.get("dependency", "parallel")
execution_date_start = all_dag_params.get("execution_date_start")
execution_date_end = all_dag_params.get("execution_date_end")
only_delete_dagrun = all_dag_params.get("only_delete_dagrun")
set_dag_state = all_dag_params.get("set_dag_state", False)


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


@provide_session
def set_dag_state_def(dag_id, execution_date_str, session=None):
    execution_date = parse_execution_date(execution_date_str)
    dag_bag = DagBag()

    if not dag_bag.get_dag(dag_id):
        raise Exception(f"DAG '{dag_id}' not found")

    dag_runs = session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun.execution_date >= execution_date,
        DagRun.execution_date <= execution_date,
    ).all()

    if not dag_runs:
        print(f"No DAG runs found for DAG '{dag_id}' in range.")
        return

    for dr in dag_runs:
        if dr.state != State.RUNNING:
            print(f"Setting DagRun {dag_id} @ {dr.execution_date} to RUNNING")
            dr.state = State.RUNNING

    session.commit()
    print("All new tasks queued.")


def clear_trigger_dag(dag_id, exec_date, sanitized_task, previous_task = None):

    # Step 1: delete existing DAG run
    clear_task = PythonOperator(
        task_id=f"clear_{sanitized_task}",
        python_callable=clear_dag_run,
        op_kwargs={
            'dag_id': dag_id,
            'execution_date_str': exec_date,
        },
        dag=dag,
    )

    if not only_delete_dagrun:
        # Step 2: trigger DAG run
        trigger_dag_run = TriggerDagRunOperator(
            task_id=sanitized_task,
            trigger_dag_id=dag_id,
            execution_date=exec_date,
            conf={},
            dag=dag,
        )
        clear_task >> trigger_dag_run

        if dependency != "parallel":
            if previous_task:
                previous_task >> clear_task
                clear_task >> trigger_dag_run
                previous_task = trigger_dag_run


dag = DAG(
    dag_id='dag_clear_dag_instance_operator_v2',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
)

# Generate list of execution dates
if execution_date_start and execution_date_end:
    start_date = parse_execution_date(execution_date_start)
    end_date = parse_execution_date(execution_date_end)
    current_date = start_date
    while current_date <= end_date:
        execution_dates_list.append(current_date.strftime('%Y-%m-%dT%H:%M:%S'))
        current_date += timedelta(days=1)

for dag_nm in dags_to_run:
    for exec_dt in sorted(set(execution_dates_list)):
        task_id = f"{dag_nm}_{exec_dt}"
        sanitized_task_id = re.sub(r"[^a-zA-Z0-9_-]", "-", task_id)

        if set_dag_state:
            set_state = PythonOperator(
                task_id=f"set_state_{sanitized_task_id}",
                python_callable=set_dag_state_def,
                op_kwargs={
                    'dag_id': dag_nm,
                    'execution_date_str': exec_dt,
                },
                dag=dag,
            )
        else:
            clear_trigger_dag(dag_nm, exec_dt, sanitized_task_id)
