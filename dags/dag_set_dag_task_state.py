import re
from airflow import DAG
from airflow.models import Variable
from airflow.models import DagRun, TaskInstance, DagBag
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.utils.dates import days_ago
from airflow.utils.dates import parse_execution_date
from sqlalchemy.orm import Session
from datetime import timedelta

all_dag_params = Variable.get("dag_set_state_params", default_var="{}", deserialize_json=True)
dag_tasks = all_dag_params.get("dag_tasks", {})
execution_dates_list = all_dag_params.get("execution_dates_list", [])
execution_date_start = all_dag_params.get("execution_date_start")
execution_date_end = all_dag_params.get("execution_date_end")
desired_state_str = all_dag_params.get("desired_state", "success").upper()


@provide_session
def manual_set_state(dag_id, task_id, execution_date_str, session: Session = None, **context):
    dagbag = DagBag()
    target_dag = dagbag.get_dag(dag_id)
    if not target_dag:
        raise ValueError(f"DAG '{dag_id}' not found.")

    target_task = target_dag.get_task(task_id)
    if not target_task:
        raise ValueError(f"Task '{task_id}' not found in the currently parsed DAG '{dag_id}'.")

    execution_date = parse_execution_date(execution_date_str)
    dag_runs = session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun.execution_date == execution_date
    ).all()

    if not dag_runs:
        print(f"No DAG runs found for DAG '{dag_id}.{execution_date}' in range.")
        return

    if not hasattr(State, desired_state_str):
        raise ValueError(
            f"Invalid desired state '{desired_state_str}'. Must be a valid airflow.utils.state.State value.")

    desired_state = getattr(State, desired_state_str)

    for dag_run in dag_runs:
        task_instance = session.query(TaskInstance).filter_by(
            dag_id=dag_id,
            task_id=task_id,
            execution_date=dag_run.execution_date
        ).one_or_none()

        if task_instance:
            task_instance.state = desired_state
            print(f"Task exists: {dag_id}.{task_id} state set to SUCCESS for execution_date: {dag_run.execution_date}")
        else:
            ti = TaskInstance(task=target_task, execution_date=dag_run.execution_date)
            # ti.state = State.NONE
            # ti.state = desired_state
            ti.set_state(State.NONE, session=session)
            session.add(ti)
            print(f"Task didn't exists, added: {dag_id}.{task_id} and state set to SUCCESS for execution_date: {dag_run.execution_date}")

    session.commit()


if execution_date_start and execution_date_end:
    start_date = parse_execution_date(execution_date_start)
    end_date = parse_execution_date(execution_date_end)
    current_date = start_date
    while current_date <= end_date:
        execution_dates_list.append(current_date.strftime('%Y-%m-%dT%H:%M:%S'))
        current_date += timedelta(days=1)

with DAG(
    dag_id='dag_set_dag_task_state',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['maintenance', 'utility', 'trigger'],
) as dag:

    for exec_dt in sorted(set(execution_dates_list)):
        for dag_nm in dag_tasks:
            for task in dag_tasks[dag_nm]:
                task_id_nm = f"{dag_nm}_{task}_{exec_dt}"
                sanitized_task_id = re.sub(r"[^a-zA-Z0-9_-]", "-", task_id_nm)

                trigger_task = PythonOperator(
                    task_id=f"set_{desired_state_str}_{sanitized_task_id}",
                    python_callable=manual_set_state,
                    op_kwargs={
                        'dag_id': dag_nm,
                        'task_id': task,
                        'execution_date_str': exec_dt,
                    },
                    provide_context=True,
                )
