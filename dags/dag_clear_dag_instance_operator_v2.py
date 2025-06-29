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

all_dag_params = Variable.get("dag_backfill_params", default_var="{}", deserialize_json=True)
param_sets = all_dag_params if isinstance(all_dag_params, list) else [all_dag_params]


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
def set_dag_state(dag_id, execution_date_str, session=None):
    execution_date = parse_execution_date(execution_date_str)
    dag_bag = DagBag()

    if not dag_bag.get_dag(dag_id):
        raise Exception(f"DAG '{dag_id}' not found")

    dag_runs = session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun.execution_date == execution_date
    ).order_by(DagRun.execution_date.asc()).all()

    active_dagrun_count = session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun.state == State.RUNNING
    ).count()

    if not dag_runs:
        print(f"No DAG runs found for DAG '{dag_id}' in range.")
        return

    for dr in dag_runs:
        # Only touch completed DagRuns
        if dr.state in [State.SUCCESS, State.FAILED]:
            if active_dagrun_count >= 1:
                dr.state = State.QUEUED  # or custom
                session.merge(dr)
                continue

            # Set one DagRun at a time
            dr.state = State.RUNNING
            active_dagrun_count += 1

    session.commit()
    print("All new tasks queued.")


def clear_trigger_dag(dag_id, exec_date, sanitized_task, priority_weight):
    global previous_task
    # Step 1: delete existing DAG run
    clear_task = PythonOperator(
        task_id=f"delete_{sanitized_task}",
        python_callable=clear_dag_run,
        op_kwargs={
            'dag_id': dag_id,
            'execution_date_str': exec_date,
        },
        priority_weight=priority_weight,
        dag=dag,
    )

    if not only_delete_dagrun:
        # Step 2: trigger DAG run
        trigger_dag_run = TriggerDagRunOperator(
            task_id=sanitized_task,
            trigger_dag_id=dag_id,
            execution_date=exec_date,
            conf={},
            priority_weight=priority_weight,
            dag=dag,
        )
        clear_task >> trigger_dag_run

        if dependency != "parallel":
            if previous_task:
                previous_task >> clear_task
                previous_task = trigger_dag_run

    return previous_task


def run_new_tasks(dag_id, exec_date, sanitized_task, priority_weight):
    global previous_task
    set_state = PythonOperator(
        task_id=f"run_new_tasks_{sanitized_task}",
        python_callable=set_dag_state,
        op_kwargs={
            'dag_id':dag_id,
            'execution_date_str': exec_date
        },
        priority_weight=priority_weight,
        dag=dag,
    )

    if dependency != "parallel":
        if previous_task:
            previous_task >> set_state
            previous_task = set_state

    return previous_task


def parse_execution_dates():
    """Get all execution dates from explicit list and/or start-end range."""
    execution_dates_list = param_set.get("execution_dates_list", [])
    execution_date_start = param_set.get("execution_date_start")
    execution_date_end = param_set.get("execution_date_end")

    if execution_date_start and execution_date_end:
        start_date = parse_execution_date(execution_date_start)
        end_date = parse_execution_date(execution_date_end)
        current_date = start_date
        while current_date <= end_date:
            execution_dates_list.append(current_date)
            current_date += timedelta(days=1)

    return sorted(set([parse_execution_date(dt) if isinstance(dt, str) else dt for dt in execution_dates_list ]))


dag = DAG(
    dag_id='dag_clear_dag_instance_operator_v2',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
)

previous_task = []
for param_set in param_sets:
    dags_to_run = param_set.get("dags_to_run", [])
    execution_dates = parse_execution_dates()
    dependency = param_set.get("dependency", "parallel")
    only_delete_dagrun = param_set.get("only_delete_dagrun")
    only_run_new_tasks = param_set.get("only_run_new_tasks", False)

    all_tasks = len(set(execution_dates))
    for idx, exec_dt in enumerate(execution_dates):
        exec_dt = exec_dt.strftime('%Y-%m-%dT%H:%M:%S')
        priority_wt = all_tasks - idx
        for dag_nm in dags_to_run:
            task_id = f"{dag_nm}_{exec_dt}"
            sanitized_task_id = re.sub(r"[^a-zA-Z0-9_-]", "-", task_id)

            if only_run_new_tasks:
                previous_task = run_new_tasks(dag_nm, exec_dt, sanitized_task_id, priority_wt)

            else:
                previous_task = clear_trigger_dag(dag_nm, exec_dt, sanitized_task_id, priority_wt)
