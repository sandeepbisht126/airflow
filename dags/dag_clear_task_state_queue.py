from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagRun, TaskInstance, DagBag
from airflow.utils.state import State
from airflow.utils.session import provide_session
from airflow.utils.dates import days_ago
from airflow.utils import timezone

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

dag = DAG(
    dag_id="dag_clear_task_state_queue",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually
    catchup=False,
    description="Queue new task instances for historical DAG runs",
)


@provide_session
def queue_new_task_instances(dag_id, task_id, start_date, end_date, session=None):
    dag_bag = DagBag()
    dag = dag_bag.get_dag(dag_id)

    if not dag:
        raise Exception(f"DAG '{dag_id}' not found")

    if task_id not in dag.task_ids:
        raise Exception(f"Task '{task_id}' not found in DAG '{dag_id}'")

    task = dag.get_task(task_id)

    dag_runs = session.query(DagRun).filter(
        DagRun.dag_id == dag_id,
        DagRun.execution_date >= start_date,
        DagRun.execution_date <= end_date,
    ).all()

    if not dag_runs:
        print(f"No DAG runs found for DAG '{dag_id}' in range.")
        return

    for dr in dag_runs:
        # existing_ti = session.query(TaskInstance).filter_by(
        #     dag_id=dag_id,
        #     task_id=task_id,
        #     execution_date=dr.execution_date
        # ).one_or_none()

        # if existing_ti:
        #     print(f"TaskInstance already exists: {dag_id}.{task_id} @ {dr.execution_date}, skipping.")
        #     existing_ti.state = State.NONE
        # else:
        #     ti = TaskInstance(task=task, execution_date=dr.execution_date)
        #     ti.state = State.NONE
        #     session.add(ti)
        #     print(f"Queued task: {dag_id}.{task_id} for execution_date: {dr.execution_date}")

        if dr.state != State.RUNNING:
            print(f"Setting DagRun {dag_id} @ {dr.execution_date} to RUNNING")
            dr.state = State.RUNNING

    session.commit()
    print("All new tasks queued.")


def trigger_new_tasks_callable(**context):
    # You can optionally use context["dag_run"].conf to pass in parameters
    dag_id = "dag_postgres_parallel_load"  # DAG that had new tasks added
    task_id = "insert_for_FR"
    start_date = timezone.datetime(2025, 3, 1)
    end_date = timezone.datetime(2025, 3, 3)

    queue_new_task_instances(dag_id, task_id, start_date, end_date)


trigger_task = PythonOperator(
    task_id="queue_new_tasks",
    python_callable=trigger_new_tasks_callable,
    provide_context=True,
    dag=dag,
)
