from airflow import DAG
from airflow.models import DagRun, TaskInstance, DagBag
from airflow.operators.python import PythonOperator
from airflow.utils.session import provide_session
from airflow.utils.state import State
from airflow.utils import timezone
from airflow.utils.dates import days_ago
from sqlalchemy.orm import Session
from datetime import datetime, timedelta

# Timezone-aware range
TRIGGER_START_DATE = timezone.datetime(2025, 3, 1)
TRIGGER_END_DATE = timezone.datetime(2025, 3, 3)
TARGET_DAG_ID = "dag_postgres_parallel_load"
TARGET_TASK_ID = "insert_for_PK"  # Your newly added task ID


@provide_session
def trigger_specific_task(session: Session = None, **context):
    dagbag = DagBag()
    target_dag = dagbag.get_dag(TARGET_DAG_ID)
    if not target_dag:
        raise ValueError(f"DAG '{TARGET_DAG_ID}' not found.")

    target_task = target_dag.get_task(TARGET_TASK_ID)
    if not target_task:
        raise ValueError(f"Task '{TARGET_TASK_ID}' not found in the currently parsed DAG '{TARGET_DAG_ID}'.")

    processed_count = 0
    for execution_date in _date_range(TRIGGER_START_DATE, TRIGGER_END_DATE):
        dag_runs = session.query(DagRun).filter(
            DagRun.dag_id == TARGET_DAG_ID,
            DagRun.execution_date >= execution_date.replace(hour=0, minute=0, second=0, microsecond=0),
            DagRun.execution_date < (execution_date + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        ).all()

        for dag_run in dag_runs:
            task_instance = session.query(TaskInstance).filter(
                TaskInstance.dag_id == TARGET_DAG_ID,
                TaskInstance.task_id == TARGET_TASK_ID,
                TaskInstance.execution_date == dag_run.execution_date
            ).first()

            if task_instance:
                if task_instance.state not in [State.QUEUED]:
                    print(f"Attempting to reset and clear existing '{TARGET_TASK_ID}' in '{TARGET_DAG_ID}' for {dag_run.execution_date}")
                    task_instance.state = None
                    task_instance.queued_dttm = None
                    task_instance.start_date = None
                    task_instance.end_date = None
                    task_instance.duration = None
                    task_instance.try_number = 0
                    task_instance.max_tries = target_task.retries + 1
                    task_instance.hostname = None
                    task_instance.unixname = None
                    task_instance.job_id = None
                    task_instance.pool = target_task.pool if target_task.pool else 'default'
                    task_instance.pool_slots = target_task.pool_slots
                    task_instance.queued_by_job_id = None
                    task_instance.pid = None
                    task_instance.executor_id = None
                    session.add(task_instance)
                    processed_count += 1
                    print(f"Successfully reset and cleared existing '{TARGET_TASK_ID}' for re-run on {dag_run.execution_date}")
                else:
                    print(f"Existing task '{TARGET_TASK_ID}' in '{TARGET_DAG_ID}' for {dag_run.execution_date} is already in state: {task_instance.state}. Skipping.")
            else:
                # Task instance doesn't exist for this old DagRun, so create it
                new_task_instance = TaskInstance(
                    task=target_task,
                    execution_date=dag_run.execution_date,
                    state=None,  # Set initial state to None to trigger
                )
                session.add(new_task_instance)
                processed_count += 1
                print(f"Created and set state to None for new task '{TARGET_TASK_ID}' in '{TARGET_DAG_ID}' for {dag_run.execution_date}")

    session.commit()
    print(f"✅ Processed {processed_count} instances of task '{TARGET_TASK_ID}' in DAG '{TARGET_DAG_ID}' for re-run.")


def _date_range(start_date, end_date):
    current_date = start_date
    while current_date < end_date:
        yield current_date
        current_date += timedelta(days=1)


with DAG(
    dag_id='dag_clear_tasks_schedule',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['maintenance', 'utility', 'trigger'],
) as dag:

    trigger_task = PythonOperator(
        task_id='trigger_new_task_for_old_runs',
        python_callable=trigger_specific_task,
        provide_context=True,
    )