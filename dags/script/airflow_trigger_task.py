import sys
from airflow.models import DagBag, DagRun, TaskInstance
from airflow.settings import Session
from airflow.utils.state import State
from airflow.utils import timezone

# Date range to run tasks for
CLEAR_FROM = timezone.datetime(2025, 2, 1)
CLEAR_TO = timezone.datetime(2025, 2, 5)


def run_tasks(dag_id, task_id):
    session = Session()
    dagbag = DagBag()
    dag = dagbag.get_dag(dag_id)

    if not dag:
        raise Exception(f"DAG {dag_id} not found")

    task = dag.get_task(task_id)

    dag_runs = (
        session.query(DagRun)
        .filter(DagRun.dag_id == dag_id)
        .filter(DagRun.execution_date >= CLEAR_FROM)
        .filter(DagRun.execution_date < CLEAR_TO)
        .order_by(DagRun.execution_date)
        .all()
    )

    for run in dag_runs:
        ti = TaskInstance(task=task, execution_date=run.execution_date)
        ti.refresh_from_db(session=session)
        if ti.state not in [State.RUNNING, State.QUEUED, State.SUCCESS]:
            print(f"▶️ Running {task_id} for {run.execution_date}")
            ti.run(ignore_all_deps=True, ignore_ti_state=True)
        else:
            print(f"⏩ Skipping {run.execution_date}, state: {ti.state}")

    session.close()
    print("✅ Task trigger completed.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python airflow_trigger_tasks.py <dag_id> <task_id>")
        sys.exit(1)

    dag_id = sys.argv[1]
    task_id = sys.argv[2]
    run_tasks(dag_id, task_id)
