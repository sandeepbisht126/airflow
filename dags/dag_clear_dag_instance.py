from airflow import DAG
from airflow.models import DagRun, DagBag
from airflow.utils.db import create_session
import pendulum
import subprocess
from airflow.operators.python import PythonOperator


def clear_and_rerun_dag_run(dag_id, execution_date_rerun):
    """Clears and re-runs a DAG run for the specified execution date, handling DagRunAlreadyExists."""

    # dagbag = DagBag()
    # dag = dagbag.get_dag(dag_id)

    if True:
        with create_session() as session:
            try:
                dag_run = session.query(DagRun).filter(
                    DagRun.dag_id == dag_id,
                    DagRun.execution_date == execution_date_rerun,
                ).first()

                if dag_run:
                    session.delete(dag_run)
                    session.commit()
                    print(f"Existing DagRun deleted: {dag_id} - {execution_date_rerun}")

                # Attempt to trigger the DAG run
                print(f"Triggering DAG run: {dag_id} - {execution_date_rerun} ...")
                execution_date_str = execution_date_rerun.strftime("%Y-%m-%dT%H:%M:%S")
                command = ["airflow", "dags", "trigger", dag_id, "-e", execution_date_str]
                #subprocess.run(command, check=True, capture_output=True, text=True)
                print(f"DAG run triggered: {dag_id} - {execution_date_rerun}")

            except subprocess.CalledProcessError as e:
                print(f"Error triggering DAG run: {e}")
                print(e.stderr)

    else:
        print(f"DAG with ID '{dag_id}' not found.")


dag_id_to_rerun = "dag_jinja_str_dates"
execution_date_to_rerun = pendulum.datetime(2025, 1, 15, 16, 0, 0, tz="UTC")

with DAG(
    dag_id='dag_clear_dag_instance',
    schedule_interval='00 16 * * *',
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
) as dag:

    clear_task = PythonOperator(
        task_id='clear_task',
        python_callable=clear_and_rerun_dag_run,
        op_args=[dag_id_to_rerun, execution_date_to_rerun]
    )
