from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pendulum
from airflow import DAG
from airflow.models import DagRun
from airflow.utils.state import State
from datetime import datetime, timedelta
from airflow.models import DagBag
from airflow.utils.db import create_session
import pendulum


def clear_and_delete_dag(dag_id, start_date, end_date):
    """
    Clears DAG runs between start_date and end_date. If the DAG has no runs,
    it creates and runs the DAG for the specified period, respecting the DAG's schedule.

    Args:
        dag_id (str): The ID of the DAG.
        start_date (datetime): The start date for clearing/running.
        end_date (datetime): The end date for clearing/running.
    """

    with create_session() as session:
        dag_runs = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.execution_date >= start_date,
            DagRun.execution_date <= end_date,
        ).all()

        if dag_runs:
            # Delete existing runs
            for dag_run in dag_runs:
                print(f"Deleting DAG run: {dag_id} - {dag_run.execution_date}")
                session.delete(dag_run)
            session.commit()
            print(f"Deleted existing DAG runs for {dag_id} between {start_date} and {end_date}")

        else:
            # No existing runs, trigger the DAG respecting its schedule
            dagbag = DagBag()
            dag = dagbag.get_dag(dag_id)

            if dag:
                current_date = start_date
                while current_date <= end_date:
                    # Calculate the next scheduled time based on the DAG's schedule
                    next_scheduled = dag.schedule_interval.next(current_date)
                    if start_date <= next_scheduled <= end_date:
                        print(f"Triggering DAG run: {dag_id} - {next_scheduled}")
                        dag_run = dag.create_dagrun(
                            run_id=f"manual__{next_scheduled.strftime('%Y%m%dT%H%M%S')}",
                            state=State.RUNNING,
                            conf={},
                            execution_date=next_scheduled,
                            data_interval=(next_scheduled, dag.schedule_interval.next(next_scheduled)),
                            external_trigger=True,
                        )
                        session.add(dag_run)
                        session.commit()
                    current_date += timedelta(days=1)
                print(f"Triggered DAG runs for {dag_id} between {start_date} and {end_date} respecting schedule")
            else:
                print(f"DAG with ID '{dag_id}' not found.")


dag_id_to_clear_run = "dag_jinja_str_dates"   # replace with your dag id
start_date_to_clear = pendulum.datetime(2024, 12, 1, tz="UTC")  # Use pendulum.datetime
end_date_to_clear = pendulum.datetime(2025, 3, 3, tz="UTC")

with DAG(
    dag_id='dag_delete_dag_run',
    schedule_interval='00 20 * * *',         # Runs at 20:00 daily
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
) as dag:

    hello_task = PythonOperator(
        task_id='delete_dag_run',
        python_callable=clear_and_delete_dag,
        op_args=[dag_id_to_clear_run, start_date_to_clear, end_date_to_clear]
    )
