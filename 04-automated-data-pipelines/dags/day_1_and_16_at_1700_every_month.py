from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def _world():
    return "World"


with DAG(
    dag_id="day_1_and_16_at_1700_every_month",
    schedule="0 17 1,16 * *",
    start_date=timezone.datetime(2026, 8, 30),
    catchup=False,
    tags=["DEB", "Skooldio"],
):

    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'Hello'",
    )

    world = PythonOperator(
        task_id="world",
        python_callable=_world,
    )

    hello >> world