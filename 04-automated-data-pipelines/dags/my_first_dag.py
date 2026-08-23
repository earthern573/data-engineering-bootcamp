from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils import timezone


with DAG(
    "my_first_dag",
    schedule=None,
    start_date=timezone.datetime(2026, 8, 23),
):

    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'hello'",
    )

    world = BashOperator(
        task_id="world",
        bash_command="echo 'world'",
    )

    hello >> world