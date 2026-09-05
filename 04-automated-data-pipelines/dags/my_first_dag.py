from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone


with DAG(
    dag_id="my_first_dag",
    schedule=None,
    start_date=timezone.datetime(2026, 8, 30),
):

    t1 = EmptyOperator(task_id="t1")
    t2 = EmptyOperator(task_id="t2")

    t1 >> t2