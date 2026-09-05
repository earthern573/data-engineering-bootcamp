from airflow import DAG
from airflow.macros import ds_format
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def _get_date_part(ds, **context):
    print(ds)
    return ds_format(ds, "%Y-%m-%d", "%Y/%m/%d/")

def _one_plus_one():
    return 1+1

# To rerun Backfill, the schedule parameter is compulsory
# When rerun through UI, we need to specify the range of date to backfill
# To run backfill, `reprocess behavior` possiblilities
# Missing Run = only the missed schedule
# Missing and Error Run = only the missed schedule and the one with errors
# All Runs = all tasks regardless of result, jsut repeating all tasks for every schedules specified
with DAG(
    dag_id="play_with_templating_backfill",
    schedule="@daily",
    start_date=timezone.datetime(2024, 3, 10),
    catchup=False,
    tags=["DEB", "Skooldio"],
):

    run_this = PythonOperator(
        task_id="get_date_part",
        python_callable=_get_date_part,
        op_kwargs={"ds": "{{ ds }}"},
    )

    echo = BashOperator(
        task_id="echo",
        bash_command="echo {{ logical_date }}",
    )

    echo_1 = BashOperator(
        task_id="echo_1",
        bash_command="echo {{ data_interval_start }}",
    )

    echo_2 = BashOperator(
        task_id="echo_2",
        bash_command="echo {{ data_interval_end }}",
    )

    one_plus_one = PythonOperator(
        task_id="one_plus_one",
        python_callable=_one_plus_one,
    )

    # For this operator, name should be add as variable through Airflow UI
    # Admin >> Variables >> Add Variable
    var = BashOperator(
        task_id="var",
        bash_command="echo {{ var.value.name }}",
    )