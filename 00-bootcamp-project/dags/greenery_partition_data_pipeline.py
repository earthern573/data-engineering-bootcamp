import csv
import json

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils import timezone
from airflow.exceptions import AirflowSkipException
from airflow.utils.state import State

import requests
from google.cloud import bigquery, storage
from google.oauth2 import service_account


BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
GCP_PROJECT_ID = "project-d069ecb2-d645-45e0-a1b"
DAGS_FOLDER = "/opt/airflow/dags"
# 'events' data availability: 2021-02-09 to 2021-02-12
# 'orders' data availability: 2021-02-10 to 2021-02-11
# 'users' data availability: 2020-01-05 to 2020-12-26
DATA_WITH_PARTITION = ['events', 'orders', 'users']

def _extract_data(DATA, ds):
    url = f"http://34.87.139.82:8000/{DATA}/?created_at={ds}"
    response = requests.get(url)
    data = response.json()
    print(type(data), data)

    if data:
        with open(f"{DAGS_FOLDER}/{DATA}-{ds}.csv", "w") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)

def _load_data_to_gcs(DATA, ds):
    keyfile_gcs = f"{DAGS_FOLDER}/deb-load-data-to-gcs.json"
    service_account_info_gcs = json.load(open(keyfile_gcs))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    # Load data from Local to GCS
    bucket_name = "deb-bootcamp-06-earth"
    storage_client = storage.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials_gcs,
    )
    bucket = storage_client.bucket(bucket_name)

    file_path = f"{DAGS_FOLDER}/{DATA}-{ds}.csv"

    try:
        destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{DATA}/{ds}/{DATA}.csv"
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
    except FileNotFoundError:
        # If there is no file exist Airflow will skip this task and proceed
        raise AirflowSkipException(f"{file_path} does not exist, skipping upload for {DATA}")


def _load_data_from_gcs_to_bigquery(DATA, ds):
    keyfile_bigquery = f"{DAGS_FOLDER}/deb-loading-to-bigquery.json"
    service_account_info_bigquery = json.load(open(keyfile_bigquery))
    credentials_bigquery = service_account.Credentials.from_service_account_info(
        service_account_info_bigquery
    )

    bigquery_client = bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials_bigquery,
        location=LOCATION,
    )

    table_id = f"{GCP_PROJECT_ID}.deb_bootcamp.{DATA}"
    job_config = bigquery.LoadJobConfig(
        skip_leading_rows=1,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.CSV,
        autodetect=True,
    )

    bucket_name = "deb-bootcamp-06-earth"
    destination_blob_name = f"cleaned/{BUSINESS_DOMAIN}/{DATA}/{ds}/*.csv"
    job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{destination_blob_name}",
        table_id,
        job_config=job_config,
        location=LOCATION,
    )

    try:
        job.result()
    except Exception as e:
        raise AirflowSkipException(f"BigQuery load failed for {DATA} on {ds}, skipping: {e}")

    table = bigquery_client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

#  a function to check error from spark and skip
# def _check_transform_result(item, **context):
#     ti = context["ti"]
#     transform_ti = ti.get_dagrun().get_task_instance(task_id=f"transform_data.transform_{item}")

#     if transform_ti.state == State.FAILED:
#         raise AirflowSkipException(
#             f"transform_{item} failed (likely missing input file), marking as skipped"
#         )
#     # otherwise do nothing, let downstream proceed normally

default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2021, 2, 9),
}

with DAG(
    dag_id=f"greenery_partition_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["DEB", "Skooldio", "greenery"],
):

    with TaskGroup(group_id="extract_data") as extract_data_group:
        for item in DATA_WITH_PARTITION:
            # Extract data from Postgres, API, or SFTP
            extract_data = PythonOperator(
                # task_id must be unique, thus f-string used to pass item into task instance id
                task_id=f"extract_{item}",
                python_callable=_extract_data,
                # required op_kwargs to pass argument to the python method
                op_kwargs={"DATA": item, "ds": "{{ ds }}"},
            )

    with TaskGroup(group_id="load_data_to_gcs") as load_data_to_gcs_group:
        for item in DATA_WITH_PARTITION:
            # Load data to GCS
            load_data_to_gcs = PythonOperator(
                # task_id must be unique, thus f-string used to pass item into task instance id
                task_id=f"load_{item}_to_gcs",
                python_callable=_load_data_to_gcs,
                # required op_kwargs to pass argument to the python method
                op_kwargs={"DATA": item, "ds": "{{ ds }}"},
            )
 
    with TaskGroup(group_id="transform_data") as transform_data_group:
        for item in DATA_WITH_PARTITION:
            # Submit a Spark app to transform data
            # To run spark, it is crucial to add connection in Airflow by navigate to Admin >> Connections >> Add connection
            transform_data = SparkSubmitOperator(
                task_id=f"transform_{item}",
                # application=f"/opt/spark/pyspark/w04_{item}_from_gcs_to_bq.py",
                application=f"/opt/spark/pyspark/w04_partition_from_gcs_to_bq.py",
                conn_id="my_spark",
                # Send argument to spark, start at index 1
                application_args=[item, "{{ ds }}"],
                # Add trigger rule `all_done` so the AirflowSkipException won't skip this task group 
                trigger_rule="all_done"
            )

            # check_transform_result = PythonOperator(
            #     task_id=f"check_transform_result_{item}",
            #     python_callable=_check_transform_result,
            #     op_kwargs={"item": item},
            #     trigger_rule="all_done",
            # )

            # transform_data >> check_transform_result

    with TaskGroup(group_id="load_data_from_gcs_to_bigquery") as load_data_from_gcs_to_bigquery_group:
        for item in DATA_WITH_PARTITION:
            # Load data from GCS to BigQuery
            load_data_from_gcs_to_bigquery = PythonOperator(
                # task_id must be unique, thus f-string used to pass item into task instance id
                task_id=f"load_{item}_from_gcs_to_bigquery",
                python_callable=_load_data_from_gcs_to_bigquery,
                # required op_kwargs to pass argument to the python method
                op_kwargs={"DATA": item},
                # Add trigger rule `all_done` so the AirflowSkipException won't skip this task group 
                trigger_rule="all_done"
            )

    # Task dependencies
    extract_data_group >> load_data_to_gcs_group >> transform_data_group >> load_data_from_gcs_to_bigquery_group