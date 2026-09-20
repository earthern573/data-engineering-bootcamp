import csv
import json
import logging

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils import timezone

from google.cloud import bigquery
from google.oauth2 import service_account


DAGS_FOLDER = "/opt/airflow/dags"
BUSINESS_DOMAIN = "networkrail"
DATA = "movements"
LOCATION = "asia-southeast1"
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
GCS_BUCKET = "YOUR_GCS_BUCKET"
BIGQUERY_DATASET = "networkrail"
KEYFILE_FOR_GCS = "YOUR_KEYFILE_PATH"
KEYFILE_FOR_GCS_TO_BIGQUERY = "YOUR_KEYFILE_PATH"


def _load_data_from_gcs_to_bigquery(data_interval_start, **context):
    ds = data_interval_start.to_date_string()

    # Your code here


default_args = {
    "owner": "Skooldio",
    "start_date": timezone.datetime(2026, 9, 20),
}
with DAG(
    dag_id="networkrail_movements",
    default_args=default_args,
    schedule=None,  # Set the schedule here
    catchup=False,
    tags=["DEB", "Skooldio", "networkrail"],
    max_active_runs=3,
):

    # Start
    start = EmptyOperator(task_id="start")

    # Transform data in data lake using Spark
    transform_data = EmptyOperator(task_id="transform_data")

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = EmptyOperator(task_id="load_data_from_gcs_to_bigquery")

    # End
    end = EmptyOperator(task_id="end", trigger_rule="one_success")

    # Task dependencies
    start >> transform_data >> load_data_from_gcs_to_bigquery >> end