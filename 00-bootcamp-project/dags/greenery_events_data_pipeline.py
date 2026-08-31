import csv
import json

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone

import requests
from google.cloud import bigquery, storage
from google.oauth2 import service_account


BUSINESS_DOMAIN = "greenery"
LOCATION = "asia-southeast1"
GCP_PROJECT_ID = "YOUR_GCP_PROJECT_ID"
DAGS_FOLDER = "/opt/airflow/dags"
DATA = "events"


# def _extract_data(**context):
#     ds = context["ds"]

def _extract_data(ds):
    url = f"http://34.87.139.82:8000/{DATA}/?created_at={ds}"
    response = requests.get(url)
    data = response.json()

    if data:
        with open(f"{DAGS_FOLDER}/{DATA}-{ds}.csv", "w") as f:
            writer = csv.writer(f)
            header = [
                "event_id",
                "session_id",
                "page_url",
                "created_at",
                "event_type",
                "user",
                "order",
                "product",
            ]
            writer.writerow(header)
            for each in data:
                data = [
                    each["event_id"],
                    each["session_id"],
                    each["page_url"],
                    each["created_at"],
                    each["event_type"],
                    each["user"],
                    each["order"],
                    each["product"]
                ]
                writer.writerow(data)


def _load_data_to_gcs(ds):
    # YOUR CODE HERE


def _load_data_from_gcs_to_bigquery(ds):
    # YOUR CODE HERE


default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2021, 2, 9),
}
with DAG(
    dag_id="greenery_events_data_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["DEB", "Skooldio", "greenery"],
):

    # Extract data from Postgres, API, or SFTP
    extract_data = EmptyOperator(
        task_id="extract_data",
    )

    # Load data to GCS
    load_data_to_gcs = EmptyOperator(
        task_id="load_data_to_gcs",
    )
    
    # Submit a Spark app to transform data
    transform_data = EmptyOperator(
        task_id="transform_data",
    )

    # Load data from GCS to BigQuery
    load_data_from_gcs_to_bigquery = EmptyOperator(
        task_id="load_data_from_gcs_to_bigquery",
    )

    # Task dependencies
    extract_data >> load_data_to_gcs >> transform_data >> load_data_from_gcs_to_bigquery