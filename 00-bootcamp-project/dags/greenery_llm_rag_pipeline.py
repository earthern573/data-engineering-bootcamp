import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.hooks.base import BaseHook

import pandas as pd
from google import genai
from google.genai import types
from google.cloud import bigquery, storage
from google.oauth2 import service_account

from openai import OpenAI
from io import BytesIO


GCP_PROJECT_ID = "project-d069ecb2-d645-45e0-a1b"
DATASET_ID = "deb_bootcamp"
TABLE_ID = "YOUR_TABLE_ID"
BUCKET_NAME = "deb-bootcamp-06-earth"
KEYFILE = "/opt/airflow/dags/deb-dbt-bigquery.json"
KEYFILE_LOAD_TO_GCS = "/opt/airflow/dags/deb-load-data-to-gcs.json"
# GEMINI_API_KEY = "YOUR_GEMINI_API_KEY"
DAGS_FOLDER = "/opt/airflow/dags"


def _gather_data(dataset_id, table_id, ds):
    service_account_info = json.load(open(KEYFILE))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    bigquery_client = bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials,
    )

    month = 2
    year = 2021

    query = f"""
        SELECT
	          product_name
	          , count(1) as record_count
        FROM `{GCP_PROJECT_ID}.{dataset_id}.{table_id}`
        WHERE
            state = 'California'
            AND EXTRACT(MONTH FROM order_created_at_utc) = {month}
            AND EXTRACT(YEAR FROM order_created_at_utc) = {year}
        GROUP BY product_name
        ORDER BY 2 DESC
        LIMIT 3
    """
    query_job = bigquery_client.query(query)
    results = query_job.result()

    products = []
    for row in results:
        products.append(row.product_name)

    houseplants = ", ".join(products)

    query = f"""
        SELECT
            count(1) as record_count
        FROM `{GCP_PROJECT_ID}.{dataset_id}.{table_id}`
        WHERE
            state = 'California'
            AND EXTRACT(MONTH FROM order_created_at_utc) = {month}
            AND EXTRACT(YEAR FROM order_created_at_utc) = {year}
    """
    query_job = bigquery_client.query(query)
    results = query_job.result()

    data = []
    for row in results:
        data.append(row.record_count)

    number_of_orders = data[0]

    date_object = datetime.strptime(ds, "%Y-%m-%d")
    formatted_date = date_object.strftime("%B %Y")

    df = pd.DataFrame(data={
        "text": [
            f"In California, the most ordered houseplants are {houseplants}. In {formatted_date}, there were {number_of_orders} orders.",
        ]
    })
    df.to_parquet(f"{DAGS_FOLDER}/greenery-summary-data.parquet")

def _load_data_to_gcs():
    service_account_info_gcs = json.load(open(KEYFILE_LOAD_TO_GCS))
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

    file_path = f"{DAGS_FOLDER}/greenery-summary-data.parquet"
    destination_blob_name = f"summarized/greenery/context/greenery-summary-data.parquet"
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

def _get_embeddings():
    # df = pd.read_parquet(f"{DAGS_FOLDER}/greenery-summary-data.parquet")

    service_account_info_gcs = json.load(open(KEYFILE_LOAD_TO_GCS))
    credentials_gcs = service_account.Credentials.from_service_account_info(
        service_account_info_gcs
    )

    client = storage.Client(
        credentials=credentials_gcs
    )

    bucket = client.bucket(BUCKET_NAME)

    blob = bucket.blob(
        "summarized/greenery/context/greenery-summary-data.parquet"
    )

    data = blob.download_as_bytes()

    df = pd.read_parquet(BytesIO(data))


    def generate_embeddings(text):
        conn = BaseHook.get_connection("llm_openai")
        api_key = conn.password
        client = OpenAI(api_key=api_key)
        result = client.embeddings.create(
            model="text-embedding-3-small",
            input=text,
        )

        print(text)

        return result.data[0].embedding


		# YOUR CODE HERE
    df["embedding"] = df.text.map(generate_embeddings)
    df.to_parquet(f"{DAGS_FOLDER}/greenery-summary-data-with-embeddings.parquet", index=False)



def _load_data_to_bigquery():
    df = pd.read_parquet(f"{DAGS_FOLDER}/greenery-summary-data-with-embeddings.parquet")

    # service_account_info_gcs = json.load(open(KEYFILE_LOAD_TO_GCS))
    # credentials_gcs = service_account.Credentials.from_service_account_info(
    #     service_account_info_gcs
    # )

    # client = storage.Client(
    #     credentials=credentials_gcs
    # )

    # bucket = client.bucket(BUCKET_NAME)

    # blob = bucket.blob(
    #     "summarized/greenery/context/greenery-summary-data-with-embeddings.parquet"
    # )

    # data = blob.download_as_bytes()

    # df = pd.read_parquet(BytesIO(data))

    # YOUR CODE HERE
    service_account_info = json.load(open(KEYFILE))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    bigquery_client = bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials,
    )

    schema = [
        bigquery.SchemaField("text", "STRING"),
        bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE"
    )
    table_id = f"{GCP_PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    load_job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()

    print(f"Loaded {load_job.output_rows} rows into {table_id}")
    

with DAG(
    dag_id="greenery_llm_rag_pipeline",
    schedule="@daily",
    start_date=timezone.datetime(2024, 3, 10),
    catchup=False,
    tags=["DEB", "Skooldio"],
):

    gather_data = PythonOperator(
        task_id="gather_data",
        python_callable=_gather_data,
        op_kwargs={
            "dataset_id": "deb_bootcamp",
            "table_id": "fct_orders",
        },
    )

    load_data_to_gcs = PythonOperator(
            task_id="load_data_to_gcs",
            python_callable=_load_data_to_gcs,
    )

    get_embeddings = PythonOperator(
        task_id="get_embeddings",
        python_callable=_get_embeddings,
    )

    load_data_to_bigquery = PythonOperator(
        task_id="load_data_to_bigquery",
        python_callable=_load_data_to_bigquery,
    )

    gather_data >> load_data_to_gcs >> get_embeddings >> load_data_to_bigquery