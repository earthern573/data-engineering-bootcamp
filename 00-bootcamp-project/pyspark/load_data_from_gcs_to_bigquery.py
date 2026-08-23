import json

from google.cloud import bigquery
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
project_id = "YOUR_PROJECT_ID"
location = "asia-southeast1"
bucket_name = "deb-bootcamp-YOUR_STUDENT_ID"
data = "addresses"

# Prepare and Load Credentials to Connect to GCP Services
keyfile_bigquery = "YOUR_KEYFILE_PATH_FOR_GCS_TO_BIGQUERY"
service_account_info_bigquery = json.load(open(keyfile_bigquery))
credentials_bigquery = service_account.Credentials.from_service_account_info(
    service_account_info_bigquery
)

# Load data from GCS to BigQuery
bigquery_client = bigquery.Client(
    project=project_id,
    credentials=credentials_bigquery,
    location=location,
)

source_data_in_gcs = f"gs://{bucket_name}/cleaned/{BUSINESS_DOMAIN}/{data}/*.parquet"
table_id = f"{project_id}.deb_bootcamp.{data}"
job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.PARQUET,
    # autodetect=True,
    schema=[
        bigquery.SchemaField("address_id", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("address", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("zipcode", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("state", bigquery.SqlTypeNames.STRING),
        bigquery.SchemaField("country", bigquery.SqlTypeNames.STRING),
    ],
)
job = bigquery_client.load_table_from_uri(
    source_data_in_gcs,
    table_id,
    job_config=job_config,
    location=location,
)
job.result()

table = bigquery_client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")