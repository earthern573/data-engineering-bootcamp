import json

from google.cloud import storage
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
project_id = "YOUR_PROJECT_ID"
location = "asia-southeast1"
bucket_name = "deb-bootcamp-YOUR_STUDENT_ID"
data = "addresses"

# Prepare and Load Credentials to Connect to GCP Services
keyfile_gcs = "YOUR_KEYFILE_PATH_FOR_GCS"
service_account_info_gcs = json.load(open(keyfile_gcs))
credentials_gcs = service_account.Credentials.from_service_account_info(
    service_account_info_gcs
)

# Load data from Local to GCS
storage_client = storage.Client(
    project=project_id,
    credentials=credentials_gcs,
)
bucket = storage_client.bucket(bucket_name)

file_path = f"{DATA_FOLDER}/{data}.csv"
destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{data}/{data}.csv"

# YOUR CODE HERE TO LOAD DATA TO GCS