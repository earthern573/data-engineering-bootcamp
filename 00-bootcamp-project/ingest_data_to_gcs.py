import json

from google.cloud import storage
from google.oauth2 import service_account


DATA_FOLDER = "data"
BUSINESS_DOMAIN = "greenery"
project_id = "project-d069ecb2-d645-45e0-a1b"
location = "asia-southeast1"
bucket_name = "deb-bootcamp-06-earth"
# data = "products"

# Prepare and Load Credentials to Connect to GCP Services
keyfile_gcs = "deb-load-data-to-gcs.json"
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

# Load data without partition to GCS
data_no_partition = ['addresses', 'products', 'order_items', 'promos']

for data in data_no_partition:
    file_path = f"{DATA_FOLDER}/{data}.csv"
    destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{data}/{data}.csv"

    # YOUR CODE HERE TO LOAD DATA TO GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

# Load data with partition to GCS
data_with_partition = [
    {'data': 'events', 'dt': '2021-02-10'},
    {'data': 'orders', 'dt': '2021-02-10'},
    {'data': 'users', 'dt': '2020-10-23'}
]

for item in data_with_partition:

    data = item['data']
    dt = item['dt']

    file_path = f"{DATA_FOLDER}/{data}.csv"
    destination_blob_name = f"raw/{BUSINESS_DOMAIN}/{data}/{dt}/{data}.csv"

    # YOUR CODE HERE TO LOAD DATA TO GCS
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)

