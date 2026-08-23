import json
import os
import sys

from google.api_core import exceptions
from google.cloud import storage
from google.oauth2 import service_account


def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"
    # The path to your file to upload
    # source_file_name = "local/path/to/file"
    # The ID of your GCS object
    # destination_blob_name = "storage-object-name"

    # keyfile = os.environ.get("KEYFILE_PATH")
    # Step 1. Also, add key file to the `keyfile` variable
    # Step 2. Add project-id, available in key file
    keyfile = "deb-load-data-to-gcs.json"
    service_account_info = json.load(open(keyfile))
    credentials = service_account.Credentials.from_service_account_info(service_account_info)
    project_id = "project-d069ecb2-d645-45e0-a1b"

    storage_client = storage.Client(
        project=project_id,
        credentials=credentials,
    )
    bucket = storage_client.bucket(bucket_name)

    # try:
    #     bucket.delete_blob(blob_name=destination_blob_name)
    # except exceptions.NotFound as ex:
    #     print(f"File {destination_blob_name} not found")

    blob = bucket.blob(destination_blob_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.
    # generation_match_precondition = 0
    # blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

    blob.upload_from_filename(source_file_name)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

# To run this script 3 parameters are required, these parameters are based on coded written in the main.py, varied by the code authored.
# They are bucket_name, source_file, destination_bucket
# To run use the command `poetry run python main.py $bucket_name $source_file $destination`. Dont forgot to `poetry install` to install package.
# Sample: `poetry run python main.py earth-deb06-week03-attempt01 events.avro cleaned/events.avro`
if __name__ == "__main__":
    upload_blob(
        bucket_name=sys.argv[1],
        source_file_name=sys.argv[2],
        destination_blob_name=sys.argv[3],
    )