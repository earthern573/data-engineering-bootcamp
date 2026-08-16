import json
import os

from google.cloud import bigquery
from google.oauth2 import service_account


DATA_FOLDER = "data" # 🚨 ตรวจสอบชื่อโฟลเดอร์ที่ใช้เก็บไฟล์ข้อมูลต่าง ๆ ให้ถูกต้อง

# ตัวอย่างการกำหนด Path ของ Keyfile ในแบบที่ใช้ Environment Variable มาช่วย
# จะทำให้เราไม่ต้อง Hardcode Path ของไฟล์ไว้ในโค้ดของเรา
# keyfile = os.environ.get("KEYFILE_PATH")

keyfile = "deb-loading-to-bigquery.json" # 🚨 แก้ไขชื่อ keyfile ให้ถูกต้อง
service_account_info = json.load(open(keyfile))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
project_id = "project-d069ecb2-d645-45e0-a1b" # 🚨 แก้ไข project_id ให้สอดคล้องกับ GCP project ของตัวเอง, also available in keyfile
client = bigquery.Client(
    project=project_id,
    credentials=credentials,
)

job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
)

# Addressess
data = "addresses"
file_path = f"{DATA_FOLDER}/{data}.csv"
with open(file_path, "rb") as f:
    table_id = f"{project_id}.deb_bootcamp.{data}"
    job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

# ----------

job_config = bigquery.LoadJobConfig(
    skip_leading_rows=1,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    autodetect=True,
    time_partitioning=bigquery.TimePartitioning(
        type_=bigquery.TimePartitioningType.DAY,
        field="created_at",
    ),
)

# Events
dt = "2021-02-10"
partition = dt.replace("-", "")
data = "events"
file_path = f"{DATA_FOLDER}/{data}.csv"
with open(file_path, "rb") as f:
    table_id = f"{project_id}.deb_bootcamp.{data}${partition}"
    job = client.load_table_from_file(f, table_id, job_config=job_config)
    job.result()

table = client.get_table(table_id)
print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

# ถึงตรงนี้เราโหลดข้อมูลไปแล้ว 2 ชุด ยังเหลืออีก 5 ชุดที่ต้องโหลดเพิ่ม
# YOUR CODE HERE