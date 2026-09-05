from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType

# Step 1. Add key_file_path
KEYFILE_PATH = "/opt/spark/pyspark/deb-load-data-to-gcs.json"

spark = SparkSession.builder.appName("demo_gcs") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .getOrCreate()

# read the real input instead of hardcoded data
GCS_INPUT_PATH = "gs://deb-bootcamp-06-earth/raw/greenery/promos/promos.csv"
df = spark.read.option("header", True).csv(GCS_INPUT_PATH)

# Step 3 (optional) Rename `YOUR_TABLE_NAME`
df.createOrReplaceTempView("YOUR_TABLE_NAME")
result = spark.sql("""
    select
        *

    from YOUR_TABLE_NAME
""")

# same bucket BigQuery reads from
OUTPUT_PATH = "gs://deb-bootcamp-06-earth/cleaned/greenery/promos"
result.write.mode("overwrite").option("header", True).csv(OUTPUT_PATH)