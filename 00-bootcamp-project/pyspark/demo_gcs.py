from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType

# Step 1. Add key_file_path
KEYFILE_PATH = "/opt/spark/pyspark/deb-load-data-to-gcs.json"

# GCS Connector Path (on Spark): /opt/spark/jars/gcs-connector-hadoop3-latest.jar
# GCS Connector Path (on Airflow): /home/airflow/.local/lib/python3.9/site-packages/pyspark/jars/gcs-connector-hadoop3-latest.jar
# spark = SparkSession.builder.appName("demo") \
#     .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
#     .config("spark.memory.offHeap.enabled", "true") \
#     .config("spark.memory.offHeap.size", "5g") \
#     .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
#     .config("google.cloud.auth.service.account.enable", "true") \
#     .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
#     .getOrCreate()

spark = SparkSession.builder.appName("demo_gcs") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .getOrCreate()

# Example schema for Greenery addresses data
# struct_schema = StructType([
#     StructField("address_id", StringType()),
#     StructField("address", StringType()),
#     StructField("zipcode", StringType()),
#     StructField("state", StringType()),
#     StructField("country", StringType()),
# ])

# Example schema for Greenery users data
# struct_schema = StructType([
#     StructField("user_id", StringType()),
#     StructField("first_name", StringType()),
#     StructField("last_name", StringType()),
#     StructField("email", StringType()),
#     StructField("phone_number", StringType()),
#     StructField("created_at", TimestampType()),
#     StructField("updated_at", TimestampType()),
#     StructField("address_id", StringType()),
# ])

# Step 2. Comment Line 47 - 61, and used Line 63 - 77 instead
# GCS_FILE_PATH = "gs://YOUR_BUCKET_PATH_TO_CSV_FILE"

# df = spark.read \
#     .option("header", True) \
#     .option("inferSchema", True) \
#     .csv(GCS_FILE_PATH)

# # df = spark.read \
# #     .option("header", True) \
# #     .schema(struct_schema) \
# #     .csv(GCS_FILE_PATH)

# df.show()
# df.printSchema()

data = [
    ("James", "", "Smith", "1991-04-01", "M", 3000),
    ("Michael", "Rose", "", "2000-05-19", "M", 4000),
    ("Maria", "Anne", "Jones", "1967-12-01", "F", 4000),
    ("Jen", "Mary", "Brown", "1980-02-17", "F", -1),
]
columns = [
    "firstname",
    "middlename",
    "lastname",
    "dob",
    "gender",
    "salary",
]
df = spark.createDataFrame(data=data, schema=columns)

# Step 3 (optional) Rename `YOUR_TABLE_NAME`
df.createOrReplaceTempView("hello")
result = spark.sql("""
    select
        *

    from hello
""")

# Step 4. Declare output destination, in GCS bucket
# OUTPUT_PATH = "gs://earth-deb06-week03-attempt01/output"
# [30-08-2026] modify path for Week-04 bootcamp
OUTPUT_PATH = "gs://deb-bootcamp-06-earth/cleaned/greenery/addresses"

# Step 5. [Optional] `.mode` write mode and `OUTPUT_PATH` declare at line 89
# result.write.mode("overwrite").parquet(OUTPUT_PATH)
# [30-08-2026] modify output file to csv
result.write.mode("overwrite").csv(OUTPUT_PATH)

# Step 6. Modify Permission, by adding the following permission `storage.objects.get` and `storage.objects.list` to the custom role in IAM on Google Cloud.
# Step 7. Run cmd `make submit`
# Step 8. In terminal the prompt shown `Enter PySpark relative path:` >> Pass `demo_gcs.py` into the prompt and wait