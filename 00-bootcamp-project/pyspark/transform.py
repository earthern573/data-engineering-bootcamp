from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType


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

spark = SparkSession.builder.appName("transform") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .getOrCreate()

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

data_no_partition = ['addresses', 'products', 'order_items', 'promos']

for data in data_no_partition:
    GCS_FILE_PATH = f"gs://deb-bootcamp-06-earth/raw/greenery/{data}/{data}.csv"

    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(GCS_FILE_PATH)

    # df = spark.read \
    #     .option("header", True) \
    #     .schema(struct_schema) \
    #     .csv(GCS_FILE_PATH)

    df.show()

    df.createOrReplaceTempView(f"{data}")
    result = spark.sql(f"""
        select
            *

        from {data}
    """)

    OUTPUT_PATH = f"gs://deb-bootcamp-06-earth/cleaned/greenery/{data}"
    result.write.mode("overwrite").parquet(OUTPUT_PATH)

data_with_partition = [
    {'data': 'events', 'dt': '2021-02-10'},
    {'data': 'orders', 'dt': '2021-02-10'},
    {'data': 'users', 'dt': '2020-10-23'}
]

for item in data_with_partition:

    data = item['data']
    dt = item['dt']

    GCS_FILE_PATH = f"gs://deb-bootcamp-06-earth/raw/greenery/{data}/{dt}/{data}.csv"

    df = spark.read \
        .option("header", True) \
        .option("inferSchema", True) \
        .csv(GCS_FILE_PATH)

    # df = spark.read \
    #     .option("header", True) \
    #     .schema(struct_schema) \
    #     .csv(GCS_FILE_PATH)

    df.show()

    df.createOrReplaceTempView(f"{data}")
    result = spark.sql(f"""
        select
            *

        from {data}
    """)

    OUTPUT_PATH = f"gs://deb-bootcamp-06-earth/cleaned/greenery/{data}/{dt}"
    result.write.mode("overwrite").parquet(OUTPUT_PATH)