import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone

import pandas as pd
from google import genai
from google.genai import types
from google.cloud import bigquery
from google.oauth2 import service_account

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()


GCP_PROJECT_ID = "project-d069ecb2-d645-45e0-a1b"
DATASET_ID = "deb_bootcamp"
TABLE_ID = "courses"
KEYFILE = "/opt/airflow/dags/deb-dbt-bigquery.json"
# GEMINI_API_KEY = "YOUR_GEMINI_API_KEY"
DAGS_FOLDER = "/opt/airflow/dags"


def _gather_data():
    df = pd.DataFrame(data={
        "text": [
            "คอร์ส Probability for Data Science - ความน่าจะเป็นถูกนำมาใช้ในงาน Data Science ในงานด้านวิเคราะห์ข้อมูลและสร้างโมเดล เพื่อทำให้มั่นใจได้ว่าข้อมูลที่ได้มา มันมีความหมาย และมี Insight ถ้าคุณยังต้องนำข้อมูลไปทำโมเดลต่อ ไม่ว่าจะเป็นโมเดล Machine Learning หรือการทำนาย ล้วนก็มีงานใช้งานความน่าจะเป็นในการสร้างโมเดล ซึ่งคุณไม่มีทางจะหลีกเลี่ยงสิ่งนี้ได้ เพราะถ้าคุณไม่เข้าใจความน่าจะเป็นอย่างแท้จริง คุณอาจกำลังใช้โมเดล Machine Learning ที่จำลองสถานการณ์ผิดไปจากโจทย์ที่กำลังเผชิญอยู่ก็ได้! ข่าวดีคือ คุณไม่จำเป็นต้องเป็นอัจฉริยะด้านคณิตศาสตร์ก็สามารถเข้าใจความน่าจะเป็นได้!",
            "คอร์ส Data Pipelines with Airflow - คอร์สออนไลน์สำหรับ Data Engineer คอร์สแรกของไทยที่สอนการสร้าง End-to-End Data Pipelines ด้วย Airflow โดยเป็นคอร์สที่สอนการสร้าง Data Pipelines เพื่อจัดการข้อมูลขนาดใหญ่ (Big Data) แบบ Step by Step ตั้งแต่การอ่านข้อมูล ทำความสะอาด ปรับให้อยู่ในรูปแบบที่เหมาะสม สุดท้ายคือโหลดข้อมูลเข้า Data Lake/Data Warehouse แบบอัตโนมัติ เพื่อนำไปวิเคราะห์ข้อมูล และประกอบการตัดสินใจทางธุรกิจต่อไป",
        ]
    })
    df.to_parquet(f"{DAGS_FOLDER}/course-data.parquet", index=False)


def _get_embeddings():
    df = pd.read_parquet(f"{DAGS_FOLDER}/course-data.parquet")


    # def generate_embeddings(text):
    #     genai_client = genai.Client(api_key=GEMINI_API_KEY)
    #     result = genai_client.models.embed_content(
    #         model="gemini-embedding-exp-03-07",
    #         contents=text,
    #     )

    #     print(text)

    #     return result.embeddings[0].values

    def generate_embeddings(text):
        openai_client = OpenAI()

        result = openai_client.embeddings.create(
            model="text-embedding-3-small",
            input=text,
        )

        print(text)

        return result.data[0].embedding


    df["embedding"] = df.text.map(generate_embeddings)
    df.to_parquet(f"{DAGS_FOLDER}/course-data-with-embeddings.parquet", index=False)


def _load_data_to_bigquery():
    df = pd.read_parquet(f"{DAGS_FOLDER}/course-data-with-embeddings.parquet")

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
    dag_id="llm_course_data_pipeline",
    schedule="@daily",
    start_date=timezone.datetime(2024, 3, 10),
    catchup=False,
    tags=["DEB", "Skooldio"],
):

    gather_data = PythonOperator(
        task_id="gather_data",
        python_callable=_gather_data,
    )

    get_embeddings = PythonOperator(
        task_id="get_embeddings",
        python_callable=_get_embeddings,
    )

    load_data_to_bigquery = PythonOperator(
        task_id="load_data_to_bigquery",
        python_callable=_load_data_to_bigquery,
    )

    gather_data >> get_embeddings >> load_data_to_bigquery