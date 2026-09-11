# import csv
import json
import yaml
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.operators.dbt import DbtOperator
# from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.empty import EmptyOperator
# from airflow.utils.task_group import TaskGroup
from airflow.utils import timezone
# from airflow.exceptions import AirflowSkipException
# from airflow.utils.state import State

# from google.cloud import bigquery, storage
# from google.oauth2 import service_account

with open("service-account.json") as f:
    credentials = json.load(f)

PROJECT_ID = credentials["project_id"]
REGION_ID = "asia-southeast1"
DATASET_ID = 'deb-earth'
TABLE_ID = ['$table']
N_SAMPLE = 100

def _extract_information_schema(
    project_id=PROJECT_ID,
    region=REGION_ID,
):
    hook = BigQueryHook(
        gcp_conn_id="google_cloud_default",
        use_legacy_sql=False,
    )

    sql = f"""
        SELECT
            c.table_catalog AS project_id
            , c.table_schema AS dataset_id
            , c.table_name AS table_id
            , c.column_name
            , c.data_type
            , p.description
        FROM
            `{project_id}.{region}.INFORMATION_SCHEMA.COLUMNS` AS c
        LEFT JOIN
            `{project_id}.{region}.INFORMATION_SCHEMA.COLUMN_FIELD_PATH` AS p
        ON
            c.table_catalog = p.table_catalog
            AND c.table_schema = p.table_schema
            AND c.table_name = p.table_name
            AND c.column_name = p.column_name
    """

    records = hook.get_records(sql)

    return records

def _extract_sample_data(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    samples=N_SAMPLE,
):
    hook = BigQueryHook(
        gcp_conn_id="google_cloud_default",
        use_legacy_sql=False,
    )

    bq_client = hook.get_client(project_id=project_id)

    table_ref = f"{project_id}.{dataset_id}.{table_id}"

    table_obj = bq_client.get_table(table_ref)

    rows = bq_client.list_rows(
        table_obj,
        max_results=samples,
    )

    df = rows.to_dataframe()

    return df.to_dict(orient="records")
    
def _masking_data(DATA_from_sample_data_extraction_task, DATA_from_information_schema_extraction):
    # extract column from DATA_from_information_schema_extraction
    df_column = DATA_from_information_schema_extraction['column_name']
    df_policy_tag = DATA_from_information_schema_extraction['policy_tag']
    df_column_sample = DATA_from_sample_data_extraction_task['column_value']
    df_column_mask = ['']

    for df_column_element, df_policy_tag_element IN df_column, df_policy_tag:
        for df_column_sample_element IN df_column_sample:
            if df_policy_tag:
                df_column_sample_element = df_column_mask.('Masking')

    return df_column_sample_element

def _system_prompt(DATA_from_data_masking, DATA_from_information_schema_extraction):
    system_prompt = f"schema: {DATA_from_information_schema_extraction} and sample_data_masking: {DATA_from_data_masking}, ..."
    return system_prompt

def _calling_LLM(DATA_from_system_prompt):
    # request model to selected LLM
    result = requests.json()
    return result

def _export_metadata_to_yaml(DATA_from_LLM_response):
    with open($read_yaml_from__calling_LLM, "w") as f:
        writer = yaml.xxx
        ...
    return yaml_file

def _dbt_test_verification(DATA_from_dbt_test_task):
    with open($DATA_from_dbt_test_task, "r") as f:
        dbt_result = yaml.xxx
        ...

    system_prompt = f"here is the test result: {DATA_from_dbt_test_task}, please ... output result PASS or FAIL"

    # request model to selected LLM
    result = system_prompt.json()
    
    return result

default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2021, 2, 9),
}

with DAG(
    dag_id=f"greenery_metadata_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["Metadata", "greenery"],
):

    start = EmptyOperator(task_id="start") 

    information_schema_extraction = PythonOperator(
        task_id="information_schema_extraction",
        python_callable=_extract_information_schema,
        op_kwargs={
            "project_id": PROJECT_ID,
            "region": REGION_ID,
        },
    )

    sample_data_extraction = PythonOperator(
        task_id="sample_data_extraction",
        python_callable=_extract_sample_data,
        op_kwargs={
            "project_id": PROJECT_ID,
            "dataset_id": DATASET_ID,
            "table_id": TABLE_ID,
            "samples": N_SAMPLE,
        },
    )

    data_masking = PythonOperator(
        task_id="data_masking",
        python_callable=_masking_data,
        op_kwargs={"DATA_from_sample_data_extraction_task": $output_sample_data_extraction_task, "DATA_from_information_schema_extraction": $output_information_schema_extraction_task},
    )

    system_prompt = PythonOperator(
        task_id="system_prompt",
        python_callable=_system_prompt,
        op_kwargs={"DATA_from_data_masking_task": $output_data_masking_task, "DATA_from_information_schema_extraction": $output_information_schema_extraction_task},
    )

    calling_LLM = PythonOperator(
        task_id="calling_LLM",
        python_callable=_calling_LLM,
        op_kwargs={"DATA_from_system_prompt_task": $output_system_prompt_task},
    )

    write_to_yaml = PythonOperator(
        task_id="write_to_yaml",
        python_callable=_write_to_yaml,
        op_kwargs={"DATA_from_calling_LLM_task": $output_calling_LLM_task},
    )

    dbt_test_task = DbtOperator(
        # dbt test --project-dir $project_directory -t $target --select $target_dataset.target_table
        # need to fensure yaml from the previous task write_to_yaml is add to correct model folder
    )

    dbt_test_verification = PythonOperator(
        task_id="dbt_test_verification",
        python_callable=_dbt_test_verification,
        op_kwargs={"DATA_from_dbt_test_task_task": $output_dbt_test_task_task},
    )

    end = EmptyOperator(task_id="end", trigger_rule="one_success")

    # Task dependencies
    start >> [information_schema_extraction, sample_data_extraction] >> data_masking >> system_prompt >> calling_LLM >> write_to_yaml >> dbt_test_task >> dbt_test_verification >> end