# import csv
import os
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
from airflow.exceptions import AirflowSkipException
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
DEFAULT_LLM_PROVIDER = 'GEMINI'
PII_KEYWORDS = [
    "name",
    "first_name",
    "last_name",
    "full_name",
    "email",
    "phone",
    "mobile",
    "address",
    "street",
    "city",
    "state",
    "zipcode",
    "zip_code",
    "postal_code",
    "date_of_birth",
    "dob",
]
FINANCIAL_KEYWORDS = [
    "account_number",
    "bank_account",
    "credit_card",
    "card_number",
    "debit_card",
    "cvv",
    "cvc",
    "iban",
    "swift",
    "routing_number",
    "salary",
    "income",
    "balance",
    "payment",
    "transaction",
    "amount",
    "price",
    "cost",
    "total",
    "discount",
    "tax",
]
CREDENTIAL_KEYWORDS = [
    "password",
    "passwd",
    "passcode",
    "pin",
    "secret",
    "api_key",
    "apikey",
    "access_token",
    "refresh_token",
    "auth_token",
    "authorization",
    "private_key",
    "client_secret",
]
IDENTIFIER_KEYWORDS = [
    "national_id",
    "citizen_id",
    "passport",
    "driver_license",
    "social_security",
    "tax_id",
]
HEALTH_KEYWORDS = [
    "medical",
    "patient",
    "diagnosis",
    "disease",
    "medication",
    "prescription",
    "blood_type",
    "health",
]
SKIP_KEYWORDS = [
    "product_name",
]

MASKING_POLICY = {
    "PII_MASKING": PII_KEYWORDS,
    "FINANCIAL_MASKING": FINANCIAL_KEYWORDS,
    "CREDENTIAL_MASKING": CREDENTIAL_KEYWORDS,
    "IDENTIFIER_MASKING": IDENTIFIER_KEYWORDS,
    "HEALTH_MASKING": HEALTH_KEYWORDS,
}

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
    
def _classify_column(column_name):

    column_name = column_name.lower()

    if column_name in SKIP_KEYWORDS:
        return None

    for category, keywords in MASKING_POLICY.items():

        for keyword in keywords:

            if keyword in column_name:
                return category

    return None

def _data_masking(**context):

    ti = context["ti"]

    sample_data = ti.xcom_pull(
        task_ids="sample_data_extraction"
    )

    information_schema = ti.xcom_pull(
        task_ids="information_schema_extraction"
    )

    for column in information_schema:

        column_name = column["column_name"]

        category = _classify_column(column_name)

        if category is None:
            continue

        for row in sample_data:

            if column_name in row:
                row[column_name] = f"MASKED_{category}"

    return sample_data

def _system_prompt(path_to_yaml, **context):

    ti = context["ti"]

    data_from_data_masking = ti.xcom_pull(
        task_ids="data_masking"
    )

    data_from_information_schema = ti.xcom_pull(
        task_ids="information_schema_extraction"
    )

    with open(path_to_yaml, "r") as f:
        expected_sample_model_schema = f.read()

    system_prompt = f"""
        You are a data engineering assistant responsible for generating a dbt schema YAML file.

        You are given:

        1. The extracted database schema:
        {data_from_information_schema}

        2. Masked sample data:
        {data_from_data_masking}

        3. The expected dbt YAML schema:
        {expected_sample_model_schema}


        STRICT REQUIREMENTS:

        - Generate the final output following the expected dbt YAML schema exactly.
        - Do NOT skip any model.
        - Do NOT skip any column.
        - Do NOT remove columns even if the column appears unimportant.
        - Preserve every model and column from the provided database schema.
        - Use the expected YAML structure as the required format.
        - Preserve the `version: 2` structure.
        - Each model must be under `models:`.
        - Each column must be under its corresponding model's `columns:`.
        - Include a `name` for every model and every column.
        - Include descriptions where they can be determined from the provided schema or sample data.
        - Generate appropriate dbt data tests when they can be determined from the provided information.
        - Do not invent columns that do not exist in the source schema.
        - Do not invent data values.
        - Do not expose or reconstruct masked sensitive data.
        - Do not change the masked values back to their original values.
        - Preserve existing relationships, uniqueness, nullability, accepted values, and other test requirements when they can be determined.
        - The final result must be valid YAML.
        - Return ONLY the YAML content.
        - Do NOT include Markdown code fences.
        - Do NOT include explanations before or after the YAML.

        The expected structure is the template and must be followed strictly.
        """

    return system_prompt

def _LLM_selector(llm_provider):

    if llm_provider == "OPENAI":
        return os.getenv("OPENAI_API_KEY")

    elif llm_provider == "GEMINI":
        return os.getenv("GEMINI_API_KEY")

    elif llm_provider == "CLAUDE":
        return os.getenv("CLAUDE_API_KEY")

    else:
        raise AirflowSkipException(
            f"Unsupported LLM provider: {llm_provider}"
        )

def _calling_LLM(LLM_provider, **context):

    ti = context["ti"]

    system_prompt = ti.xcom_pull(
        task_ids="system_prompt"
    )

    API_KEY = _LLM_selector(LLM_provider)

    if LLM_provider == "OPENAI":

        from openai import OpenAI

        client = OpenAI(api_key=API_KEY)

        response = client.responses.create(
            model="gpt-5",
            instructions=system_prompt,
            input="Generate the dbt YAML."
        )

        result = response.output_text

    elif LLM_provider == "GEMINI":

        from google import genai

        client = genai.Client(api_key=API_KEY)

        response = client.models.generate_content(
            model="gemini-3.8-flash",
            contents=system_prompt
        )

        result = response.text

    elif LLM_provider == "CLAUDE":

        from anthropic import Anthropic

        client = Anthropic(api_key=API_KEY)

        response = client.messages.create(
            model="claude-opus-5",
            max_tokens=8192,
            system=system_prompt,
            messages=[
                {
                    "role": "user",
                    "content": "Generate the dbt YAML."
                }
            ]
        )

        result = response.content[0].text

    else:
        raise ValueError(
            f"Unsupported LLM provider: {LLM_provider}"
        )

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
        python_callable=_data_masking,
    )

    system_prompt = PythonOperator(
        task_id="system_prompt",
        python_callable=_system_prompt,
        op_kwargs={
            "path_to_yaml": "00-bootcamp-project/dbt/greenery/models/staging/greenery/_models.yml",
        },
    )

    calling_LLM = PythonOperator(
        task_id="calling_LLM",
        python_callable=_calling_LLM,
        op_kwargs={
            "LLM_provider": DEFAULT_LLM_PROVIDER,
        },
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