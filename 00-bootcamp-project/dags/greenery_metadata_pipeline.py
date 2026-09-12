import os
import json
import yaml

from google import genai
from openai import OpenAI
from anthropic import Anthropic

from airflow import DAG
from airflow.utils import timezone
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig
from cosmos.profiles import GoogleCloudServiceAccountDictProfileMapping

# Required packages:
# pip install openai google-genai anthropic

with open("service-account.json") as f:
    credentials = json.load(f)

PROJECT_ID = credentials["project_id"]
REGION_ID = "asia-southeast1"
DATASET_ID = 'deb-earth'
TABLE_ID = ['$table']
N_SAMPLE = 100
DEFAULT_LLM_PROVIDER = 'OPENAI'
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
DEFAULT_MODELS = {
    "OPENAI": "gpt-5-mini",
    "GEMINI": "gemini-3.8-flash",
    "CLAUDE": "claude-sonnet-5",
}

DBT_PROJECT_DIR = "/opt/airflow/dbt/greenery"

profile_config = ProfileConfig(
    profile_name="greenery",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountDictProfileMapping(
        conn_id="bigquery_dbt",
        profile_args={
            "schema": "dataset_output",
            "location": "asia-southeast1",
        },
    ),
)

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
        return "llm_openai"

    elif llm_provider == "GEMINI":
        return "llm_gemini"

    elif llm_provider == "CLAUDE":
        return "llm_claude"

    else:
        raise AirflowSkipException(
            f"Unsupported LLM provider: {llm_provider}"
        )

def _llm_connection_test(llm_provider, model=None):

    if model is None:
        model = DEFAULT_MODELS[llm_provider]

    conn_id = _LLM_selector(llm_provider)

    try:
        conn = BaseHook.get_connection(conn_id)
        api_key = conn.password

        if not api_key:
            raise Exception(
                f"API key not found in connection: {conn_id}"
            )

        if llm_provider == "OPENAI":
            client = OpenAI(api_key=api_key)

            response = client.responses.create(
                model=model,
                input="Reply with OK."
            )

        elif llm_provider == "GEMINI":
            client = genai.Client(api_key=api_key)

            response = client.models.generate_content(
                model=model,
                contents="Reply with OK."
            )

        elif llm_provider == "CLAUDE":
            client = Anthropic(api_key=api_key)

            response = client.messages.create(
                model=model,
                max_tokens=10,
                messages=[
                    {
                        "role": "user",
                        "content": "Reply with OK."
                    }
                ]
            )

        return {
            "status": "PASSED",
            "provider": llm_provider,
            "model": model,
            "connection_id": conn_id,
        }

    except Exception as e:
        raise AirflowSkipException(
            f"LLM connection test failed for "
            f"{llm_provider}: {e}"
        )

def _calling_LLM(llm_provider, **context):

    ti = context["ti"]

    system_prompt = ti.xcom_pull(
        task_ids="system_prompt"
    )

    conn_id = _LLM_selector(llm_provider)

    conn = BaseHook.get_connection(conn_id)
    api_key = conn.password

    if not api_key:
        raise AirflowException(
            f"API key not found in connection: {conn_id}"
        )

    model = DEFAULT_MODELS[llm_provider]

    if llm_provider == "OPENAI":

        client = OpenAI(api_key=api_key)

        response = client.responses.create(
            model=model,
            instructions=system_prompt,
            input="Generate the dbt YAML."
        )

        result = response.output_text

    elif llm_provider == "GEMINI":

        client = genai.Client(api_key=api_key)

        response = client.models.generate_content(
            model=model,
            contents=system_prompt
        )

        result = response.text

    elif llm_provider == "CLAUDE":

        client = Anthropic(api_key=api_key)

        response = client.messages.create(
            model=model,
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
        raise AirflowSkipException(
            f"Unsupported LLM provider: {llm_provider}"
        )

    return result

def _write_schema(path_to_save_yaml, **context):
    ti = context["ti"]

    data_from_calling_LLM = ti.xcom_pull(
        task_ids="calling_LLM"
    )

    with open(path_to_save_yaml, "w") as f:
        f.write(data_from_calling_LLM)

    return path_to_save_yaml

def _capture_tests(path_to_yaml):
    with open(path_to_yaml, "r") as f:
        data = yaml.safe_load(f)

    tests = []

    for model in data.get("models", []):
        model_name = model.get("name")

        for column in model.get("columns", []):
            column_name = column.get("name")

            for test in column.get("tests", []):
                tests.append({
                    "model": model_name,
                    "column": column_name,
                    "test": test,
                })

    return tests

def _dbt_test_verification(**context):
    ti = context["ti"]

    before_tests = ti.xcom_pull(
        task_ids="capture_tests_before"
    )

    after_tests = ti.xcom_pull(
        task_ids="capture_tests_after"
    )

    before_tests = set(
        (
            item["model"],
            item["column"],
            str(item["test"]),
        )
        for item in before_tests
    )

    after_tests = set(
        (
            item["model"],
            item["column"],
            str(item["test"]),
        )
        for item in after_tests
    )

    removed_tests = before_tests - after_tests
    added_tests = after_tests - before_tests

    if removed_tests or added_tests:
        raise AirflowException(
            f"DBT test definitions changed.\n"
            f"Removed tests: {removed_tests}\n"
            f"Added tests: {added_tests}"
        )

    result = {
        "status": "PASSED",
        "removed_tests": [],
        "added_tests": [],
    }

    return result

def _generate_report(**context):

    ti = context["ti"]

    # Get XCom results
    information_schema = ti.xcom_pull(
        task_ids="information_schema_extraction"
    )

    sample_data = ti.xcom_pull(
        task_ids="sample_data_extraction"
    )

    masked_data = ti.xcom_pull(
        task_ids="data_masking"
    )

    system_prompt_result = ti.xcom_pull(
        task_ids="system_prompt"
    )

    llm_connection_test = ti.xcom_pull(
        task_ids="llm_connection_test"
    )

    tests_before = ti.xcom_pull(
        task_ids="capture_tests_before"
    )

    tests_after = ti.xcom_pull(
        task_ids="capture_tests_after"
    )

    verification = ti.xcom_pull(
        task_ids="dbt_test_verification"
    )

    # Get task states
    task_ids = [
        "information_schema_extraction",
        "sample_data_extraction",
        "data_masking",
        "system_prompt",
        "llm_connection_test",
        "calling_LLM",
        "capture_tests_before",
        "write_to_yaml",
        "capture_tests_after",
        "dbt_test_verification",
        "dbt_test",
    ]

    task_status = {}

    dag_run = ti.get_dagrun()

    for task_id in task_ids:
        task_instance = dag_run.get_task_instance(task_id)

        task_status[task_id] = (
            task_instance.state
            if task_instance
            else "UNKNOWN"
        )

    report = {
        "information_schema_extraction": {
            "status": task_status["information_schema_extraction"],
            "tables": len(
                set(
                    (
                        x["project_id"],
                        x["dataset_id"],
                        x["table_id"],
                    )
                    for x in information_schema
                )
            )
            if information_schema
            else 0,
            "columns": len(information_schema)
            if information_schema
            else 0,
        },

        "sample_data_extraction": {
            "status": task_status["sample_data_extraction"],
            "records": len(sample_data)
            if sample_data
            else 0,
        },

        "data_masking": {
            "status": task_status["data_masking"],
            "records": len(masked_data)
            if masked_data
            else 0,
        },

        "system_prompt": {
            "status": task_status["system_prompt"],
            "generated": system_prompt_result is not None,
        },

        "llm_connection_test": {
            "status": task_status["llm_connection_test"],
            "provider": (
                llm_connection_test.get("provider")
                if llm_connection_test
                else DEFAULT_LLM_PROVIDER
            ),
            "model": (
                llm_connection_test.get("model")
                if llm_connection_test
                else DEFAULT_MODELS.get(DEFAULT_LLM_PROVIDER)
            ),
            "connection_id": (
                llm_connection_test.get("connection_id")
                if llm_connection_test
                else None
            ),
        },

        "calling_LLM": {
            "status": task_status["calling_LLM"],
            "provider": DEFAULT_LLM_PROVIDER,
            "model": DEFAULT_MODELS.get(DEFAULT_LLM_PROVIDER),
        },

        "capture_tests_before": {
            "status": task_status["capture_tests_before"],
            "total_tests": len(tests_before)
            if tests_before
            else 0,
            "tests": tests_before or [],
        },

        "write_to_yaml": {
            "status": task_status["write_to_yaml"],
        },

        "capture_tests_after": {
            "status": task_status["capture_tests_after"],
            "total_tests": len(tests_after)
            if tests_after
            else 0,
            "tests": tests_after or [],
        },

        "dbt_test_verification": (
            verification
            if verification
            else {
                "status": task_status["dbt_test_verification"],
            }
        ),

        "dbt_test": {
            "status": task_status["dbt_test"],
        },
    }

    return report

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

    llm_connection_test = PythonOperator(
        task_id="llm_connection_test",
        python_callable=_llm_connection_test,
        op_kwargs={
            "llm_provider": DEFAULT_LLM_PROVIDER,
            "model": DEFAULT_MODELS[DEFAULT_LLM_PROVIDER],
        },
    )

    calling_LLM = PythonOperator(
        task_id="calling_LLM",
        python_callable=_calling_LLM,
        op_kwargs={
            "LLM_provider": DEFAULT_LLM_PROVIDER,
        },
    )

    capture_tests_before = PythonOperator(
        task_id="capture_tests_before",
        python_callable=_capture_tests,
        op_kwargs={
            "path_to_yaml": "00-bootcamp-project/dbt/greenery/models/staging/greenery/_models.yml",
        },
    )

    write_schema = PythonOperator(
        task_id="write_schema",
        python_callable=_write_schema,
        op_kwargs={
            "path_to_save_yaml": f"/workspaces/data-engineering-bootcamp/00-bootcamp-project/dbt/greenery/models/staging/greenery/_{TABLE_ID}_schema.yml",
        },
    )

    capture_tests_after = PythonOperator(
        task_id="capture_tests_after",
        python_callable=_capture_tests,
        op_kwargs={
            "path_to_yaml": "base/greenery/models/staging/greenery/_models.yml",
        },
    )

    dbt_test_verification = PythonOperator(
        task_id="dbt_test_verification",
        python_callable=_dbt_test_verification,
    )

    dbt_test = DbtTaskGroup(
        group_id="dbt_test",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
    )

    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=_generate_report,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    
    end = EmptyOperator(task_id="end", trigger_rule="one_success")

    # Task dependencies
(
    start
    >> [information_schema_extraction, sample_data_extraction]
    >> data_masking
    >> system_prompt
    >> llm_connection_test
    >> calling_LLM
    >> capture_tests_before
    >> write_schema
    >> capture_tests_after
    >> dbt_test_verification
    >> dbt_test
    >> generate_report
    >> end
)