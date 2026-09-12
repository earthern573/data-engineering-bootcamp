import os
import json
import yaml

from google import genai
from openai import OpenAI
# from anthropic import Anthropic

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

PROJECT_ID = "project-d069ecb2-d645-45e0-a1b"
REGION_ID = "asia-southeast1"
DATASET_ID = 'deb_bootcamp'
TABLE_ID = ['addresses', 'products', 'order-items', 'promos', 'events', 'orders', 'users']
N_SAMPLE = 100
DEFAULT_LLM_PROVIDER = 'OPENAI'
MAINTAIN_ORIGINAL_TEST = True
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

# Pay Attention to profile_args.schema >> this control the output schema that send to bigquery
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
    dataset_id=DATASET_ID,
):
    hook = BigQueryHook(
        gcp_conn_id="bigquery_dbt",
        use_legacy_sql=False,
        location="asia-southeast1",
    )

    sql = f"""
        SELECT
            c.table_catalog AS project_id,
            c.table_schema AS dataset_id,
            c.table_name AS table_id,
            c.column_name,
            c.data_type,
            p.description
        FROM
            `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS` AS c
        LEFT JOIN
            `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` AS p
        ON
            c.table_catalog = p.table_catalog
            AND c.table_schema = p.table_schema
            AND c.table_name = p.table_name
            AND c.column_name = p.column_name
    """

    records = hook.get_records(sql)

    columns = [
        "project_id",
        "dataset_id",
        "table_id",
        "column_name",
        "data_type",
        "description",
    ]

    return [
        dict(zip(columns, row))
        for row in records
    ]

def _extract_sample_data(
    project_id=PROJECT_ID,
    dataset_id=DATASET_ID,
    table_id=TABLE_ID,
    samples=N_SAMPLE,
):
    hook = BigQueryHook(
        gcp_conn_id="bigquery_dbt",
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

def _system_prompt(
    path_to_yaml,
    maintain_original_test,
    **context
):

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
        You are a data engineering assistant responsible for generating
        a dbt schema YAML file and providing data quality test recommendations.

        You are given:

        1. The extracted database schema:
        {data_from_information_schema}

        2. Masked sample data:
        {data_from_data_masking}

        3. The expected dbt YAML schema:
        {expected_sample_model_schema}

        4. Maintain original dbt tests:
        {maintain_original_test}

        IMPORTANT CONTEXT:

        The sample data has been masked to protect sensitive information.
        Therefore, recommendations must be based only on the available
        database schema, column metadata, column descriptions, and masked
        sample data.

        Do not attempt to reconstruct, infer, or expose the original values
        of masked sensitive data.

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
        - Include descriptions where they can be determined from the provided
          schema or sample data.
        - Do not invent columns that do not exist in the source schema.
        - Do not invent data values.
        - Do not expose or reconstruct masked sensitive data.
        - Do not change masked values back to their original values.
        - Preserve all existing dbt tests.

        TEST REQUIREMENTS:

        If `maintain_original_test` is True:

        - Do NOT remove any existing dbt test.
        - Do NOT modify any existing dbt test.
        - Do NOT add any new dbt test to the generated YAML.
        - Keep all existing tests exactly as provided.
        - You MAY provide recommendations for additional tests.
        - Recommendations must NOT be added to the generated YAML.

        If `maintain_original_test` is False:

        - Do NOT remove any existing dbt test.
        - Do NOT modify existing dbt tests unnecessarily.
        - New dbt tests MAY be added when they can be reasonably determined
          from the provided schema or masked sample data.
        - New tests must not replace or remove existing tests.
        - You MAY also provide recommendations for additional tests that
          were not added to the YAML.
        - Recommendations may include tests that are better implemented
          as singular SQL tests.

        TEST RECOMMENDATIONS:

        Provide recommendations when there are reasonable opportunities
        to improve data quality testing.

        Recommendations may include:

        - not_null tests
        - unique tests
        - accepted_values tests
        - relationships tests
        - dbt-expectations tests
        - other appropriate dbt tests
        - singular SQL tests for business rules or cross-column validations
          that cannot be represented well using standard YAML tests

        For singular SQL test recommendations:

        - Do NOT generate or execute the SQL file.
        - Only provide a recommendation describing what the singular test
          should validate.
        - Include a suggested file name when appropriate.
        - Make clear that it is a recommendation only.

        IMPORTANT:

        Recommendations are suggestions, not confirmed data-quality failures.

        Because the sample data is masked and may be limited, do not claim
        that a recommended test is definitely required unless the provided
        information clearly supports that conclusion.

        OUTPUT FORMAT:

        Return ONLY valid JSON.

        The JSON must contain exactly these top-level fields:

        {{
            "yaml": "<complete dbt YAML content as a string>",
            "recommendations": [
                {{
                    "type": "column_test | singular_test | other",
                    "model": "<model name>",
                    "column": "<column name or null>",
                    "recommendation": "<description>",
                    "reason": "<reason for recommendation>",
                    "suggested_path": "<path or null>"
                }}
            ]
        }}

        REQUIREMENTS FOR THE `yaml` FIELD:

        - The value must contain the complete valid dbt YAML content.
        - Follow the expected YAML structure exactly.
        - Do NOT include Markdown code fences.

        REQUIREMENTS FOR `recommendations`:

        - Return an empty list if there are no useful recommendations.
        - Do not recommend tests based on information that is unavailable.
        - Do not include sensitive or unmasked sample data.
        - For singular tests, `column` may be null when the test involves
          multiple columns or a model-level business rule.
        - `suggested_path` may be null for normal dbt YAML tests.
        - For singular tests, provide a suggested `.sql` filename when useful.

        The expected YAML structure is the template and must be followed strictly.
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
            input="Generate the dbt YAML and test recommendations."
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
                    "content": (
                        "Generate the dbt YAML and test recommendations."
                    )
                }
            ]
        )

        result = response.content[0].text

    else:
        raise AirflowSkipException(
            f"Unsupported LLM provider: {llm_provider}"
        )

    try:
        llm_result = json.loads(result)
    except json.JSONDecodeError as e:
        raise AirflowException(
            f"LLM returned invalid JSON: {e}"
        )

    if "yaml" not in llm_result:
        raise AirflowException(
            "LLM response does not contain the required 'yaml' field."
        )

    if "recommendations" not in llm_result:
        llm_result["recommendations"] = []

    return llm_result

def _write_schema(path_to_save_yaml, **context):

    ti = context["ti"]

    data_from_calling_LLM = ti.xcom_pull(
        task_ids="calling_LLM"
    )

    if not data_from_calling_LLM:
        raise AirflowException(
            "No result received from calling_LLM."
        )

    yaml_content = data_from_calling_LLM.get("yaml")

    if not yaml_content:
        raise AirflowException(
            "LLM result does not contain YAML content."
        )

    with open(path_to_save_yaml, "w") as f:
        f.write(yaml_content)

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

def _dbt_test_verification(
    maintain_original_test,
    **context
):

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

    # Existing tests must always be preserved
    if removed_tests:
        if maintain_original_test:
            raise AirflowException(
                f"Original DBT tests were removed while "
                f"maintain_original_test=True.\n"
                f"Removed tests: {removed_tests}"
            )
        else:
            raise AirflowException(
                f"Original DBT tests were removed.\n"
                f"Existing tests must always be preserved.\n"
                f"Removed tests: {removed_tests}"
            )

    # New tests depend on the flag
    if added_tests and maintain_original_test:
        raise AirflowException(
            f"New DBT tests were added while "
            f"maintain_original_test=True.\n"
            f"Added tests: {added_tests}"
        )

    result = {
        "status": "PASSED",
        "maintain_original_test": maintain_original_test,
        "removed_tests": list(removed_tests),
        "added_tests": list(added_tests),
    }

    if added_tests and not maintain_original_test:
        result["status"] = "PASSED_WITH_WARNING"
        result["warning"] = (
            "New DBT tests were added. "
            "Original tests were preserved."
        )

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

    calling_llm_result = ti.xcom_pull(
        task_ids="calling_LLM"
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

    # Get LLM recommendations
    recommendations = []

    if calling_llm_result:
        recommendations = calling_llm_result.get(
            "recommendations",
            []
        )

    # Determine task status from available XCom results
    task_status = {
        "information_schema_extraction": (
            "SUCCESS"
            if information_schema is not None
            else "UNKNOWN"
        ),

        "sample_data_extraction": (
            "SUCCESS"
            if sample_data is not None
            else "UNKNOWN"
        ),

        "data_masking": (
            "SUCCESS"
            if masked_data is not None
            else "UNKNOWN"
        ),

        "system_prompt": (
            "SUCCESS"
            if system_prompt_result is not None
            else "UNKNOWN"
        ),

        "llm_connection_test": (
            "SUCCESS"
            if llm_connection_test is not None
            else "UNKNOWN"
        ),

        "calling_LLM": (
            "SUCCESS"
            if calling_llm_result is not None
            else "UNKNOWN"
        ),

        "capture_tests_before": (
            "SUCCESS"
            if tests_before is not None
            else "UNKNOWN"
        ),

        "write_to_yaml": (
            "SUCCESS"
            if calling_llm_result is not None
            else "UNKNOWN"
        ),

        "capture_tests_after": (
            "SUCCESS"
            if tests_after is not None
            else "UNKNOWN"
        ),

        "dbt_test_verification": (
            verification.get("status")
            if verification
            else "UNKNOWN"
        ),

        "dbt_test": "UNKNOWN",
    }

    report = {
        "information_schema_extraction": {
            "status": task_status[
                "information_schema_extraction"
            ],
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
            "status": task_status[
                "sample_data_extraction"
            ],
            "records": len(sample_data)
            if sample_data
            else 0,
        },

        "data_masking": {
            "status": task_status[
                "data_masking"
            ],
            "records": len(masked_data)
            if masked_data
            else 0,
        },

        "system_prompt": {
            "status": task_status[
                "system_prompt"
            ],
            "generated": (
                system_prompt_result is not None
            ),
        },

        "llm_connection_test": {
            "status": task_status[
                "llm_connection_test"
            ],
            "provider": (
                llm_connection_test.get("provider")
                if llm_connection_test
                else DEFAULT_LLM_PROVIDER
            ),
            "model": (
                llm_connection_test.get("model")
                if llm_connection_test
                else DEFAULT_MODELS.get(
                    DEFAULT_LLM_PROVIDER
                )
            ),
            "connection_id": (
                llm_connection_test.get("connection_id")
                if llm_connection_test
                else None
            ),
        },

        "calling_LLM": {
            "status": task_status[
                "calling_LLM"
            ],
            "provider": DEFAULT_LLM_PROVIDER,
            "model": DEFAULT_MODELS.get(
                DEFAULT_LLM_PROVIDER
            ),
            "generated_yaml": (
                calling_llm_result is not None
                and bool(
                    calling_llm_result.get("yaml")
                )
            ),
            "recommendation_count": len(
                recommendations
            ),
        },

        "test_recommendations": {
            "total": len(recommendations),
            "recommendations": recommendations,
            "note": (
                "Recommendations are based on the available "
                "schema and masked sample data and should be "
                "reviewed before implementation."
            ),
        },

        "capture_tests_before": {
            "status": task_status[
                "capture_tests_before"
            ],
            "total_tests": len(tests_before)
            if tests_before
            else 0,
            "tests": tests_before or [],
        },

        "write_to_yaml": {
            "status": task_status[
                "write_to_yaml"
            ],
        },

        "capture_tests_after": {
            "status": task_status[
                "capture_tests_after"
            ],
            "total_tests": len(tests_after)
            if tests_after
            else 0,
            "tests": tests_after or [],
        },

        "dbt_test_verification": (
            verification
            if verification
            else {
                "status": "UNKNOWN",
            }
        ),

        "dbt_test": {
            "status": task_status[
                "dbt_test"
            ],
        },
    }

    report_path = (
        f"{DBT_PROJECT_DIR}/models/metadata_generation_report.json"
    )

    with open(report_path, "w") as f:
        json.dump(
            report,
            f,
            indent=2,
            default=str
        )

    return report_path

default_args = {
    "owner": "airflow",
    "start_date": timezone.datetime(2021, 2, 9),
}

with DAG(
    dag_id=f"greenery_metadata_pipeline",
    default_args=default_args,
    schedule=None,
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
            "table_id": TABLE_ID[0],
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
            "path_to_yaml": f"{DBT_PROJECT_DIR}/models/staging/greenery/_models.yml",
            "maintain_original_test": MAINTAIN_ORIGINAL_TEST,
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
            "llm_provider": DEFAULT_LLM_PROVIDER,
        },
    )

    capture_tests_before = PythonOperator(
        task_id="capture_tests_before",
        python_callable=_capture_tests,
        op_kwargs={
            "path_to_yaml": f"{DBT_PROJECT_DIR}/models/staging/greenery/_models.yml",
        },
    )

    write_schema = PythonOperator(
        task_id="write_schema",
        python_callable=_write_schema,
        op_kwargs={
            # IMPORTANT: need to declare how to write the result to ensure, no schema duplicate
            "path_to_save_yaml": f"{DBT_PROJECT_DIR}/models/staging/greenery/_models.yml",
            # "path_to_save_yaml": f"{DBT_PROJECT_DIR}/models/staging/greenery/_{TABLE_ID}_schema.yml",
        },
    )

    capture_tests_after = PythonOperator(
        task_id="capture_tests_after",
        python_callable=_capture_tests,
        op_kwargs={
            # IMPORTANT: need to declare how to write the result to ensure, no schema duplicate
            "path_to_yaml": f"{DBT_PROJECT_DIR}/models/staging/greenery/_models.yml",
            # "path_to_yaml": f"{DBT_PROJECT_DIR}/models/staging/greenery/_{TABLE_ID}_schema.yml",
        },
    )

    dbt_test_verification = PythonOperator(
        task_id="dbt_test_verification",
        python_callable=_dbt_test_verification,
        op_kwargs={
            "maintain_original_test": MAINTAIN_ORIGINAL_TEST,
        },
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