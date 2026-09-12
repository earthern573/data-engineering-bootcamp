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
MAINTAIN_ORIGINAL_SCHEMA = True
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
    "OPENAI": "gpt-5.4-mini",
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
    maintain_original_schema,
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
        You are a data engineering assistant responsible for generating a dbt
        schema YAML file and providing data quality test recommendations.

        You are given:

        1. The extracted database schema:
        {data_from_information_schema}

        2. Masked sample data:
        {data_from_data_masking}

        3. Existing dbt schema YAML and tests:
        {expected_sample_model_schema}

        4. Maintain original dbt schema:
        {maintain_original_schema}

        5. Maintain original dbt tests:
        {maintain_original_test}


        IMPORTANT CONTEXT:

        The sample data has been masked to protect sensitive information.

        Therefore, all reasoning must be based only on:

        - database schema
        - table/model names
        - column names
        - data types
        - column descriptions
        - column metadata
        - relationships that can be reasonably inferred from the schema
        - masked sample data
        - existing dbt schema and tests

        Do not attempt to reconstruct, guess, or expose the original values of
        masked sensitive data.

        Do not treat the provided YAML structure or existing tests as proof that
        additional tests are appropriate.


        INDEPENDENT REASONING REQUIREMENT:

        You must independently analyze the provided schema and sample data.

        Do NOT simply copy tests from the provided YAML.

        Do NOT assume that every test in the provided YAML should be added
        to other columns or models.

        Do NOT add a test merely because it appears in the existing YAML.

        For every new test, determine whether there is sufficient evidence
        from the provided information to justify it.

        Prefer fewer well-justified tests over many speculative tests.


        GOLDEN RULE — EXISTING NAMES ARE IMMUTABLE:

        Existing model names and column names are part of the schema contract.

        This rule applies regardless of the value of `maintain_original_schema`.

        You MUST preserve exactly:

        - Every existing model name.
        - Every existing column name.
        - Existing capitalization.
        - Existing prefixes and suffixes.
        - Existing underscores.
        - Existing naming conventions.
        - Existing names such as `*_GUID`.

        You MUST NOT:

        - Rename an existing model.
        - Rename an existing column.
        - Remove an existing model.
        - Remove an existing column.
        - Change capitalization.
        - Change prefixes or suffixes.
        - Normalize or reformat existing names.
        - Replace an existing name with a semantically equivalent name.
        - Change a `*_GUID` column to a `*_ID`.
        - Change a `*_GUID` column to `*_GUID` with different capitalization.
        - Convert an existing naming convention to another naming convention.

        For example, if the existing column is:

            USER_GUID

        the generated YAML MUST contain exactly:

            USER_GUID

        It MUST NOT become:

            USER_ID
            user_guid
            user_id
            User_Guid

        The exact original name is mandatory.

        This is a non-negotiable rule and takes precedence over schema
        interpretation, naming conventions, LLM recommendations, and schema
        enhancements.

        Changing an existing model or column name can break downstream dbt
        models, tests, unit tests, SQL references, relationships, and other
        dependencies.


        DESCRIPTION RULES — DESCRIPTIONS ARE ALWAYS EDITABLE:

        Descriptions are NOT part of the immutable structural schema.

        Existing descriptions may always be:

        - added
        - updated
        - improved
        - corrected
        - rewritten
        - removed when appropriate

        This rule applies regardless of the value of `maintain_original_schema`.

        Changing a description MUST NOT be considered a schema-preservation
        violation.

        If an existing column does not have a description, you SHOULD infer
        an appropriate description when sufficient evidence is available.

        When creating or improving a description, use ALL available evidence,
        including:

        - table/model name
        - column name
        - data type
        - existing description, if available
        - column metadata
        - relationships between columns or models
        - masked sample data
        - existing dbt schema context
        - other relevant information provided in the input

        Do NOT rely only on the column name when other useful evidence is
        available.

        For example, if the available information contains:

            model: users
            column: USER_GUID
            data_type: STRING

        you may infer a description such as:

            "Unique identifier associated with a user."

        However, do NOT claim that a column is a primary key, guaranteed
        unique, globally unique, or has another specific business meaning
        unless the available evidence supports that conclusion.

        If an existing description is present, you MAY improve or correct it
        when additional reliable evidence supports a more accurate description.

        If a description is missing, infer the most accurate useful description
        possible from ALL available evidence.

        Do NOT leave a description empty merely because the original YAML did
        not contain one when sufficient evidence exists.

        If there is insufficient evidence to determine a meaningful
        description, do NOT invent unsupported business meaning.

        For example, if an existing column is:

            USER_GUID

        and its existing description is:

            "User identifier"

        it is allowed to improve the description to:

            "Unique identifier associated with a user."

        However, the column name MUST remain exactly:

            USER_GUID

        Description changes must NEVER modify the model name or column name.


        SCHEMA REQUIREMENTS:

        The provided YAML is the existing schema source of truth.

        Existing models and columns must always be preserved.

        IMPORTANT:

        There is a strict distinction between structural schema and descriptions.

        Structural schema includes:

        - model names
        - column names
        - model existence
        - column existence

        Structural schema is immutable.

        Descriptions are always editable regardless of the
        `maintain_original_schema` flag.


        If `maintain_original_schema` is True:

        - Preserve every existing model from the provided YAML.
        - Preserve every existing column from the provided YAML.
        - Preserve all existing model and column names exactly.
        - Do NOT remove any existing model.
        - Do NOT remove any existing column.
        - Do NOT rename any existing model.
        - Do NOT rename any existing column.
        - Do NOT add new models.
        - Do NOT add new columns.
        - Preserve existing schema structure.
        - Descriptions MAY be added, updated, improved, corrected, or removed
          when justified by ALL available evidence.
        - If a description is missing, infer an appropriate description when
          sufficient evidence exists.
        - Description changes are ALWAYS allowed.
        - Description changes are NOT considered structural schema changes.
        - Do NOT add schema enhancements such as policy tags or new
          classifications.
        - Do NOT invent models.
        - Do NOT invent columns.
        - Do NOT invent data values.


        If `maintain_original_schema` is False:

        - Preserve every existing model.
        - Preserve every existing column.
        - Preserve every existing model name exactly.
        - Preserve every existing column name exactly.
        - Do NOT remove existing models or columns.
        - Do NOT rename existing models or columns.
        - Descriptions MAY be added, updated, improved, corrected, or removed
          when justified by ALL available evidence.
        - If a description is missing, infer an appropriate description when
          sufficient evidence exists.
        - You MAY enhance the existing schema when justified by the available
          information.
        - You MAY add appropriate schema metadata such as:
            - policy tags
            - PII classifications
            - sensitive-data classifications
            - column descriptions
            - other supported dbt schema metadata
        - Any schema enhancement must be based on available evidence.
        - Do NOT invent metadata without sufficient evidence.
        - Schema enhancement must NEVER modify an existing model or column name.

        IMPORTANT:

        `maintain_original_schema=False` does NOT give permission to rename
        or remove existing models or columns.

        Existing model names and column names remain immutable regardless of
        this flag.

        The maintain flag controls schema enrichment, NOT structural
        name preservation.


        In all cases:

        - Include a `name` for every model.
        - Include a `name` for every column.
        - Preserve the exact original model and column names.
        - Descriptions may be changed regardless of the maintain flag.
        - If a description is missing, infer one using ALL available evidence
          when sufficient evidence exists.
        - Do NOT leave a description empty merely because the original YAML
          did not contain one when sufficient evidence exists.
        - Do NOT invent unsupported business meaning.
        - Preserve the dbt `version: 2` structure.
        - Each model must be under `models:`.
        - Each column must be under its corresponding model's `columns:`.


        EXISTING TEST REQUIREMENTS:

        The provided YAML contains existing dbt tests.

        Existing tests are considered authoritative.

        If `maintain_original_test` is True:

        - Preserve every existing test exactly.
        - Do NOT remove existing tests.
        - Do NOT modify existing tests.
        - Do NOT add new tests to the YAML.
        - You MAY recommend additional tests separately.
        - Recommended tests must NOT be added to the YAML.

        If `maintain_original_test` is False:

        - Preserve every existing test.
        - Do NOT remove existing tests.
        - Do NOT modify existing tests unnecessarily.
        - You MAY add new tests when they are independently justified by the
          provided schema, metadata, or masked sample data.
        - New tests must be additional tests, not replacements for existing
          tests.
        - You MAY recommend additional tests that are not added to the YAML.


        TEST REASONING:

        Consider appropriate tests such as:

        - not_null
        - unique
        - accepted_values
        - relationships
        - dbt-expectations
        - other appropriate dbt tests
        - singular SQL tests for more complex business rules

        However, do not automatically add these tests.

        For example:

        - A column named `id` does not automatically prove that it is unique.
        - A column named `status` does not automatically prove which values
          are valid.
        - A column ending in `_id` does not automatically prove a relationship.
        - A column ending in `_GUID` does not automatically prove that it is
          unique.
        - A numeric column does not automatically require a non-negative test.
        - A timestamp column does not automatically require a specific range.
        - Sample values alone should not be treated as proof of a business rule.

        Use the available evidence to determine whether a test is justified.


        TEST RECOMMENDATIONS:

        Provide recommendations when there are reasonable opportunities to
        improve data quality testing.

        Each recommendation must explain:

        1. What should be tested.
        2. Why the test is reasonable based on the available evidence.
        3. Whether it should be implemented as a normal dbt YAML test or
           a singular SQL test.

        Recommendations are suggestions, not confirmed data-quality failures.

        Do not claim that a recommended test is definitely required unless
        the provided information clearly supports that conclusion.

        For singular SQL test recommendations:

        - Do NOT generate the SQL file.
        - Do NOT execute SQL.
        - Only describe what the test should validate.
        - `column` may be null for model-level or multi-column rules.
        - Provide a suggested `.sql` filename when appropriate.


        MASKED DATA:

        - Do NOT expose sensitive values.
        - Do NOT reconstruct masked values.
        - Do NOT attempt to infer the original value of masked fields.
        - You may reason about the existence, type, or structure of a masked
          column when that information is available.


        OUTPUT REQUIREMENTS:

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

        - Return the complete valid dbt YAML content.
        - Preserve every existing model.
        - Preserve every existing column.
        - Preserve every existing model name exactly.
        - Preserve every existing column name exactly.
        - The generated YAML must NOT be a subset of the existing schema.
        - Do NOT remove or rename existing models.
        - Do NOT remove or rename existing columns.
        - Do NOT modify existing names for any reason.
        - Descriptions MAY be added, updated, improved, corrected, or removed
          regardless of `maintain_original_schema`.
        - If a description is missing, infer one using ALL available evidence
          when sufficient evidence exists.
        - Description changes are NOT considered structural schema changes.
        - Description generation must NEVER change an existing model or column
          name.
        - Preserve all existing tests according to the
          `maintain_original_test` rules.
        - If `maintain_original_schema` is True, do not add schema metadata
          other than description changes.
        - If `maintain_original_schema` is False, schema metadata may be added
          when justified by available evidence.
        - Add new tests only when justified.
        - Do not include Markdown code fences.
        - Do not include explanatory text outside the JSON response.


        REQUIREMENTS FOR `recommendations`:

        - Return an empty list if there are no useful recommendations.
        - Do not recommend tests based on unavailable information.
        - Do not include sensitive or unmasked sample data.
        - Do not duplicate an existing test as a recommendation unless there is
          a meaningful reason to reconsider it.
        - For singular tests, `column` may be null.
        - `suggested_path` may be null for normal YAML tests.
        - Provide a suggested `.sql` filename for singular tests when useful.


        FINAL PRINCIPLE:

        The goal is not to maximize the number of tests.

        The goal is to produce a valid dbt YAML file while independently
        identifying useful, evidence-based opportunities for improving data
        quality.

        Existing model names and column names are immutable regardless of
        `maintain_original_schema`.

        Descriptions are always editable when supported by available evidence,
        regardless of `maintain_original_schema`.

        If a description is missing, use ALL available evidence to infer the
        most accurate useful description possible.

        Do not invent unsupported business meaning.

        `maintain_original_schema=True` prevents structural additions and
        schema metadata enhancements, but does NOT prevent description changes.

        `maintain_original_schema=False` allows justified schema enrichment,
        but does NOT allow renaming or removing existing models or columns.

        `maintain_original_test=True` prevents adding new tests to the YAML,
        while existing tests must remain exactly unchanged.

        `maintain_original_test=False` allows additional independently
        justified tests, while existing tests must still be preserved.

        Existing schema names and existing tests must never be removed or
        renamed.

        If the available evidence does not justify a new test or schema
        enhancement, do not add it.
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


    """
    Capture dbt models and columns from the schema YAML file.
    """

    schema_path = (
        f"{DBT_PROJECT_DIR}/models/staging/greenery/_models.yml"
    )

    if not os.path.exists(schema_path):
        raise AirflowException(
            f"Schema file not found: {schema_path}"
        )

    with open(schema_path, "r") as f:
        schema = yaml.safe_load(f)

    captured_schema = []

    for model in schema.get("models", []):
        model_name = model.get("name")

        if not model_name:
            continue

        # Capture model even if it has no columns
        if not model.get("columns"):
            captured_schema.append({
                "model": model_name,
                "column": None,
            })
            continue

        for column in model.get("columns", []):
            captured_schema.append({
                "model": model_name,
                "column": column.get("name"),
            })

    return captured_schema

def _capture_schema(path_to_yaml):
    with open(path_to_yaml, "r") as f:
        data = yaml.safe_load(f)

    schema = []

    for model in data.get("models", []):
        model_name = model.get("name")

        # Capture model even if it has no columns
        if not model.get("columns"):
            schema.append({
                "model": model_name,
                "column": None,
            })
            continue

        for column in model.get("columns", []):
            schema.append({
                "model": model_name,
                "column": column.get("name"),
            })

    return schema

def _dbt_schema_verification(
    maintain_original_schema,
    **context
):
    ti = context["ti"]

    before_schema = ti.xcom_pull(
        task_ids="capture_schema_before"
    )

    after_schema = ti.xcom_pull(
        task_ids="capture_schema_after"
    )

    if before_schema is None:
        raise AirflowException(
            "No schema captured before YAML generation."
        )

    if after_schema is None:
        raise AirflowException(
            "No schema captured after YAML generation."
        )

    # Exact model-name comparison
    before_models = {
        item["model"]
        for item in before_schema
    }

    after_models = {
        item["model"]
        for item in after_schema
    }

    removed_models = before_models - after_models
    added_models = after_models - before_models

    # Exact model + column-name comparison
    before_columns = {
        (item["model"], item["column"])
        for item in before_schema
    }

    after_columns = {
        (item["model"], item["column"])
        for item in after_schema
    }

    removed_columns = before_columns - after_columns
    added_columns = after_columns - before_columns

    # Changes are NOT allowed when maintain=True
    if maintain_original_schema:
        if removed_models:
            raise AirflowException(
                "Existing dbt models were removed while "
                "maintain_original_schema=True.\n"
                f"Removed models: {sorted(removed_models)}"
            )

        if removed_columns:
            raise AirflowException(
                "Existing dbt columns were removed while "
                "maintain_original_schema=True.\n"
                f"Removed columns: {sorted(removed_columns)}"
            )

    # maintain=False = changes are allowed,
    # but verification still reports what changed.
    return {
        "status": "PASSED",
        "maintain_original_schema": maintain_original_schema,
        "models": {
            "before": len(before_models),
            "after": len(after_models),
            "removed": sorted(removed_models),
            "added": sorted(added_models),
        },
        "columns": {
            "before": len(before_columns),
            "after": len(after_columns),
            "removed": sorted(removed_columns),
            "added": sorted(added_columns),
        },
    }

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

    if before_tests is None:
        raise AirflowException(
            "No tests captured before YAML generation."
        )

    if after_tests is None:
        raise AirflowException(
            "No tests captured after YAML generation."
        )

    before_tests = {
        (
            item["model"],
            item["column"],
            str(item["test"]),
        )
        for item in before_tests
    }

    after_tests = {
        (
            item["model"],
            item["column"],
            str(item["test"]),
        )
        for item in after_tests
    }

    removed_tests = before_tests - after_tests
    added_tests = after_tests - before_tests

    # Only enforce preservation when maintain_original_test=True
    if maintain_original_test:
        if removed_tests:
            raise AirflowException(
                "Original DBT tests were removed while "
                "maintain_original_test=True.\n"
                f"Removed tests: {sorted(removed_tests)}"
            )

        if added_tests:
            raise AirflowException(
                "New DBT tests were added while "
                "maintain_original_test=True.\n"
                f"Added tests: {sorted(added_tests)}"
            )

    # maintain=False:
    # still verify and report changes, but do not fail.
    return {
        "status": "PASSED",
        "maintain_original_test": maintain_original_test,
        "removed_tests": sorted(removed_tests),
        "added_tests": sorted(added_tests),
    }

def _generate_report(**context):

    ti = context["ti"]

    # ============================================================
    # Get XCom results
    # ============================================================

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

    schema_before = ti.xcom_pull(
        task_ids="capture_schema_before"
    )

    schema_after = ti.xcom_pull(
        task_ids="capture_schema_after"
    )

    tests_before = ti.xcom_pull(
        task_ids="capture_tests_before"
    )

    tests_after = ti.xcom_pull(
        task_ids="capture_tests_after"
    )

    schema_verification = ti.xcom_pull(
        task_ids="dbt_schema_verification"
    )

    test_verification = ti.xcom_pull(
        task_ids="dbt_test_verification"
    )

    # ============================================================
    # Get LLM recommendations
    # ============================================================

    recommendations = []

    if calling_llm_result:
        recommendations = calling_llm_result.get(
            "recommendations",
            []
        )

    # ============================================================
    # Determine task status from XCom results
    # ============================================================

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

        "capture_schema_before": (
            "SUCCESS"
            if schema_before is not None
            else "UNKNOWN"
        ),

        "write_to_yaml": (
            "SUCCESS"
            if calling_llm_result is not None
            else "UNKNOWN"
        ),

        "capture_schema_after": (
            "SUCCESS"
            if schema_after is not None
            else "UNKNOWN"
        ),

        "capture_tests_before": (
            "SUCCESS"
            if tests_before is not None
            else "UNKNOWN"
        ),

        "capture_tests_after": (
            "SUCCESS"
            if tests_after is not None
            else "UNKNOWN"
        ),

        "dbt_schema_verification": (
            schema_verification.get("status")
            if schema_verification
            else "UNKNOWN"
        ),

        "dbt_test_verification": (
            test_verification.get("status")
            if test_verification
            else "UNKNOWN"
        ),

        "dbt_test": "UNKNOWN",
    }

    # ============================================================
    # Report
    # ============================================================

    report = {

        # --------------------------------------------------------
        # Extraction
        # --------------------------------------------------------

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

        # --------------------------------------------------------
        # LLM
        # --------------------------------------------------------

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

        # --------------------------------------------------------
        # Recommendations
        # --------------------------------------------------------

        "test_recommendations": {
            "total": len(recommendations),
            "recommendations": recommendations,
            "note": (
                "Recommendations are based on the available "
                "schema and masked sample data and should be "
                "reviewed before implementation."
            ),
        },

        # --------------------------------------------------------
        # Schema capture
        # --------------------------------------------------------

        "capture_schema_before": {
            "status": task_status[
                "capture_schema_before"
            ],
            "total_models": (
                len(
                    set(
                        item["model"]
                        for item in schema_before
                    )
                )
                if schema_before
                else 0
            ),
            "total_columns": (
                len(schema_before)
                if schema_before
                else 0
            ),
        },

        "write_to_yaml": {
            "status": task_status[
                "write_to_yaml"
            ],
        },

        "capture_schema_after": {
            "status": task_status[
                "capture_schema_after"
            ],
            "total_models": (
                len(
                    set(
                        item["model"]
                        for item in schema_after
                    )
                )
                if schema_after
                else 0
            ),
            "total_columns": (
                len(schema_after)
                if schema_after
                else 0
            ),
        },

        # --------------------------------------------------------
        # Schema verification
        # --------------------------------------------------------

        "dbt_schema_verification": (
            schema_verification
            if schema_verification
            else {
                "status": "UNKNOWN",
            }
        ),

        # --------------------------------------------------------
        # Test capture
        # --------------------------------------------------------

        "capture_tests_before": {
            "status": task_status[
                "capture_tests_before"
            ],
            "total_tests": (
                len(tests_before)
                if tests_before
                else 0
            ),
            "tests": tests_before or [],
        },

        "capture_tests_after": {
            "status": task_status[
                "capture_tests_after"
            ],
            "total_tests": (
                len(tests_after)
                if tests_after
                else 0
            ),
            "tests": tests_after or [],
        },

        # --------------------------------------------------------
        # Test verification
        # --------------------------------------------------------

        "dbt_test_verification": (
            test_verification
            if test_verification
            else {
                "status": "UNKNOWN",
            }
        ),

        # --------------------------------------------------------
        # dbt test
        # --------------------------------------------------------

        "dbt_test": {
            "status": task_status[
                "dbt_test"
            ],
        },
    }

    # ============================================================
    # Write report
    # ============================================================

    report_path = (
        f"{DBT_PROJECT_DIR}/models/"
        "metadata_generation_report.json"
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
            "maintain_original_schema": MAINTAIN_ORIGINAL_SCHEMA,
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

    capture_schema_before = PythonOperator(
        task_id="capture_schema_before",
        python_callable=_capture_schema,
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

    capture_schema_after = PythonOperator(
        task_id="capture_schema_after",
        python_callable=_capture_schema,
        op_kwargs={
            "path_to_yaml": f"{DBT_PROJECT_DIR}/models/staging/greenery/_models.yml",
        },
    )

    dbt_schema_verification = PythonOperator(
        task_id="dbt_schema_verification",
        python_callable=_dbt_schema_verification,
        op_kwargs={
            "maintain_original_schema": MAINTAIN_ORIGINAL_SCHEMA,
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

    # 1. Extract source information
    (
        start
        >> [information_schema_extraction, sample_data_extraction]
        >> data_masking
    )

    # 2. Tasks that can run independently
    data_masking >> [system_prompt, llm_connection_test]

    # Capture original state BEFORE LLM/write
    start >> [capture_schema_before, capture_tests_before]

    # 3. LLM generation
    [system_prompt, llm_connection_test] >> calling_LLM

    # 4. Write generated YAML
    calling_LLM >> write_schema

    # 5. Capture state AFTER YAML update
    write_schema >> [capture_schema_after, capture_tests_after]

    # 6. Verification
    [capture_schema_before, capture_schema_after] >> dbt_schema_verification

    [capture_tests_before, capture_tests_after] >> dbt_test_verification

    # 7. Run dbt tests after verification
    [
        dbt_schema_verification,
        dbt_test_verification,
    ] >> dbt_test

    # 8. Generate final report
    [
        dbt_schema_verification,
        dbt_test_verification,
        dbt_test,
    ] >> generate_report

    generate_report >> end