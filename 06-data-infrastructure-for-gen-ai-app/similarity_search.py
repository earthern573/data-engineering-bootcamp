import json
import os

from google import genai
from google.cloud import bigquery
from google.oauth2 import service_account

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()


GCP_PROJECT_ID = "project-d069ecb2-d645-45e0-a1b"
DATASET_ID = "deb_bootcamp"
TABLE_ID = "my_embeddings"
KEYFILE = "/workspaces/data-engineering-bootcamp/00-bootcamp-project/deb-dbt-bigquery.json"
# api_key = os.environ.get("GEMINI_API_KEY")
# GEMINI_API_KEY = "YOUR_GEMINI_API_KEY"


# def get_embedding(client, model: str = "gemini-embedding-exp-03-07", text: str = ""):
#     result = client.models.embed_content(
#         model=model,
#         contents=text,
#     )
#     return result.embeddings[0]

def get_embedding(client, model: str = "text-embedding-3-small", text: str = ""):
    result = client.embeddings.create(
        model=model,
        input=text,
    )
    return result.data[0].embedding


service_account_info = json.load(open(KEYFILE))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
bigquery_client = bigquery.Client(
    project=GCP_PROJECT_ID,
    credentials=credentials,
)

# Set up a Gemini client
# genai_client = genai.Client(api_key=GEMINI_API_KEY)
genai_client = OpenAI()
vec = get_embedding(
    genai_client,
    model="text-embedding-3-small",
    # text="QR codes systems for COVID-19.\nSimple tools for bars, restaurants, offices, and other small proximity businesses."
    # text="I went to a doctor and he said I need to take a break from work and rest for a while."
    # text="Today is Sunday."
    text="""
    QR codes systems for COVID-19.\nSimple tools for bars, restaurants, offices, and other small proximity businesses.\nTurning experience into better medicine.
    \nIodine is creating a massive community of people sharing their experience with what works - and what doesn't - in medicine.
    \nWe believe Iodine is transforming the consumer experience around health, by providing personal, clear, actionable,...
    \nQR code, beacon, and other mobile transactions
    \nWe have created web and mobile tools which enable both companies and consumers to benefit from mobile transaction technologies 
    (QR codes, beacon, and more). These benefits include mobile commerce, social media, lead generation, analytics, networking, and more
    """
)

query = f"""
    SELECT
        base.text,
        distance
    FROM
    VECTOR_SEARCH(
        TABLE `{DATASET_ID}.{TABLE_ID}`,
        'embedding',
        (select {vec} as embedding),
        top_k => 3,
        distance_type => 'EUCLIDEAN'
    )
"""

# Run the query
query_job = bigquery_client.query(query)

# Get the results
results = query_job.result()

# Print the results
for row in results:
    print(row.text)
    print(row.distance)
