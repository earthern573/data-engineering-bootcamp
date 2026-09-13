import json
import os

import pandas as pd
import streamlit as st
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


# def ask_gemini(client, model: str = "gemini-2.0-flash-001", prompt: str = ""):
#     response = client.models.generate_content(
#         model=model,
#         contents=prompt,
#         config=types.GenerateContentConfig(
#             system_instruction=[
#                 "You are a course recommender.",
#                 "Your mission is to recommend courses for people who want to upskill and switch careers."
#             ]
#         ),
#     )
#     return response.text

def ask_openai(client, model: str = "gpt-5.6-mini", prompt: str = ""):

    response = client.responses.create(
        model=model,
        instructions=(
            "You are a data analyst who works for the Greenery company.\n"
            "Your mission is to summarize the Greenery data and prepare "
            "the reports for the management.\n"
            "Greenery, a tech startup that delivers flowers and houseplants. "
            "You are here to grow revenue and acquire new customers!"
        ),
        input=prompt,
    )

    return response.output_text


def search_similar_texts(client, vec):
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
    query_job = client.query(query)

    # Get the results
    results = query_job.result()

    # Print the results
    similar_texts = []
    for row in results:
        similar_texts.append(row.text)

    return similar_texts


def main():
    st.title("Q&A App")

    option = st.selectbox(
        "Which model?",
        (
            "gpt-4o-mini",
            "gpt-5.4-mini",
        ),
    )
    st.write("You selected:", option)

    user_question = st.text_input("Ask a question:")
    if user_question:
        with st.spinner("Cooking up a response... 🍳", show_time=True):
            # Example
            # user_question = "อยากทำสาย Data Engineer ควรเรียนคอร์สอะไรดี?"
            df = pd.DataFrame(data={
                "text": [
                    user_question,
                ]
            })

            # Set up a Gemini client
            # genai_client = genai.Client(api_key=GEMINI_API_KEY)
            genai_client = OpenAI()

            # Set up a BigQuery client
            service_account_info = json.load(open(KEYFILE))
            credentials = service_account.Credentials.from_service_account_info(service_account_info)
            bigquery_client = bigquery.Client(
                project=GCP_PROJECT_ID,
                credentials=credentials,
            )

            vec = get_embedding(genai_client, text=user_question)
            similar_texts = search_similar_texts(bigquery_client, vec)

            # Create context by gathering results together
            context = " / ".join([each for each in similar_texts])

            prompt_with_context = f"""
            Given the context below, find the actionable insights and 
            answer the question. Explain like I'm 10.
            
            Context:
            {context}

            Question:
            {user_question}
            """
            response = ask_openai(genai_client, model=option, prompt=prompt_with_context)

        st.subheader("AI Assistant:")
        st.write(response)


if __name__ == "__main__":
    main()