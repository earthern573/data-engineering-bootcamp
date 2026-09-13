import os

import google.genai as genai

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()


# GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
# GEMINI_API_KEY = "YOUR_GEMINI_API_KEY"


# def get_embedding(client, model: str = "gemini-embedding-exp-03-07", text: str = ""):
#     result = client.models.embed_content(
#         model=model,
#         contents=text,
#     )
#     return result.embeddings[0]


# Set up a Gemini client
# client = genai.Client(api_key=GEMINI_API_KEY)

# # Get embeddings
# vec_q = get_embedding(client, model="gemini-embedding-2", text="Remote work allows employees to be more flexible and productive.").values
# vec_c = get_embedding(client, model="gemini-embedding-2", text="Work from home is very productive for me").values
# print(vec_q, vec_c)

# vec_q = get_embedding(client, model="gemini-embedding-2", text="Hello").values
# vec_c = get_embedding(client, model="gemini-embedding-2", text="Hey").values
# print(vec_q, vec_c)

###
# Use openai
def get_embedding(client, model: str = "text-embedding-3-small", text: str = ""):
    result = client.embeddings.create(
        model=model,
        input=text,
    )

    return result.data[0].embedding

# Set up a OpenAI client
client = OpenAI()

# Get embeddings
vec_q = get_embedding(
    client,
    model="text-embedding-3-small",
    text="Remote work allows employees to be more flexible and productive."
)

vec_c = get_embedding(
    client,
    model="text-embedding-3-small",
    text="Work from home is very productive for me"
)

print(vec_q, vec_c)


vec_q = get_embedding(
    client,
    model="text-embedding-3-small",
    text="Hello"
)

vec_c = get_embedding(
    client,
    model="text-embedding-3-small",
    text="Hey"
)

print(vec_q, vec_c)