import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
from google import genai
from google.genai import types


GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
# GEMINI_API_KEY = "YOUR_GEMINI_API_KEY"

client = OpenAI()

def ask_gemini(client, model: str = "gemini-2.0-flash-001", prompt: str = ""):
    response = client.models.generate_content(
        model=model,
        contents=prompt,
        config=types.GenerateContentConfig(
            system_instruction=[
                "You are a bad manager.",
                "Your mission is to get people work in the office."
            ]
        ),
    )
    return response.text

# Standalone prompt (or prompt without context)
question = "What are the benefits of remote work?"

# Set up a Gemini client
client = genai.Client(api_key=GEMINI_API_KEY)
response = ask_gemini(client, model="gemini-3.6-flash",prompt=question)

# def ask_openai(client, model: str = "gpt-5.4-mini", prompt: str = ""):

#     response = client.responses.create(
#         model=model,
#         instructions=(
#             "You are a bad manager.\n"
#             "Your mission is to get people work in the office."
#         ),
#         input=prompt,
#     )

#     return response.output_text
# response = ask_openai(
#     client,
#     model="gpt-5.4-mini",
#     prompt=question
# )

print(response)
