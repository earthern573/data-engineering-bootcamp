import json
import os

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

def ask_openai(client, model: str = "gpt-4o-mini", prompt: str = ""):

    response = client.responses.create(
        model=model,
        instructions=(
            "You are a course recommender.\n"
            "Your mission is to recommend courses for people who want to upskill "
            "and switch careers."
        ),
        input=prompt,
    )

    return response.output_text


def load_data_to_bigquery(client, df):
    schema = [
        bigquery.SchemaField("text", "STRING"),
        bigquery.SchemaField("embedding", "FLOAT64", mode="REPEATED"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition="WRITE_TRUNCATE"
    )
    table_id = f"{GCP_PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    load_job.result()

    print(f"Loaded {load_job.output_rows} rows into {table_id}")


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
        print(row.text)
        print(row.distance)

    return similar_texts


# Add more course data here
df = pd.DataFrame(data={
    "text": [
        "คอร์ส Probability for Data Science - ความน่าจะเป็นถูกนำมาใช้ในงาน Data Science ในงานด้านวิเคราะห์ข้อมูลและสร้างโมเดล เพื่อทำให้มั่นใจได้ว่าข้อมูลที่ได้มา มันมีความหมาย และมี Insight ถ้าคุณยังต้องนำข้อมูลไปทำโมเดลต่อ ไม่ว่าจะเป็นโมเดล Machine Learning หรือการทำนาย ล้วนก็มีงานใช้งานความน่าจะเป็นในการสร้างโมเดล ซึ่งคุณไม่มีทางจะหลีกเลี่ยงสิ่งนี้ได้ เพราะถ้าคุณไม่เข้าใจความน่าจะเป็นอย่างแท้จริง คุณอาจกำลังใช้โมเดล Machine Learning ที่จำลองสถานการณ์ผิดไปจากโจทย์ที่กำลังเผชิญอยู่ก็ได้! ข่าวดีคือ คุณไม่จำเป็นต้องเป็นอัจฉริยะด้านคณิตศาสตร์ก็สามารถเข้าใจความน่าจะเป็นได้!",
        "คอร์ส Data Pipelines with Airflow - คอร์สออนไลน์สำหรับ Data Engineer คอร์สแรกของไทยที่สอนการสร้าง End-to-End Data Pipelines ด้วย Airflow โดยเป็นคอร์สที่สอนการสร้าง Data Pipelines เพื่อจัดการข้อมูลขนาดใหญ่ (Big Data) แบบ Step by Step ตั้งแต่การอ่านข้อมูล ทำความสะอาด ปรับให้อยู่ในรูปแบบที่เหมาะสม สุดท้ายคือโหลดข้อมูลเข้า Data Lake/Data Warehouse แบบอัตโนมัติ เพื่อนำไปวิเคราะห์ข้อมูล และประกอบการตัดสินใจทางธุรกิจต่อไป",
        "คอร์ส Fundamentals of Data Analytics - เริ่มต้นเรียนรู้พื้นฐานการวิเคราะห์ข้อมูล (Data Analytics) ตั้งแต่การเก็บรวบรวม จัดการ วิเคราะห์ และสื่อสารข้อมูลอย่างมีประสิทธิภาพ โดยใช้เครื่องมือพื้นฐานที่ทุกคนสามารถเข้าถึงได้ เช่น Excel และ Google Sheets พร้อมสร้างความตระหนักถึงคุณค่าของข้อมูลในการตัดสินใจในชีวิตประจำวันและการทำงาน เพื่อเตรียมความพร้อมสู่โลกดิจิทัลอย่างมั่นใจ",
        "คอร์ส Service Design Essentials - เรียนรู้การออกแบบประสบการณ์ที่น่าจดจำให้กับลูกค้า และการออกแบบแผนการจัดการ Operations ทั้งหน้าบ้านและหลังบ้านให้ลื่นไหล ไร้รอยต่อ แบบเข้าใจง่าย พร้อมกับตัวอย่างจากธุรกิจจริงที่จะช่วยให้คุณเห็นภาพการนำไปใช้งานจริงมากที่สุด Service Design คือการออกแบบระบบการประสานงานระหว่าง ลูกค้า ทีมงานหน้าบ้าน และทีมงานหลังบ้าน ให้มีประสิทธิภาพมากที่สุด เพื่อสร้างประสบการณ์ที่ดีสำหรับทุกฝ่าย ซึ่งเกิดขึ้นจากแนวคิด Design Thinking โดยคำนึงถึงตั้งแต่ช่วงต้นของการทําการตลาด การใช้งานสินค้าและบริการ การรักษาความพึงพอใจของลูกค้า และการทําให้ลูกค้าบอกต่อ ไปจนถึงส่วนงานหลังบ้านทั้งการทำ Operations และ Internal Experience เรียกได้ว่าเป็นการออกแบบประสบการณ์ในทุกๆ Touchpoints ในคอร์ส Service Design Essentials นี้ เราจะพาผู้เรียนทุกคนไปเข้าใจกระบวนการคิดและวิธีการทำ Service Design ตั้งแต่ต้นจนจบกระบวนการ เพื่อให้คุณสามารถนำไปประยุกต์ใช้กับธุรกิจของคุณได้เลย",
        "คอร์ส Webflow for Designers - ปลดล็อกขีดจำกัดในการออกแบบเว็บไซต์แบบไม่ต้องเขียนโค้ดด้วย Webflow เครื่องมือออกแบบเว็บที่กำลังมาแรงในหมู่ Designer ทั่วโลก ให้คุณปรับแต่งเว็บได้อย่างอิสระ โดยที่ไม่ต้องเขียนโค้ดสักบรรทัด! คอร์สเรียนนี้จะสอนทั้งวิธีใช้ Webflow ตั้งแต่เครื่องมือพื้นฐาน ไปจนถึงการสร้าง Animation ซึ่งเป็นจุดเด่นสำคัญของ Webflow และยังสอนหลักการสร้างเว็บไซต์แบบมืออาชีพตั้งแต่การเริ่มต้นวางโครงสร้างเว็บ ให้คุณอัปสกิลการสร้างเว็บได้อย่างครบเครื่อง ต่อยอดสู่การเป็นนักออกแบบเว็บมืออาชีพได้จริง!",
        "คอร์ส Calligraphy with Procreate - Calligraphy หรืออักษรวิจิต คือศิลปะการเขียนตัวอักษรบรรจงให้ออกมาสวยงาม ผ่านลวดลาย น้ำหนัก และเทคนิคที่หลากหลาย ซึ่งเราสามารถเห็นงานประเภทนี้ได้ตาม การ์ด ป้าย โลโก้ ฯลฯ การเขียน Calligraphy เป็นศาสตร์ที่มีมายาวนานกว่า 1,000 ปี ซึ่งอยู่ในหลากหลายอารยธรรม ไม่ว่าจะเป็นฝั่งตะวันตก หรือฝั่งเอเชียอย่างจีน ซึ่งการเขียนอักษร Calligraphy นั้นก็ได้ถูกพัฒนามาเรื่อยๆ ทั้งรูปแบบ เทคนิค รวมถึงอุปกรณ์ต่างๆ ตั้งแต่ปากกาขนนก พู่กัน จนมาถึงยุคปัจจุบันที่เราสามารถเขียน Calligraphy บนแทปเล็ตอย่าง iPad ได้ด้วย การเขียน Calligraphy บน iPad ผ่านแอปอย่าง Procreate สามารถทำให้คุณเขียนอักษรออกมาได้สวยงามไม่แพ้การเขียนบนกระดาษเลย และทำให้คุณสามารถควบคุมน้ำหนักรวมถึงแก้ไขงานได้อย่างอิสระมากขึ้นอีกด้วย ในคอร์ส Calligraphy with Procreate นี้จะพาคุณไปเรียนรู้กับการฝึกเขียนตัวอักษรวิจิตตั้งแต่เส้นพื้นฐาน ผ่านเครื่องมือ Procreate บน iPad พร้อมลงมือทำ Project จริง **พิเศษ **สำหรับผู้เรียน รับ Brush และ Worksheet ไว้ใช้ฝึก ฟรี",
        "คอร์ส Tech Jumpstart for Non-Techies - ในโลกดิจิทัลทุกวันนี้ เทคโนโลยีเข้ามามีบทบาทสำคัญในหลายอุตสาหกรรม หลายอาชีพ และหลายคน ไม่ว่าจะเป็น Product Manager, Business Professional, Entrepreneurs, หรือคนอยากเริ่มต้นเข้ามาทำงานในบริษัท Tech ก็ตาม การเข้าใจพื้นฐาน Technology ที่อยู่เบื้องหลังการพัฒนา \‘Web/App\’ เป็นทักษะสำคัญที่ใครใครต่างต้องการตัวแต่สำหรับคนที่ไม่มีพื้นฐาน หรือคนที่เป็น Non-Tech Individual นั้น Technology อาจจะฟังดูซับซ้อน และใช้เวลานานในการทำความเข้าใจ ไม่ว่าจะเป็นคำศัพท์เฉพาะ หรือ Framework ที่ไม่คุ้นเยอะแยะมากมายคอร์สออนไลน์ Tech Jumpstart for non-techies จะมาช่วยคุณปูพื้นฐาน Tech ให้แข็งแกร่ง เปิดมุมมองโลกของการพัฒนา Website และ Application พร้อมต่อยอดการทำงานใน Tech Industry ได้อย่างมั่นใจ",
        "คอร์ส Intro to Product Management - ปัจจุบันหลายๆองค์กรเริ่มให้ความสำคัญกับการทำ Digital Transformation อยากที่จะเริ่มต้นทำ Digital Product เช่น Website หรือ Application ของตัวเองเพื่อช่วยให้ตอบสนองความต้องการของลูกค้าและธุรกิจไปพร้อมๆกัน รวมไปถึงยังต้องนำเทคโนโลยีเข้ามาประยุกต์ใช้เพื่อให้สามารถส่งมอบคุณค่า(Values)ให้กับลูกค้าได้อย่างรวดเร็วและตามทันคู่แข่ง ทำให้ตำแหน่งที่ชื่อว่า Product Manager เข้ามามีบทบาทสำคัญอย่างมากในยุค Digital Transformation และเป็นสายงานที่มีความต้องการสูงมากในปัจจุบัน",
        "คอร์ส Driving Business Impact with Data - การนำข้อมูลมาวิเคราะห์เพื่อสร้างกลยุทธ์ สร้างผลิตภัณฑ์ และสร้างมูลค่าทางธุรกิจไม่ใช่เรื่องใหม่ แต่ในโลกยุคดิจิทัล เรามีแหล่งข้อมูลใหม่ๆ เป็นจำนวนมาก ซึ่งส่งผลให้ปริมาณข้อมูลเพิ่มขึ้นอย่างมหาศาล องค์กรที่ต้องการจะนำข้อมูลไปใช้ให้เกิดประโยชน์ต้องมีวิธีคิด มีกระบวนการตัดสินใจ มีเครื่องมือ มีบุคลากรที่พร้อมรับมือกับความเปลี่ยนแปลงนี้ ให้ธุรกิจของคุณตัดสินใจได้อย่างชาญฉลาด เพิ่มประสิทธิภาพการทำงาน และสร้างโอกาสใหม่ๆ การวิเคราะห์ข้อมูลจึงเป็นเครื่องมืออันทรงพลังที่ช่วยให้ธุรกิจ",
        "คอร์ส AI Tools for Tourism: Customer Analytics - หยุดเดาใจลูกค้า! ใช้ AI วิเคราะห์พฤติกรรมนักท่องเที่ยวให้แม่นยำ หลักสูตรนี้จะเปลี่ยนวิธีที่คุณเข้าใจลูกค้านักท่องเที่ยว จากการ \"คาดเดา\" ไปสู่การ \"วิเคราะห์\" ที่แม่นยำด้วยพลังของ AI คอร์สนี้ออกแบบมาสำหรับผู้ประกอบการท่องเที่ยวโดยเฉพาะ คุณจะได้เรียนรู้แนวคิดพื้นฐาน วิธีการใช้เครื่องมือ AI (เช่น ChatGPT, Claude) ในการวิเคราะห์ข้อมูลลูกค้า (Customer Analytics) การวิเคราะห์ความรู้สึก (Sentiment Analysis) จากรีวิว และพฤติกรรม ไปจนถึงการทำนายและคาดการณ์แนวโน้ม (Prediction) เพื่อสร้างกลยุทธ์การตลาดและปรับปรุงบริการให้ตรงใจลูกค้ายิ่งขึ้น"
    ]
})
print(df.head())

# Set up a llm (Gemini, openai) client
# genai_client = genai.Client(api_key=GEMINI_API_KEY)
genai_client = OpenAI()

# Set up a BigQuery client
service_account_info = json.load(open(KEYFILE))
credentials = service_account.Credentials.from_service_account_info(service_account_info)
bigquery_client = bigquery.Client(
    project=GCP_PROJECT_ID,
    credentials=credentials,
)

# Create the embeddings
df["embedding"] = df.text.map(
    lambda x: get_embedding(genai_client, text=x)
)
print(df.head())

# Store embeddings in BigQuery
load_data_to_bigquery(bigquery_client, df)

# Change your quesiton here
question = "อยากเป็น manager เริ่มต้นอย่างไรจาก คอร์สไหนดี แต่ไม่มีเงินจ่ายค่าคอร์ส"

vec = get_embedding(genai_client, text=question)

similar_texts = search_similar_texts(bigquery_client, vec)

# Create context by gathering results together
context = " / ".join([each for each in similar_texts])

prompt_with_context = f"""
Context:
{context}

Question:
{question}
"""

response = ask_openai(genai_client, prompt=prompt_with_context)
print(response)
