import asyncio
import json
import os
import sys
from pathlib import Path

import streamlit as st
from openai import AsyncOpenAI
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from dotenv import load_dotenv

load_dotenv()


MODEL = "gpt-5.4-mini"
SERVER_FILE = Path(__file__).with_name("bigquery_mcp.py")

OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]


async def ask_openai(
    prompt: str,
    history: list,
) -> tuple[str, list]:

    server = StdioServerParameters(
        command=sys.executable,
        args=[str(SERVER_FILE)],
        env=None,
    )

    client = AsyncOpenAI(api_key=OPENAI_API_KEY)

    async with stdio_client(server) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Get MCP tools
            mcp_tools = await session.list_tools()

            # Convert MCP tools into OpenAI function tools
            tools = [
                {
                    "type": "function",
                    "name": tool.name,
                    "description": tool.description or "",
                    "parameters": tool.input_schema,
                    "strict": False,
                }
                for tool in mcp_tools.tools
            ]

            # Conversation history
            input_items = [
                *history,
                {
                    "role": "user",
                    "content": prompt,
                },
            ]

            while True:

                response = await client.responses.create(
                    model=MODEL,
                    input=input_items,
                    tools=tools,
                )

                # Add OpenAI response items to conversation history
                response_items = [
                    item.model_dump(exclude_none=True)
                    for item in response.output
                ]

                input_items.extend(response_items)

                # Find tool calls
                function_calls = [
                    item
                    for item in response.output
                    if item.type == "function_call"
                ]

                # No tool call -> final answer
                if not function_calls:
                    answer = response.output_text or (
                        "I couldn't generate a response."
                    )

                    return answer, input_items

                # Execute MCP tools
                for call in function_calls:

                    result = await session.call_tool(
                        call.name,
                        call.arguments,
                    )

                    # Send MCP result back to OpenAI
                    input_items.append(
                        {
                            "type": "function_call_output",
                            "call_id": call.call_id,
                            "output": json.dumps(
                                result.model_dump(mode="json")
                            ),
                        }
                    )


st.set_page_config(
    page_title="BigQuery Chat for Network Rail Movements",
    page_icon="📊",
)

st.title("📊 BigQuery Chat for Network Rail Movements")

st.caption(
    "OpenAI can query only the MCP server's permitted table."
)


if "messages" not in st.session_state:
    st.session_state.messages = []


if "history" not in st.session_state:
    st.session_state.history = []


# Display previous messages
for message in st.session_state.messages:

    with st.chat_message(message["role"]):
        st.markdown(message["content"])


# New user message
if prompt := st.chat_input(
    "Ask about network rail movements "
    "(e.g., 'Show me the last 5 movements')"
):

    st.session_state.messages.append(
        {
            "role": "user",
            "content": prompt,
        }
    )

    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):

        with st.spinner("Querying BigQuery..."):

            try:

                answer, updated_history = asyncio.run(
                    ask_openai(
                        prompt,
                        st.session_state.history,
                    )
                )

                st.session_state.history = updated_history

                st.markdown(answer)

                st.session_state.messages.append(
                    {
                        "role": "assistant",
                        "content": answer,
                    }
                )

            except BaseException as error:

                st.exception(error)