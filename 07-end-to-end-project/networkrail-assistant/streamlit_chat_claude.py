import asyncio
import json
import os
import sys
from pathlib import Path

import streamlit as st
import anthropic
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from dotenv import load_dotenv

load_dotenv()

MODEL = "claude-sonnet-5"
SERVER_FILE = Path(__file__).with_name("bigquery_mcp.py")

ANTHROPIC_API_KEY = os.environ["ANTHROPIC_API_KEY"]


async def ask_claude(
    prompt: str,
    history: list,
) -> tuple[str, list]:

    server = StdioServerParameters(
        command=sys.executable,
        args=[str(SERVER_FILE)],
        env=None,
    )

    client = anthropic.AsyncAnthropic(
        api_key=ANTHROPIC_API_KEY
    )

    async with stdio_client(server) as (read, write):
        async with ClientSession(read, write) as session:

            await session.initialize()

            # Get tools from MCP server
            mcp_tools = await session.list_tools()

            # Convert MCP tools to Claude tools
            tools = [
                {
                    "name": tool.name,
                    "description": tool.description or "",
                    "input_schema": tool.input_schema,
                }
                for tool in mcp_tools.tools
            ]

            messages = [
                *history,
                {
                    "role": "user",
                    "content": prompt,
                },
            ]

            while True:

                response = await client.messages.create(
                    model=MODEL,
                    max_tokens=4096,
                    messages=messages,
                    tools=tools,
                )

                # Add Claude response to conversation
                messages.append(
                    {
                        "role": "assistant",
                        "content": [
                            block.model_dump()
                            for block in response.content
                        ],
                    }
                )

                # Find tool calls
                tool_calls = [
                    block
                    for block in response.content
                    if block.type == "tool_use"
                ]

                # No tool call -> final answer
                if not tool_calls:

                    answer = "\n".join(
                        block.text
                        for block in response.content
                        if block.type == "text"
                    )

                    return answer, messages

                # Execute MCP tools
                tool_results = []

                for tool_call in tool_calls:

                    result = await session.call_tool(
                        tool_call.name,
                        tool_call.input,
                    )

                    tool_results.append(
                        {
                            "type": "tool_result",
                            "tool_use_id": tool_call.id,
                            "content": json.dumps(
                                result.model_dump(
                                    mode="json"
                                )
                            ),
                        }
                    )

                # Send tool results back to Claude
                messages.append(
                    {
                        "role": "user",
                        "content": tool_results,
                    }
                )


st.set_page_config(
    page_title="BigQuery Chat for Network Rail Movements",
    page_icon="📊",
)

st.title("📊 BigQuery Chat for Network Rail Movements")

st.caption(
    "Claude can query only the MCP server's permitted table."
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
                    ask_claude(
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