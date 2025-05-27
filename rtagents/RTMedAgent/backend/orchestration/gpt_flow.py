# orchestration/gpt_flow.py
# =========================
"""
All OpenAI-streaming + tool plumbing in one place.

Public API
----------
process_gpt_response()  – stream GPT → TTS (+ tools)
"""
from __future__ import annotations

import asyncio, json, time, uuid
from typing import Any, Dict, List, Optional

from fastapi import WebSocket
from rtagents.RTMedAgent.backend.services.openai_services import (
    client as az_openai_client,
)
from utils.ml_logging import get_logger
from rtagents.RTMedAgent.backend.settings import (
    AZURE_OPENAI_CHAT_DEPLOYMENT_ID,
    TTS_END,
)
from rtagents.RTMedAgent.backend.helpers import add_space
from rtagents.RTMedAgent.backend.agents.tool_store.tools import (
    available_tools as DEFAULT_TOOLS,
)
from rtagents.RTMedAgent.backend.agents.tool_store.tools_helper import (
    function_mapping,
    push_tool_start,
    push_tool_end,
)
from rtagents.RTMedAgent.backend.shared_ws import (
    send_tts_audio,
    push_final,
    broadcast_message,
    send_response_to_acs,
)

logger = get_logger("gpt_flow")


async def process_gpt_response(
    cm,                       # ConversationManager
    user_prompt: str,
    ws: WebSocket,
    *,
    is_acs: bool = False,
    model_id: str = AZURE_OPENAI_CHAT_DEPLOYMENT_ID,
    temperature: float = 0.5,
    top_p: float = 1.0,
    max_tokens: int = 4096,
    available_tools: Optional[List[Dict[str, Any]]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Stream a chat completion, emit TTS, handle tool calls.

    All sampling / model / tool parameters are injected by RTAgent and
    forwarded untouched through any follow-up calls.
    """
    # ------------------------------------------------------------------#
    cm.hist.append({"role": "user", "content": user_prompt})

    if available_tools is None:
        available_tools = DEFAULT_TOOLS

    chat_kwargs = dict(
        stream=True,
        messages=cm.hist,
        model=model_id,
        max_tokens=max_tokens,
        temperature=temperature,
        top_p=top_p,
        tools=available_tools,
        tool_choice="auto" if available_tools else "none",
    )

    response = az_openai_client.chat.completions.create(**chat_kwargs)

    collected: List[str] = []
    final_chunks: List[str] = []
    tool_started = False
    tool_name = tool_id = args = ""

    for chunk in response:
        if not chunk.choices:
            continue
        delta = chunk.choices[0].delta

        # ---- tool-call tokens -----------------------------------------
        if delta.tool_calls:
            tc = delta.tool_calls[0]
            tool_id = tc.id or tool_id
            tool_name = tc.function.name or tool_name
            args += tc.function.arguments or ""
            tool_started = True
            continue

        # ---- normal content tokens ------------------------------------
        if delta.content:
            collected.append(delta.content)
            if delta.content in TTS_END:
                streaming = add_space("".join(collected).strip())
                await _emit_streaming_text(streaming, ws, is_acs)
                final_chunks.append(streaming)
                collected.clear()

    # ---- flush tail ---------------------------------------------------
    if collected:
        pending = "".join(collected).strip()
        await _emit_streaming_text(pending, ws, is_acs)
        final_chunks.append(pending)

    full_text = "".join(final_chunks).strip()
    if full_text:
        cm.hist.append({"role": "assistant", "content": full_text})
        await push_final(ws, "assistant", full_text, is_acs=is_acs)

    # ---- follow-up tool call -----------------------------------------
    if tool_started:
        cm.hist.append(
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": tool_id,
                        "type": "function",
                        "function": {"name": tool_name, "arguments": args},
                    }
                ],
            }
        )
        return await _handle_tool_call(
            tool_name,
            tool_id,
            args,
            cm,
            ws,
            is_acs,
            model_id,
            temperature,
            top_p,
            max_tokens,
            available_tools,
        )

    return None


# ======================================================================#
#  Helper routines                                                      #
# ======================================================================#
async def _emit_streaming_text(text: str, ws: WebSocket, is_acs: bool) -> None:
    """Send one streaming chunk (TTS + relay)."""
    if is_acs:
        await broadcast_message(ws.app.state.clients, text, "Assistant")
        await send_response_to_acs(ws, text, latency_tool=ws.state.lt)
    else:
        await send_tts_audio(text, ws, latency_tool=ws.state.lt)
        await ws.send_text(json.dumps({"type": "assistant_streaming", "content": text}))


async def _handle_tool_call(
    tool_name: str,
    tool_id: str,
    args: str,
    cm,
    ws: WebSocket,
    is_acs: bool,
    model_id: str,
    temperature: float,
    top_p: float,
    max_tokens: int,
    available_tools: List[Dict[str, Any]],
) -> dict:
    params = json.loads(args or "{}")
    fn = function_mapping.get(tool_name)
    if fn is None:
        raise ValueError(f"Unknown tool '{tool_name}'")

    call_id = uuid.uuid4().hex[:8]

    await push_tool_start(ws, call_id, tool_name, params, is_acs=is_acs)

    t0 = time.perf_counter()
    result = await fn(params)
    elapsed = (time.perf_counter() - t0) * 1000
    result = json.loads(result) if isinstance(result, str) else result

    cm.hist.append(
        {
            "tool_call_id": tool_id,
            "role": "tool",
            "name": tool_name,
            "content": json.dumps(result),
        }
    )

    await push_tool_end(
        ws, call_id, tool_name, "success", elapsed, result=result, is_acs=is_acs
    )

    if is_acs:
        await broadcast_message(ws.app.state.clients, f"🛠️ {tool_name} ✔️", "Assistant")

    await _process_tool_followup(
        cm,
        ws,
        is_acs,
        model_id,
        temperature,
        top_p,
        max_tokens,
        available_tools,
    )
    return result


async def _process_tool_followup(
    cm,
    ws: WebSocket,
    is_acs: bool,
    model_id: str,
    temperature: float,
    top_p: float,
    max_tokens: int,
    available_tools: List[Dict[str, Any]],
) -> None:
    """Ask GPT to respond *after* tool execution (no new user input)."""
    await process_gpt_response(
        cm,
        "",
        ws,
        is_acs=is_acs,
        model_id=model_id,
        temperature=temperature,
        top_p=top_p,
        max_tokens=max_tokens,
        available_tools=available_tools,
    )
