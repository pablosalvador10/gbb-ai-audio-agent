import os
import time
import json
import uuid
import asyncio
from base64 import b64decode
from typing import Any, Dict, List, Optional

from aiohttp import web, WSMsgType
from azure.core.messaging import CloudEvent
from azure.cognitiveservices.speech.audio import AudioStreamFormat, PushAudioInputStream
from openai import AzureOpenAI

from src.speech.text_to_speech import SpeechSynthesizer
from src.speech.speech_to_text import SpeechCoreTranslator
from rtagents.RTMedAgent.backend.services.acs.acs_helpers import (
    broadcast_message,
    initialize_acs_caller_instance,
    send_pcm_frames,
)
from rtagents.RTMedAgent.backend.memory.conversation_state import ConversationManager
from rtagents.RTMedAgent.backend.helpers import add_space, check_for_stopwords
from rtagents.RTMedAgent.backend.settings import ACS_WEBSOCKET_PATH, TTS_END
from rtagents.RTMedAgent.backend.tools_helper import (
    function_mapping,
    push_tool_start,
    push_tool_end,
)
from rtagents.RTMedAgent.backend.tools import available_tools

# ── Initialize clients ─────────────────────────────────────────────────────
az_openai = AzureOpenAI(
    api_version="2025-02-01-preview",
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_KEY"),
)
stt = SpeechCoreTranslator()
tts = SpeechSynthesizer()
acs_caller = initialize_acs_caller_instance()
greeted_call_ids: set = set()
call_user_raw_ids: Dict[str, str] = {}


# ── Helpers ─────────────────────────────────────────────────────────────────
async def send_tts_audio(text: str) -> None:
    try:
        tts.start_speaking_text(text)
    except Exception:
        pass


async def send_response_to_acs(ws: web.WebSocketResponse, response: str) -> None:
    pcm = tts.synthesize_to_base64_frames(text=response, sample_rate=16000)
    await send_pcm_frames(ws, pcm_bytes=pcm, sample_rate=16000)


async def process_gpt_response(
    cm: ConversationManager,
    user_prompt: str,
    ws: web.WebSocketResponse,
    is_acs: bool = False,
) -> Optional[Dict[str, Any]]:
    cm.hist.append({"role": "user", "content": user_prompt})
    response = az_openai.chat.completions.create(
        stream=True,
        messages=cm.hist,
        tools=available_tools,
        tool_choice="auto",
        max_tokens=4096,
        temperature=0.5,
        top_p=1.0,
        model=os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT_ID", ""),
    )

    collected: List[str] = []
    final_collected: List[str] = []
    tool_started = False
    tool_name = tool_id = args = ""

    async for chunk in response:
        delta = chunk.choices[0].delta
        if delta.tool_calls:
            tc = delta.tool_calls[0]
            tool_id = tc.id or tool_id
            tool_name = tc.function.name or tool_name
            args += tc.function.arguments or ""
            tool_started = True
            continue
        if delta.content:
            collected.append(delta.content)
            if delta.content in TTS_END:
                text_stream = add_space("".join(collected).strip())
                if is_acs:
                    await broadcast_message(text_stream, "Assistant")
                    await send_response_to_acs(ws, text_stream)
                else:
                    await send_tts_audio(text_stream)
                    await ws.send_str(
                        json.dumps(
                            {"type": "assistant_streaming", "content": text_stream}
                        )
                    )
                final_collected.append(text_stream)
                collected.clear()

    if collected:
        pending = "".join(collected).strip()
        if is_acs:
            await broadcast_message(pending, "Assistant")
            await send_response_to_acs(ws, pending)
        else:
            await send_tts_audio(pending)
            await ws.send_str(
                json.dumps({"type": "assistant_streaming", "content": pending})
            )
        final_collected.append(pending)

    full = "".join(final_collected).strip()
    if full:
        cm.hist.append({"role": "assistant", "content": full})
        if is_acs:
            await send_response_to_acs(ws, full)
        else:
            await ws.send_str(json.dumps({"type": "assistant", "content": full}))

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
        return await handle_tool_call(tool_name, tool_id, args, cm, ws, is_acs)
    return None


async def process_tool_followup(
    cm: ConversationManager, ws: web.WebSocketResponse, is_acs: bool
):
    response = az_openai.chat.completions.create(
        stream=True,
        messages=cm.hist,
        temperature=0.5,
        top_p=1.0,
        max_tokens=4096,
        model=os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT_ID"),
    )
    collected: List[str] = []
    async for chunk in response:
        delta = chunk.choices[0].delta
        if getattr(delta, "content", None):
            collected.append(delta.content)
            if delta.content in TTS_END:
                text_stream = add_space("".join(collected).strip())
                await send_tts_audio(text_stream)
                await ws.send_str(
                    json.dumps({"type": "assistant_streaming", "content": text_stream})
                )
                collected.clear()
    if collected:
        pending = "".join(collected).strip()
        if is_acs:
            await broadcast_message(pending, "Assistant")
            await send_response_to_acs(ws, pending)
        else:
            await send_tts_audio(pending)
            await ws.send_str(
                json.dumps({"type": "assistant_streaming", "content": pending})
            )


async def handle_tool_call(
    tool_name: str,
    tool_id: str,
    function_call_arguments: str,
    cm: ConversationManager,
    ws: web.WebSocketResponse,
    is_acs: bool = False,
) -> Any:
    call_id = str(uuid.uuid4())[:8]
    params = json.loads(function_call_arguments or "{}")
    await push_tool_start(ws, call_id, tool_name, params)
    t0 = time.perf_counter()
    result_json = await function_mapping[tool_name](params)
    elapsed = (time.perf_counter() - t0) * 1000
    result = json.loads(result_json) if isinstance(result_json, str) else result_json
    cm.hist.append(
        {
            "tool_call_id": tool_id,
            "role": "tool",
            "name": tool_name,
            "content": json.dumps(result),
        }
    )
    await push_tool_end(ws, call_id, tool_name, "success", elapsed, result=result)
    await process_tool_followup(cm, ws, is_acs)
    return result


# ── Authentication & main loops ─────────────────────────────────────────────
async def authentication_conversation(ws, cm):
    greeting = "👋 Welcome! Please verify your identity."
    await ws.send_str(json.dumps({"type": "status", "message": greeting}))
    await send_tts_audio(greeting)
    cm.hist.append({"role": "assistant", "content": greeting})
    while True:
        msg = await ws.receive()
        if msg.type != WSMsgType.TEXT:
            continue
        try:
            prompt = json.loads(msg.data).get("text", msg.data)
        except:
            prompt = msg.data.strip()
        if check_for_stopwords(prompt):
            bye = "Goodbye!"
            await ws.send_str(json.dumps({"type": "exit", "message": bye}))
            await send_tts_audio(bye)
            return None
        res = await process_gpt_response(cm, prompt, ws)
        if res and res.get("authenticated"):
            return res


async def main_conversation(ws, cm):
    while True:
        msg = await ws.receive()
        if msg.type != WSMsgType.TEXT:
            continue
        try:
            prompt = json.loads(msg.data).get("text", msg.data)
        except:
            prompt = msg.data.strip()
        if check_for_stopwords(prompt):
            bye = "Thank you! Goodbye."
            await ws.send_str(json.dumps({"type": "exit", "message": bye}))
            await send_tts_audio(bye)
            return
        await process_gpt_response(cm, prompt, ws)


# ── WebSocket handlers ──────────────────────────────────────────────────────
async def realtime_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    cm = ConversationManager(auth=True)
    if await authentication_conversation(ws, cm):
        cm = ConversationManager(auth=False)
        await main_conversation(ws, cm)
    return ws


async def acs_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    call_id = request.headers.get("x-ms-call-connection-id", "UnknownCall")
    cm = ConversationManager(auth=False)
    cm.cid = call_id
    loop = asyncio.get_event_loop()
    message_queue: asyncio.Queue = asyncio.Queue()

    fmt = AudioStreamFormat(samples_per_second=16000, bits_per_sample=16, channels=1)
    push_stream = PushAudioInputStream(stream_format=fmt)
    recognizer = stt.create_realtime_recognizer(
        push_stream=push_stream,
        loop=loop,
        message_queue=message_queue,
        language="en-US",
    )
    recognizer.start_continuous_recognition_async()

    if call_id not in greeted_call_ids:
        greeting = "📞 Call connected. Hello from XMYX Healthcare!"
        await broadcast_message(greeting, "Assistant")
        await send_response_to_acs(ws, greeting)
        cm.hist.append({"role": "assistant", "content": greeting})
        greeted_call_ids.add(call_id)

    try:
        while True:
            # STT queue
            try:
                recognized = message_queue.get_nowait()
            except asyncio.QueueEmpty:
                recognized = None
            if recognized:
                await broadcast_message(recognized, "User")
                if check_for_stopwords(recognized):
                    bye = "Goodbye!"
                    await broadcast_message(bye, "Assistant")
                    await send_response_to_acs(ws, bye)
                    await acs_caller.disconnect_call(call_id)
                    break
                await process_gpt_response(cm, recognized, ws, is_acs=True)
                message_queue.task_done()

            msg = await ws.receive(timeout=5.0)
            if msg.type != WSMsgType.TEXT:
                continue
            data = json.loads(msg.data)
            kind = data.get("kind")
            if kind == "AudioData":
                part = data["audioData"]["participantRawID"]
                if call_id not in call_user_raw_ids:
                    call_user_raw_ids[call_id] = part
                if call_user_raw_ids[call_id] != part:
                    continue
                b64 = data["audioData"]["data"]
                if b64:
                    push_stream.write(b64decode(b64))
            # handle CallConnected, MediaStreamingStopped, etc. if you like
    finally:
        try:
            await asyncio.wait_for(
                asyncio.to_thread(recognizer.stop_continuous_recognition_async),
                timeout=5.0,
            )
        except:
            pass
        push_stream.close()
        await ws.close()
        call_user_raw_ids.pop(call_id, None)


# ── App setup ───────────────────────────────────────────────────────────────
app = web.Application()
app.router.add_get("/realtime", realtime_ws)
app.router.add_get(ACS_WEBSOCKET_PATH, acs_ws)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=8080)
