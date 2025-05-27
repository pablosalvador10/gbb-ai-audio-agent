import os
import time
import json
import uuid
import asyncio
from base64 import b64decode
from typing import Any, Dict, List, Optional

from aiohttp import web, WSMsgType
from openai import AzureOpenAI
from azure.cognitiveservices.speech.audio import AudioStreamFormat, PushAudioInputStream

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


async def send_tts_audio(text: str) -> None:
    try:
        tts.start_speaking_text(text)
    except:
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
    stream = az_openai.chat.completions.create(
        stream=True,
        messages=cm.hist,
        tools=available_tools,
        tool_choice="auto",
        max_tokens=4096,
        temperature=0.5,
        top_p=1.0,
        model=os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT_ID", ""),
    )
    buf: List[str] = []
    final: List[str] = []
    tool_started = False
    tool_name = tool_id = args = ""
    async for chunk in stream:
        delta = chunk.choices[0].delta
        if delta.tool_calls:
            tc = delta.tool_calls[0]
            tool_started = True
            tool_id = tc.id or tool_id
            tool_name = tc.function.name or tool_name
            args += tc.function.arguments or ""
            continue
        if delta.content:
            buf.append(delta.content)
            if delta.content in TTS_END:
                txt = add_space("".join(buf).strip())
                if is_acs:
                    await broadcast_message(txt, "Assistant")
                    await send_response_to_acs(ws, txt)
                else:
                    await send_tts_audio(txt)
                    await ws.send_str(
                        json.dumps({"type": "assistant_streaming", "content": txt})
                    )
                final.append(txt)
                buf.clear()
    if buf:
        pend = "".join(buf).strip()
        if is_acs:
            await broadcast_message(pend, "Assistant")
            await send_response_to_acs(ws, pend)
        else:
            await send_tts_audio(pend)
            await ws.send_str(
                json.dumps({"type": "assistant_streaming", "content": pend})
            )
        final.append(pend)
    full = "".join(final).strip()
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
    stream = az_openai.chat.completions.create(
        stream=True,
        messages=cm.hist,
        temperature=0.5,
        top_p=1.0,
        max_tokens=4096,
        model=os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT_ID", ""),
    )
    buf: List[str] = []
    async for chunk in stream:
        delta = chunk.choices[0].delta
        if getattr(delta, "content", None):
            buf.append(delta.content)
            if delta.content in TTS_END:
                txt = add_space("".join(buf).strip())
                await send_tts_audio(txt)
                await ws.send_str(
                    json.dumps({"type": "assistant_streaming", "content": txt})
                )
                buf.clear()
    if buf:
        pend = "".join(buf).strip()
        if is_acs:
            await broadcast_message(pend, "Assistant")
            await send_response_to_acs(ws, pend)
        else:
            await send_tts_audio(pend)
            await ws.send_str(
                json.dumps({"type": "assistant_streaming", "content": pend})
            )


async def handle_tool_call(
    tool_name: str,
    tool_id: str,
    function_call_arguments: str,
    cm: ConversationManager,
    ws: web.WebSocketResponse,
    is_acs: bool = False,
) -> Any:
    cid = str(uuid.uuid4())[:8]
    params = json.loads(function_call_arguments or "{}")
    await push_tool_start(ws, cid, tool_name, params)
    t0 = time.perf_counter()
    res_json = await function_mapping[tool_name](params)
    elapsed = (time.perf_counter() - t0) * 1000
    res = json.loads(res_json) if isinstance(res_json, str) else res_json
    cm.hist.append(
        {
            "tool_call_id": tool_id,
            "role": "tool",
            "name": tool_name,
            "content": json.dumps(res),
        }
    )
    await push_tool_end(ws, cid, tool_name, "success", elapsed, result=res)
    await process_tool_followup(cm, ws, is_acs)
    return res


async def authentication_conversation(ws, cm):
    greet = "👋 Welcome! Please verify your identity."
    await ws.send_str(json.dumps({"type": "status", "message": greet}))
    await send_tts_audio(greet)
    cm.hist.append({"role": "assistant", "content": greet})
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
    call_id = request.headers.get("x-ms-call-connection-id", "Unknown")
    cm = ConversationManager(auth=False)
    cm.cid = call_id
    loop = asyncio.get_event_loop()
    q: asyncio.Queue = asyncio.Queue()
    fmt = AudioStreamFormat(samples_per_second=16000, bits_per_sample=16, channels=1)
    push = PushAudioInputStream(stream_format=fmt)
    recog = stt.create_realtime_recognizer(
        push_stream=push, loop=loop, message_queue=q, language="en-US"
    )
    recog.start_continuous_recognition_async()
    if call_id not in greeted_call_ids:
        greet = "📞 Call connected. Hello from XMYX Healthcare!"
        await broadcast_message(greet, "Assistant")
        await send_response_to_acs(ws, greet)
        cm.hist.append({"role": "assistant", "content": greet})
        greeted_call_ids.add(call_id)
    try:
        while True:
            try:
                txt = q.get_nowait()
            except asyncio.QueueEmpty:
                txt = None
            if txt:
                await broadcast_message(txt, "User")
                if check_for_stopwords(txt):
                    bye = "Goodbye!"
                    await broadcast_message(bye, "Assistant")
                    await send_response_to_acs(ws, bye)
                    await acs_caller.disconnect_call(call_id)
                    break
                await process_gpt_response(cm, txt, ws, True)
                q.task_done()
            msg = await ws.receive(timeout=5.0)
            if msg.type != WSMsgType.TEXT:
                continue
            data = json.loads(msg.data)
            if data.get("kind") == "AudioData":
                part = data["audioData"]["participantRawID"]
                if call_id not in call_user_raw_ids:
                    call_user_raw_ids[call_id] = part
                if call_user_raw_ids[call_id] != part:
                    continue
                b64 = data["audioData"]["data"]
                if b64:
                    push.write(b64decode(b64))
    finally:
        try:
            await asyncio.wait_for(
                asyncio.to_thread(recog.stop_continuous_recognition_async), timeout=5.0
            )
        except:
            pass
        push.close()
        await ws.close()
        call_user_raw_ids.pop(call_id, None)


app = web.Application()
app.router.add_get("/realtime", realtime_ws)
app.router.add_get(ACS_WEBSOCKET_PATH, acs_ws)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=8080)
