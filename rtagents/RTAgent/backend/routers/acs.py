"""
routers/acs.py
==============
Outbound phone-call flow via Azure Communication Services.

• POST  /call             – start a phone call
• POST  /call/callbacks   – receive ACS events
• WS    /call/stream      – bidirectional PCM audio stream
"""

from __future__ import annotations

import asyncio
import json
import time
from base64 import b64decode
from typing import Dict, Optional
import contextlib
import redis.exceptions

from azure.core.exceptions import HttpResponseError
from azure.core.messaging import CloudEvent
from azure.cognitiveservices.speech.audio import AudioStreamFormat, PushAudioInputStream, PullAudioInputStream
from rtagents.RTAgent.backend.services.acs.acs_helpers import stop_audio
from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.websockets import WebSocketState
from pydantic import BaseModel

from src.speech.sttv2 import AzureSpeechToTextV2

from rtagents.RTAgent.backend.orchestration.conversation_state import (
    ConversationManager,
)
from src.aoai.manager_transcribe import AudioTranscriber
from helpers import check_for_stopwords
from rtagents.RTAgent.backend.latency.latency_tool import LatencyTool
from rtagents.RTAgent.backend.orchestration.orchestrator import route_turn
from shared_ws import (
    broadcast_message,
    send_response_to_acs,
)
from rtagents.RTAgent.backend.postcall.push import build_and_flush
from rtagents.RTAgent.backend.settings import (
    ACS_CALL_PATH,
    ACS_RECORDING_CALLBACK_PATH,
    ACS_CALLBACK_PATH,
    ACS_WEBSOCKET_PATH,
    AZURE_SPEECH_REGION,
)
from rtagents.RTAgent.backend.services.acs.events import RecordingConfig

from rtagents.RTAgent.backend.routers.handlers.interruptible_pull_stream import InterruptiblePullStream

from utils.ml_logging import get_logger

logger = get_logger("routers.acs")
router = APIRouter()


# --------------------------------------------------------------------------- #
#  1. Call initiation  (POST /call)
# --------------------------------------------------------------------------- #
class CallRequest(BaseModel):
    target_number: str


@router.post(ACS_CALL_PATH)
async def initiate_call(call: CallRequest, request: Request):
    acs = request.app.state.call_service
    if not acs:
        raise HTTPException(503, "ACS Caller not initialised")

    try:
        result = await acs.initiate_call(call.target_number)
        if result.get("status") != "created":
            return JSONResponse({"status": "failed"}, status_code=400)

        call_id = result["call_id"]
        logger.info("Call initiated – ID=%s", call_id)
        return {"message": "Call initiated", "callId": call_id}
    except (HttpResponseError, RuntimeError) as exc:
        logger.error("ACS error: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc)) from exc


# --------------------------------------------------------------------------- #
#  2. Callback events  (POST /call/callbacks)
# --------------------------------------------------------------------------- #
from .handlers.acs_call_handler import (
    handle_call_connected,
    handle_participants_updated,
    handle_call_disconnected,
    handle_media_streaming_failed,  # New handler
    handle_media_streaming_started, # New handler
    handle_media_streaming_stopped, # New handler
)

# Initialize ACS Event Manager with recording configuration
recording_config = RecordingConfig(
    auto_start_on_connect=True,
    auto_stop_on_disconnect=True,
    recording_format="wav",
    recording_channel="unmixed",
    storage_container="call-recordings",
    enable_transcription=False,
)

@router.post(ACS_RECORDING_CALLBACK_PATH)
async def recording_callbacks(request: Request):
    if not request.app.state.call_service:
        return JSONResponse({"error": "ACS not initialised"}, status_code=503)

    try:
        events = await request.json()
        for raw in events:
            event = CloudEvent.from_dict(raw)
            etype = event.type
            cid = event.data.get("callConnectionId")
            if etype == "Microsoft.Communication.RecordingStateChanged":
                state = event.data.get("state")
                if state == "Completed":
                    logger.info("Recording completed for call %s", cid)
                elif state == "Failed":
                    logger.error("Recording failed for call %s", cid)
                else:
                    logger.info("Recording state changed: %s for call %s", state, cid)
        return {"status": "recording callback received"}
    except Exception as exc:    # pylint: disable=broad-except
        logger.error("Recording callback error: %s", exc, exc_info=True)
        return JSONResponse({"error": str(exc)}, status_code=500)

@router.post(ACS_CALLBACK_PATH)
async def callbacks(request: Request):
    if not request.app.state.call_service:
        return JSONResponse({"error": "ACS not initialised"}, status_code=503)

    try:
        events = await request.json()
        for raw in events:
            event = CloudEvent.from_dict(raw)
            etype = event.type
            cid = event.data.get("callConnectionId")
            correlation_id = event.data.get("correlationId", cid)

            cm = await ConversationManager.from_redis(cid, request.app.state.redis)
            handler_mapping = {
                "Microsoft.Communication.CallConnected": lambda: handle_call_connected(
                    event, 
                    recording_config, 
                    cm,
                    request.app.state.call_service
                ),
                "Microsoft.Communication.ParticipantsUpdated": lambda: handle_participants_updated(
                    event, 
                    cm
                ),
                "Microsoft.Communication.CallDisconnected": lambda: handle_call_disconnected(
                    event, 
                    call_id=cid, 
                    conversation_manager=cm,
                    call_service=request.app.state.call_service
                ),
                "Microsoft.Communication.MediaStreamingFailed": lambda: handle_media_streaming_failed(
                    event,
                    call_id=cid,
                    conversation_manager=cm,
                ),
                "Microsoft.Communication.MediaStreamingStarted": lambda: handle_media_streaming_started(
                    event,
                    call_id=cid,
                    conversation_manager=cm,
                ),
                "Microsoft.Communication.MediaStreamingStopped": lambda: handle_media_streaming_stopped(
                    event,
                    call_id=cid,
                    conversation_manager=cm,
                    # call_service=request.app.state.call_service
                ),
            }

            handler = handler_mapping.get(etype)
            if handler:
                try:
                    await handler()
                except Exception as e:
                    logger.error(f"Error handling event {etype} for call {cid}: {e}", exc_info=True)
                    # Continue processing other events even if one fails
            else:
                logger.warning(f"Unhandled event type: {etype} for call {cid}")

            emoji = {
                "Microsoft.Communication.ParticipantsUpdated": "👥",
                "Microsoft.Communication.CallConnected": "📞",
                "Microsoft.Communication.CallDisconnected": "❌",
                "Microsoft.Communication.MediaStreamingStarted": "🎙️",
                "Microsoft.Communication.MediaStreamingStopped": "🛑",
            }.get(etype, "ℹ️")

            # await broadcast_message(request.app.state.clients, f"{emoji} {etype}")
            logger.info("%s %s %s",emoji, etype, cid)
        return {"status": "callback received"}
    except Exception as exc:  # pylint: disable=broad-except
        logger.error("Callback error: %s", exc, exc_info=True)
        return JSONResponse({"error": str(exc)}, status_code=500)


# --------------------------------------------------------------------------- #
#  3. Media-streaming WebSocket  (WS /call/stream)
# --------------------------------------------------------------------------- #
call_user_raw_ids: Dict[str, str] = {}

@router.websocket(ACS_WEBSOCKET_PATH)
async def acs_media_ws(ws: WebSocket):
    await ws.accept()
    cid = ws.headers.get("x-ms-call-connection-id", "UnknownCall")
    correlation_id = ws.headers.get("x-ms-call-correlation-id", cid)
    logger.info("▶ media WS connected – %s", cid)    # Access shared resources from app state
    acs = ws.app.state.call_service
    redis_mgr = ws.app.state.redis
    clients = ws.app.state.clients
    greeted = ws.app.state.greeted_call_ids
    tts_client = ws.app.state.tts_client
    stt_client = ws.app.state.stt_client    # Redis-backed conversation state with fallback
    try:
        cm = await ConversationManager.from_redis(cid, redis_mgr)
        media_session = await cm.get_media_stream_state()
        user_raw_id = media_session.get("user_raw_id")
        logger.info(f"✅ Loaded conversation state from Redis for session {cid}")
    except redis.exceptions.AuthenticationError as auth_error:
        logger.error(f"❌ Redis authentication failed: {auth_error}")
        # Create a new conversation manager without Redis dependency
        cm = ConversationManager(session_id=cid, redis_mgr=None)
        user_raw_id = None
        logger.warning(f"⚠️ Created new conversation state (no Redis) for session {cid}")
    except redis.exceptions.ConnectionError as conn_error:
        logger.error(f"❌ Redis connection failed: {conn_error}")
        # Create a new conversation manager without Redis dependency
        cm = ConversationManager(session_id=cid, redis_mgr=None)
        user_raw_id = None
        logger.warning(f"⚠️ Created new conversation state (no Redis) for session {cid}")
    except Exception as e:
        logger.error(f"❌ Unexpected Redis error: {e}")
        # Create a new conversation manager without Redis dependency
        cm = ConversationManager(session_id=cid, redis_mgr=None)
        user_raw_id = None
        logger.warning(f"⚠️ Created new conversation state (fallback) for session {cid}")

    # Use pull stream for STT
    pull_stream = InterruptiblePullStream()
    audio_config = pull_stream.create_audio_config()
    recognizer = stt_client.create_realtime_recognizer(
        audio_config=audio_config,
        correlation_id=correlation_id,
    )

    transcript_queue: asyncio.Queue[str] = asyncio.Queue()

    # Use continuous recognition for real-time streaming audio
    def recognized_handler(evt):
        if evt.result.text:
            asyncio.create_task(transcript_queue.put(evt.result.text))

    recognizer.recognized.connect(recognized_handler)
    recognizer.start_continuous_recognition_async()

    # if cid not in greeted:
    #     greet = "Hello from XMYX Healthcare Company! How may I address you?"
    #     await broadcast_message(clients, greet, "Assistant")
    #     await send_response_to_acs(ws, greet)
    #     await cm.append_to_history("assistant", greet)
    #     greeted.add(cid)
    if cid not in greeted:
        if not await cm.is_call_greeted():
            greet = "Hello from XMYX Healthcare Company! How may I address you?"
            await broadcast_message(clients, greet, "Assistant")
            await send_response_to_acs(ws, greet)
            await cm.append_to_history("assistant", greet)
            await cm.mark_call_greeted()

    user_raw_id = None

    try:
        try:
            while True:
                # Process transcripts
                try:
                    while True:
                        phrase = transcript_queue.get_nowait()
                        tts_client.stop_speaking()
                        for t in getattr(ws.app.state, "tts_tasks", []):
                            t.cancel()

                        await broadcast_message(clients, phrase, "User")
                        if check_for_stopwords(phrase):
                            await broadcast_message(clients, "Goodbye!", "Assistant")
                            await send_response_to_acs(ws, "Goodbye!", blocking=True)
                            await acs.disconnect_call(cid)
                            break

                        await route_turn(cm, phrase, ws, is_acs=True)
                except asyncio.QueueEmpty:
                    pass

                # Listen for audio from ACS
                try:
                    raw = await asyncio.wait_for(ws.receive_text(), timeout=5.0)
                    data = json.loads(raw)
                except (asyncio.TimeoutError, WebSocketDisconnect, json.JSONDecodeError):
                    if ws.client_state != WebSocketState.CONNECTED:
                        break
                    continue

                kind = data.get("kind")

                # When participant is identified:
                if kind == "CallConnected":
                    user_raw_id = data["callConnected"]["participant"]["rawID"]
                    await cm.update_user_raw_id(user_raw_id)
                if kind == "AudioData":
                    participant_id = data["audioData"]["participantRawID"]
                    if not user_raw_id:
                        user_raw_id = participant_id
                        await cm.update_user_raw_id(user_raw_id)                    
                    if participant_id != user_raw_id:
                        continue
                    pull_stream.write_audio(b64decode(data["audioData"]["data"]))

                elif kind == "CallConnected":
                    user_raw_id = data["callConnected"]["participant"]["rawID"]

        finally:
            recognizer.stop_continuous_recognition_async()
            if ws.client_state != WebSocketState.DISCONNECTED:
                await ws.close()
            
            # Safely persist to Redis with error handling
            try:
                await cm.persist_to_redis(redis_mgr)
                logger.info("✅ Persisted conversation state to Redis for session %s", cid)
            except redis.exceptions.AuthenticationError as auth_error:
                logger.error("❌ Redis auth failed during persist: %s", auth_error)
            except redis.exceptions.ConnectionError as conn_error:
                logger.error("❌ Redis connection failed during persist: %s", conn_error)
            except Exception as e:
                logger.error("❌ Failed to persist to Redis: %s", e)
            
            logger.info("◀ media WS closed – %s", cid)
    except Exception as exc:
        logger.error("Error in acs_media_ws: %s", exc, exc_info=True)
# @router.websocket(ACS_WEBSOCKET_PATH)
# async def acs_media_ws(ws: WebSocket):
#     acs = ws.app.state.call_service
#     if not acs:
#         await ws.close(code=1011)
#         return

#     await ws.accept()
#     cid = ws.headers.get("x-ms-call-connection-id", "UnknownCall")
#     logger.info("▶ media WS connected – %s", cid)

#     # ── per-call objects ────────────────────────────────────────────────────
#     redis_mgr = ws.app.state.redis
#     cm = ConversationManager.from_redis(cid, redis_mgr)
#     ws.state.cm = cm
#     ws.state.lt = LatencyTool(cm)

#     # greeting (once per call)
#     if cid not in ws.app.state.greeted_call_ids:
#         greet = (
#             "Hello from XMYX Healthcare Company! Before I can assist you, "
#             "let’s verify your identity. How may I address you?"
#         )
#         await broadcast_message(ws.app.state.clients, greet, "Assistant")
#         await send_response_to_acs(ws, greet)
#         await cm.append_to_history("assistant", greet)
#         ws.app.state.greeted_call_ids.add(cid)

#     # ---------- AOAI Streaming STT ----------------------------------------
#     aoai_cfg = ws.app.state.aoai_stt_cfg
#     audio_q: asyncio.Queue[Optional[bytes]] = asyncio.Queue()

#     async def on_delta(d: str):
#         """
#         Handle partial speech detected by AOAI STT.

#         - Stop ACS playback (cancel any audio bot is sending to phone).
#         - Stop sending audio from browser to ACS.
#         - Stop any local TTS audio playback.
#         - Broadcast the partial transcription.
#         """
#         ws.app.state.tts_stop_flag = True
#         if not d.strip():
#             return

#         try:
#             call_connection = acs.call_automation_client.get_call_connection(cid)
#             await call_connection.cancel_all_media_operations()
#             logger.info(f"[🛑] Stopped ACS playback for call {cid}.")
#         except Exception as e:
#             logger.warning(f"[!] Could not stop ACS playback: {e}")

#         try:
#             await stop_audio(ws)
#             logger.info(f"[🛑] Stopped audio from browser to ACS for call {cid}.")
#         except Exception as e:
#             logger.warning(f"[!] Could not stop browser audio: {e}")

#         # Broadcast the partial transcription to connected dashboards
#         await broadcast_message(ws.app.state.clients, d, "User")

#     lt: LatencyTool = ws.state.lt

#     async def on_transcript(t: str):
#         logger.info(f"[AOAI-STT] {t}")
#         await broadcast_message(ws.app.state.clients, t, "User")
#         lt.stop("stt", ws.app.state.redis)

#         # Stop local TTS
#         ws.app.state.tts_client.stop_speaking()
#         for task in list(getattr(ws.app.state, "tts_tasks", [])):
#             task.cancel()

#         # Main dialog routing
#         await route_turn(cm, t, ws, is_acs=True)

#     transcriber = AudioTranscriber(
#         url=aoai_cfg["url"],
#         headers=aoai_cfg["headers"],
#         rate=aoai_cfg["rate"],
#         channels=aoai_cfg["channels"],
#         format_=aoai_cfg["format_"],
#         chunk=1024,
#     )

#     transcribe_task = asyncio.create_task(
#         transcriber.transcribe(
#             audio_queue=audio_q,
#             model="gpt-4o-transcribe",
#             prompt="Respond in English. This is a medical environment.",
#             noise_reduction="near_field",
#             vad_type="server_vad",
#             vad_config=aoai_cfg["vad"],
#             on_delta=lambda d: asyncio.create_task(on_delta(d)),
#             on_transcript=lambda t: asyncio.create_task(on_transcript(t)),
#         )
#     )

#     user_raw_id: Optional[str] = call_user_raw_ids.get(cid)
#     try:
#         while True:
#             try:
#                 raw = await asyncio.wait_for(ws.receive_text(), timeout=5.0)
#                 data = json.loads(raw)
#             except asyncio.TimeoutError:
#                 if ws.client_state != WebSocketState.CONNECTED:
#                     break
#                 continue
#             except (WebSocketDisconnect, json.JSONDecodeError):
#                 break

#             kind = data.get("kind")
#             if kind == "AudioData":
#                 # dynamically learn / confirm the caller’s participantRawID
#                 if not user_raw_id and cid in call_user_raw_ids:
#                     user_raw_id = call_user_raw_ids[cid]
#                 # ignore our own TTS loop-back
#                 if user_raw_id and data["audioData"]["participantRawID"] != user_raw_id:
#                     continue
#                 lt.start("stt")
#                 await audio_q.put(b64decode(data["audioData"]["data"]))

#             elif kind == "CallConnected":
#                 pid = data["callConnected"]["participant"]["rawID"]
#                 call_user_raw_ids[cid] = pid
#                 user_raw_id = pid

#             elif kind in ("PlayCompleted", "PlayFailed", "PlayCanceled"):
#                 logger.info("%s from ACS (%s)", kind, cid)

#             # basic hang-up keywords (optional)
#             if kind == "AudioData" and check_for_stopwords(""):
#                 await broadcast_message(ws.app.state.clients, "Goodbye!", "Assistant")
#                 await send_response_to_acs(ws, "Goodbye!", blocking=True)
#                 await asyncio.sleep(1)
#                 await acs.disconnect_call(cid)
#                 break

#     finally:
#         await audio_q.put(None)  # flush / stop AOAI
#         with contextlib.suppress(Exception):
#             await transcribe_task
#         with contextlib.suppress(Exception):
#             await ws.close()
#         call_user_raw_ids.pop(cid, None)
#         cm.persist_to_redis(redis_mgr)
#         try:
#             cm = getattr(ws.state, "cm", None)
#             cosmos = getattr(ws.app.state, "cosmos", None)
#             if cm and cosmos:
#                 build_and_flush(cm, cosmos)
#         except Exception as e:
#             logger.error(f"Error persisting analytics: {e}", exc_info=True)
#         logger.info("◀ media WS closed – %s", cid)