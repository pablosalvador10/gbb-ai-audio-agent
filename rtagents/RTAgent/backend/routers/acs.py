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

from azure.core.exceptions import HttpResponseError
from azure.core.messaging import CloudEvent
from rtagents.RTAgent.backend.services.acs.acs_helpers import stop_audio
from fastapi import APIRouter, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.websockets import WebSocketState
from pydantic import BaseModel

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
    ACS_CALLBACK_PATH,
    ACS_WEBSOCKET_PATH,
)
from rtagents.RTAgent.backend.services.acs.events import ACSEventManager, RecordingConfig
from utils.ml_logging import get_logger

logger = get_logger("routers.acs")
router = APIRouter()

# Initialize ACS Event Manager with recording configuration
recording_config = RecordingConfig(
    auto_start_on_connect=True,
    auto_stop_on_disconnect=True,
    recording_format="wav",
    recording_channel="unmixed",
    storage_container="call-recordings",
    enable_transcription=False,
)
acs_event_manager = ACSEventManager(recording_config)


# --------------------------------------------------------------------------- #
#  1. Call initiation  (POST /call)
# --------------------------------------------------------------------------- #
class CallRequest(BaseModel):
    target_number: str


@router.post(ACS_CALL_PATH)
async def initiate_call(call: CallRequest, request: Request):
    acs = request.app.state.acs_caller
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
@router.post(ACS_CALLBACK_PATH)
async def callbacks(request: Request):
    """
    Enhanced ACS callback handler with comprehensive event management.
    
    Features:
    - Automatic recording start/stop based on call events
    - Session tracking and state management
    - Comprehensive error handling and logging
    - Integration with existing conversation management
    """
    if not request.app.state.acs_caller:
        return JSONResponse({"error": "ACS not initialised"}, status_code=503)

    try:
        events = await request.json()
        
        # Process events through the centralized event manager
        result = await acs_event_manager.process_callback_events(request, events)
        
        # Log processing results
        logger.info(
            "ACS callback events processed",
            extra={
                "correlation_id": result.get("correlation_id"),
                "processed_count": len(result.get("processed_events", [])),
                "error_count": len(result.get("errors", [])),
                "status": result.get("status"),
            }
        )
        
        # Handle any processing errors
        if result.get("errors"):
            logger.warning(
                "Some events had processing errors",
                extra={
                    "errors": result["errors"],
                    "correlation_id": result.get("correlation_id"),
                }
            )
            
        # Return structured response
        return {
            "status": "callback processed",
            "correlation_id": result.get("correlation_id"),
            "processed_events": len(result.get("processed_events", [])),
            "errors": result.get("errors", []),
        }
        
    except Exception as exc:
        logger.error("Callback processing error: %s", exc, exc_info=True)
        return JSONResponse({"error": str(exc)}, status_code=500)


# --------------------------------------------------------------------------- #
#  3. Media-streaming WebSocket  (WS /call/stream)
# --------------------------------------------------------------------------- #
call_user_raw_ids: Dict[str, str] = {}

# @router.websocket(ACS_WEBSOCKET_PATH)
# async def acs_media_ws(ws: WebSocket):
#     speech = ws.app.state.stt_client
#     acs = ws.app.state.acs_caller
#     if not speech or not acs:
#         await ws.close(code=1011)
#         return

#     await ws.accept()
#     cid = ws.headers.get("x-ms-call-connection-id", "UnknownCall")
#     logger.info("▶ media WS connected – %s", cid)

#     # ----------------------------------------------------------------------- #
#     #  Local objects
#     # ----------------------------------------------------------------------- #
#     queue: asyncio.Queue[str] = asyncio.Queue()
#     push_stream = PushAudioInputStream(
#         stream_format=AudioStreamFormat(samples_per_second=16000, bits_per_sample=16, channels=1)
#     )
#     recogniser = speech.create_realtime_recognizer(
#         push_stream=push_stream,
#         loop=asyncio.get_event_loop(),
#         message_queue=queue,
#         language="en-US",
#         vad_silence_timeout_ms=500,
#     )
#     recogniser.start_continuous_recognition_async()

#     redis_mgr = ws.app.state.redis
#     cm = ConversationManager.from_redis(cid, redis_mgr)

#     clients = ws.app.state.clients
#     greeted: set[str] = ws.app.state.greeted_call_ids
#     if cid not in greeted:
#         greet = (
#             "Hello from XMYX Healthcare Company! Before I can assist you, "
#             "let’s verify your identity. How may I address you?"
#         )
#         await broadcast_message(clients, greet, "Assistant")
#         await send_response_to_acs(ws, greet)
#         cm.append_to_history("assistant", greet)
#         greeted.add(cid)

#     user_raw_id = call_user_raw_ids.get(cid)

#     try:
#         # --- inside acs_media_ws ---------------------------------------------------
#         while True:
#             spoken: str | None = None
#             try:
#                 while True:
#                     item = queue.get_nowait()
#                     spoken = f"{spoken} {item}".strip() if spoken else item
#                     queue.task_done()
#             except asyncio.QueueEmpty:
#                 pass

#             if spoken:
#                 ws.app.state.tts_client.stop_speaking()
#                 for t in list(getattr(ws.app.state, "tts_tasks", [])):
#                     t.cancel()

#                 await broadcast_message(clients, spoken, "User")

#                 if check_for_stopwords(spoken):
#                     await broadcast_message(clients, "Goodbye!", "Assistant")
#                     await send_response_to_acs(ws, "Goodbye!", blocking=True)
#                     await asyncio.sleep(1)
#                     await acs.disconnect_call(cid)
#                     break

#                 await route_turn(cm, spoken, ws, is_acs=True)
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

#                 if user_raw_id and data["audioData"]["participantRawID"] != user_raw_id:
#                     continue        # discard bot’s own audio

#                 try:
#                     push_stream.write(b64decode(data["audioData"]["data"]))
#                 except Exception:
#                     # keep going even if decode glitches
#                     continue

#             elif kind == "CallConnected":
#                 pid = data["callConnected"]["participant"]["rawID"]
#                 call_user_raw_ids[cid] = pid
#                 user_raw_id = pid

#     finally:
#         try:
#             recogniser.stop_continuous_recognition_async()
#         except Exception:  # pylint: disable=broad-except
#             pass
#         push_stream.close()
#         await ws.close()
#         call_user_raw_ids.pop(cid, None)
#         cm.persist_to_redis(redis_mgr)
#         logger.info("◀ media WS closed – %s", cid)


@router.websocket(ACS_WEBSOCKET_PATH)
async def acs_media_ws(ws: WebSocket):
    acs = ws.app.state.acs_caller
    if not acs:
        await ws.close(code=1011)
        return

    await ws.accept()
    cid = ws.headers.get("x-ms-call-connection-id", "UnknownCall")
    logger.info("▶ media WS connected – %s", cid)

    # ── per-call objects ────────────────────────────────────────────────────
    redis_mgr = ws.app.state.redis
    try:
        cm = await ConversationManager.from_redis(cid, redis_mgr)
        if not cm:
            raise ValueError(f"Failed to initialize ConversationManager with Redis for call ID: {cid}")
        ws.state.cm = cm
        ws.state.lt = LatencyTool(cm)
    except Exception as e:
        logger.error(f"Error initializing ConversationManager or LatencyTool: {e}", exc_info=True)
        await ws.close(code=1011)
        return

    # greeting (once per call)
    if cid not in ws.app.state.greeted_call_ids:
        greet = (
            "Hello from XMYX Healthcare Company! Before I can assist you, "
            "let’s verify your identity. How may I address you?"
        )
        await broadcast_message(ws.app.state.clients, greet, "Assistant")
        await send_response_to_acs(ws, greet)
        await cm.append_to_history("assistant", greet)
        ws.app.state.greeted_call_ids.add(cid)

    # ---------- AOAI Streaming STT ----------------------------------------
    aoai_cfg = ws.app.state.aoai_stt_cfg
    audio_q: asyncio.Queue[Optional[bytes]] = asyncio.Queue()

    async def on_delta(d: str):
        """
        Handle partial speech detected by AOAI STT.

        - Stop ACS playback (cancel any audio bot is sending to phone).
        - Stop sending audio from browser to ACS.
        - Stop any local TTS audio playback.
        - Broadcast the partial transcription.
        """
        ws.app.state.tts_stop_flag = True
        if not d.strip():
            return

        try:
            call_connection = acs.call_automation_client.get_call_connection(cid)
            await call_connection.cancel_all_media_operations()
            logger.info(f"[🛑] Stopped ACS playback for call {cid}.")
        except Exception as e:
            logger.warning(f"[!] Could not stop ACS playback: {e}")

        try:
            await stop_audio(ws)
            logger.info(f"[🛑] Stopped audio from browser to ACS for call {cid}.")
        except Exception as e:
            logger.warning(f"[!] Could not stop browser audio: {e}")

        # Broadcast the partial transcription to connected dashboards
        await broadcast_message(ws.app.state.clients, d, "User")

    lt: LatencyTool = ws.state.lt

    async def on_transcript(t: str):
        logger.info(f"[AOAI-STT] {t}")
        await broadcast_message(ws.app.state.clients, t, "User")
        lt.stop("stt", ws.app.state.redis)

        # Stop local TTS
        ws.app.state.tts_client.stop_speaking()
        for task in list(getattr(ws.app.state, "tts_tasks", [])):
            task.cancel()

        # Main dialog routing
        await route_turn(cm, t, ws, is_acs=True)

    transcriber = AudioTranscriber(
        url=aoai_cfg["url"],
        headers=aoai_cfg["headers"],
        rate=aoai_cfg["rate"],
        channels=aoai_cfg["channels"],
        format_=aoai_cfg["format_"],
        chunk=1024,
    )

    transcribe_task = asyncio.create_task(
        transcriber.transcribe(
            audio_queue=audio_q,
            model="gpt-4o-transcribe",
            prompt="Respond in English. This is a medical environment.",
            noise_reduction="near_field",
            vad_type="server_vad",
            vad_config=aoai_cfg["vad"],
            on_delta=lambda d: asyncio.create_task(on_delta(d)),
            on_transcript=lambda t: asyncio.create_task(on_transcript(t)),
        )
    )

    user_raw_id: Optional[str] = call_user_raw_ids.get(cid)
    try:
        while True:
            try:
                raw = await asyncio.wait_for(ws.receive_text(), timeout=5.0)
                data = json.loads(raw)
            except asyncio.TimeoutError:
                if ws.client_state != WebSocketState.CONNECTED:
                    break
                continue
            except (WebSocketDisconnect, json.JSONDecodeError):
                break

            kind = data.get("kind")
            if kind == "AudioData":
                # dynamically learn / confirm the caller’s participantRawID
                if not user_raw_id and cid in call_user_raw_ids:
                    user_raw_id = call_user_raw_ids[cid]
                # ignore our own TTS loop-back
                if user_raw_id and data["audioData"]["participantRawID"] != user_raw_id:
                    continue
                lt.start("stt")
                await audio_q.put(b64decode(data["audioData"]["data"]))

            elif kind == "CallConnected":
                pid = data["callConnected"]["participant"]["rawID"]
                call_user_raw_ids[cid] = pid
                user_raw_id = pid

            elif kind in ("PlayCompleted", "PlayFailed", "PlayCanceled"):
                logger.info("%s from ACS (%s)", kind, cid)

            # basic hang-up keywords (optional)
            if kind == "AudioData" and check_for_stopwords(""):
                await broadcast_message(ws.app.state.clients, "Goodbye!", "Assistant")
                await send_response_to_acs(ws, "Goodbye!", blocking=True)
                await asyncio.sleep(1)
                await acs.disconnect_call(cid)
                break

    finally:
        await audio_q.put(None)  # flush / stop AOAI
        with contextlib.suppress(Exception):
            await transcribe_task
        with contextlib.suppress(Exception):
            if ws.client_state == WebSocketState.CONNECTED:
                await ws.close()
        call_user_raw_ids.pop(cid, None)
        await cm.persist_to_redis(redis_mgr)
        try:
            cm = getattr(ws.state, "cm", None)
            cosmos = getattr(ws.app.state, "cosmos", None)
            if cm and cosmos:
                build_and_flush(cm, cosmos)
        except Exception as e:
            logger.error(f"Error persisting analytics: {e}", exc_info=True)
        logger.info("◀ media WS closed – %s", cid)


# --------------------------------------------------------------------------- #
#  Session Management and Recording Information Endpoints
# --------------------------------------------------------------------------- #
@router.get("/call/sessions")
async def get_active_sessions():
    """Get information about all active call sessions."""
    try:
        sessions = acs_event_manager.list_active_sessions()
        
        # Convert sessions to serializable format
        session_data = {}
        for call_id, session in sessions.items():
            session_data[call_id] = {
                "call_connection_id": session.call_connection_id,
                "server_call_id": session.server_call_id,
                "participant_raw_id": session.participant_raw_id,
                "recording_id": session.recording_id,
                "recording_state": session.recording_state,
                "connected_at": session.connected_at.isoformat() if session.connected_at else None,
                "disconnected_at": session.disconnected_at.isoformat() if session.disconnected_at else None,
                "media_streaming_active": session.media_streaming_active,
                "recording_url": session.recording_url,
            }
            
        return {
            "active_sessions": session_data,
            "total_count": len(session_data),
        }
        
    except Exception as exc:
        logger.error("Error getting active sessions: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc)) from exc


@router.get("/call/{call_connection_id}/session")
async def get_call_session(call_connection_id: str):
    """Get detailed information about a specific call session."""
    try:
        session = acs_event_manager.get_session_info(call_connection_id)
        
        if not session:
            raise HTTPException(404, f"No session found for call ID: {call_connection_id}")
            
        return {
            "call_connection_id": session.call_connection_id,
            "server_call_id": session.server_call_id,
            "participant_raw_id": session.participant_raw_id,
            "recording_id": session.recording_id,
            "recording_state": session.recording_state,
            "connected_at": session.connected_at.isoformat() if session.connected_at else None,
            "disconnected_at": session.disconnected_at.isoformat() if session.disconnected_at else None,
            "media_streaming_active": session.media_streaming_active,
            "recording_url": session.recording_url,
            "call_duration_seconds": (
                (session.disconnected_at - session.connected_at).total_seconds()
                if session.connected_at and session.disconnected_at
                else None
            ),
        }
        
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Error getting call session: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc)) from exc


@router.post("/call/sessions/cleanup")
async def cleanup_old_sessions(max_age_hours: int = 24):
    """Clean up old inactive call sessions."""
    try:
        removed_count = await acs_event_manager.cleanup_old_sessions(max_age_hours)
        
        return {
            "status": "success",
            "removed_sessions": removed_count,
            "max_age_hours": max_age_hours,
        }
        
    except Exception as exc:
        logger.error("Error cleaning up sessions: %s", exc, exc_info=True)
        raise HTTPException(500, str(exc)) from exc
