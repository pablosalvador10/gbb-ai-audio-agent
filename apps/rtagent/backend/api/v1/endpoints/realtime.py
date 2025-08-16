# apps/rtagent/backend/api/v1/endpoints/realtime.py
from __future__ import annotations

"""
V1 Realtime API Endpoints - Production
-------------------------------------

This version is aligned with the app-wide factory in main.py:
- Uses process-wide registries on app.state (session_lock, session_sockets, clients)
- Requires ?session_id= for conversation sockets
- Per-connection state lives on websocket.state only
- Outbound frames always include session_id
"""

import asyncio
import json
import uuid
from datetime import datetime
from typing import Any, Dict, Iterable, Optional, Callable

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect, status
from starlette.websockets import WebSocketState
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

# Settings & core services
from apps.rtagent.backend.settings import (
    GREETING,
    ENABLE_AUTH_VALIDATION,
    VAD_SEMANTIC_SEGMENTATION,
    SILENCE_DURATION_MS,
    RECOGNIZED_LANGUAGE,
    AUDIO_FORMAT,
)
from apps.rtagent.backend.src.utils.auth import validate_acs_ws_auth, AuthError
from apps.rtagent.backend.src.utils.tracing import log_with_context
from apps.rtagent.backend.src.helpers import check_for_stopwords
from apps.rtagent.backend.src.latency.latency_tool import LatencyTool
from apps.rtagent.backend.src.orchestration.orchestrator import route_turn
from apps.rtagent.backend.src.shared_ws import send_tts_audio
from apps.rtagent.backend.src.services import StreamingSpeechRecognizerFromBytes

# Optional orchestrator injection
from ..dependencies.orchestrator import get_orchestrator

from utils.ml_logging import get_logger

logger = get_logger("api.v1.endpoints.realtime")
tracer = trace.get_tracer(__name__)

# IMPORTANT:
# v1_router prefixes "/api/v1", so we prefix only with "/realtime"
router = APIRouter(prefix="/realtime", tags=["Realtime"])


# ======================================================================
# Helper: broadcasting
# ======================================================================

async def broadcast_to_session(request, session_id: str, payload: Dict[str, Any]) -> int:
    """
    Send JSON payload to all websockets registered under a session_id.
    Returns the count of successful sends.
    """
    app = request.app
    data = json.dumps({**payload, "session_id": session_id})

    # Snapshot targets without holding the lock while sending
    async with app.state.session_lock:
        targets: Iterable[WebSocket] = tuple(app.state.session_sockets.get(session_id, ()))

    sent = 0
    for ws in targets:
        if ws.application_state == WebSocketState.CONNECTED:
            try:
                await ws.send_text(data)
                sent += 1
            except Exception:  # best-effort; cleanup handled elsewhere
                pass
    return sent


async def broadcast_to_dashboards(request, payload: Dict[str, Any]) -> int:
    """
    Send JSON payload to all dashboard clients (process-wide).
    """
    app = request.app
    data = json.dumps(payload)
    sent = 0
    for ws in tuple(app.state.clients):
        if ws.application_state == WebSocketState.CONNECTED:
            try:
                await ws.send_text(data)
                sent += 1
            except Exception:
                pass
    return sent


# ======================================================================
# Helper: dependencies & per-connection STT stream
# ======================================================================

async def _validate_realtime_dependencies(ws: WebSocket) -> None:
    """Validate app.state dependencies for realtime use."""
    app = ws.app
    # TTS
    if not getattr(app.state, "tts_client", None):
        await ws.close(code=1011, reason="TTS not initialized")
        raise HTTPException(503, "TTS client not initialized")
    # STT provider
    if not getattr(app.state, "stt_client", None):
        await ws.close(code=1011, reason="STT not initialized")
        raise HTTPException(503, "STT client not initialized")
    # Redis (session/memory)
    if not getattr(app.state, "redis", None):
        await ws.close(code=1011, reason="Redis not initialized")
        raise HTTPException(503, "Redis client not initialized")


def _build_stt_stream(app, partial_cb: Callable[[str, str], None], final_cb: Callable[[str, str], None]):
    """
    Create a per-connection STT stream while benefiting from app-wide warm SDKs.
    Strategy:
      1) If app.state.stt_client has a factory method, use it (preferred).
      2) Else, build a fresh per-session StreamingSpeechRecognizerFromBytes with same settings.
    Returned object should expose one of:
      - push_audio_chunk(bytes) or write_bytes(bytes), and
      - start(), stop()/close()
    """
    provider = app.state.stt_client

    # Preferred: factory API from the provider
    for method in ("create_stream", "begin_stream", "new_stream"):
        if hasattr(provider, method):
            stt_stream = getattr(provider, method)(
                partial_result_callback=partial_cb,
                final_result_callback=final_cb,
            )
            # If the stream needs an explicit start:
            if hasattr(stt_stream, "start"):
                try:
                    stt_stream.start()
                except Exception:
                    pass
            return stt_stream

    # Fallback: construct a dedicated recognizer with same settings
    stt_stream = StreamingSpeechRecognizerFromBytes(
        use_semantic_segmentation=VAD_SEMANTIC_SEGMENTATION,
        vad_silence_timeout_ms=SILENCE_DURATION_MS,
        candidate_languages=RECOGNIZED_LANGUAGE,
        audio_format=AUDIO_FORMAT,
    )
    if hasattr(stt_stream, "set_partial_result_callback"):
        stt_stream.set_partial_result_callback(partial_cb)
    if hasattr(stt_stream, "set_final_result_callback"):
        stt_stream.set_final_result_callback(final_cb)
    if hasattr(stt_stream, "start"):
        stt_stream.start()
    return stt_stream


def _stt_push_bytes(stt_stream, data: bytes):
    """Send PCM bytes into the STT stream, tolerating different method names."""
    for m in ("push_audio_chunk", "write_bytes", "push", "write"):
        if hasattr(stt_stream, m):
            return getattr(stt_stream, m)(data)
    # If none exist, ignore (better than crashing)


async def _stt_close(stt_stream):
    """Close a per-connection STT stream if the method exists."""
    for m in ("stop", "close", "shutdown"):
        if hasattr(stt_stream, m):
            try:
                res = getattr(stt_stream, m)()
                if asyncio.iscoroutine(res):
                    await res
            except Exception:
                pass
            break


# ======================================================================
# Status endpoint (uses app.state registries)
# ======================================================================

@router.get(
    "/status",
    summary="Get Realtime Service Status",
    tags=["Realtime Status"],
)
async def get_realtime_status(request):
    app = request.app
    dashboard_clients = len(getattr(app.state, "clients", set()))
    async with app.state.session_lock:
        session_count = len(app.state.session_sockets)
        socket_count = sum(len(s) for s in app.state.session_sockets.values())

    return {
        "status": "available",
        "websocket_endpoints": {
            "dashboard_relay": "/api/v1/realtime/dashboard/relay",
            "conversation": "/api/v1/realtime/conversation",
            "legacy_relay": "/api/v1/realtime/ws/relay",
            "legacy_conversation": "/api/v1/realtime/ws/conversation",
        },
        "features": {
            "dashboard_broadcasting": True,
            "conversation_streaming": True,
            "orchestrator_support": True,
            "session_management": True,
            "audio_interruption": True,
            "legacy_compatibility": True,
        },
        "active_connections": {
            "dashboard_clients": dashboard_clients,
            "sessions": session_count,
            "websockets": socket_count,
        },
        "protocols_supported": ["WebSocket"],
        "version": "v1",
    }


# ======================================================================
# Dashboard relay websocket
# ======================================================================

@router.websocket("/dashboard/relay")
async def dashboard_relay_ws(websocket: WebSocket):
    """
    Read-only relay for operator dashboards.
    We add/remove the socket from app.state.clients; incoming frames are ignored.
    """
    await websocket.accept()
    websocket.state.client_id = str(uuid.uuid4())[:8]
    
    # Extract session_id from query params for session-aware broadcasting
    session_id = websocket.query_params.get("session_id")
    if session_id:
        websocket.state.session_id = session_id
        # Register this WebSocket for session-specific broadcasting
        if not hasattr(websocket.app.state, "session_sockets"):
            websocket.app.state.session_sockets = {}
        if session_id not in websocket.app.state.session_sockets:
            websocket.app.state.session_sockets[session_id] = set()
        websocket.app.state.session_sockets[session_id].add(websocket)

    # Track global dashboard clients
    if not hasattr(websocket.app.state, "clients"):
        websocket.app.state.clients = set()
    websocket.app.state.clients.add(websocket)

    with tracer.start_as_current_span(
        "realtime.dashboard.connect",
        kind=SpanKind.SERVER,
        attributes={
            "api.version": "v1",
            "realtime.endpoint": "dashboard_relay",
            "client.id": websocket.state.client_id,
        },
    ) as span:
        span.set_status(Status(StatusCode.OK))
        log_with_context(
            logger, "info", "Dashboard client connected",
            operation="dashboard_connect",
            client_id=websocket.state.client_id,
            total_clients=len(websocket.app.state.clients),
            api_version="v1",
        )

    try:
        # Push-only channel from server; ignore inbound frames but keep the socket alive.
        while True:
            msg = await websocket.receive()
            if msg.get("type") == "websocket.disconnect":
                break
            # ignore text/bytes from dashboards
    except WebSocketDisconnect as e:
        pass
    finally:
        # Clean up global clients
        websocket.app.state.clients.discard(websocket)
        
        # Clean up session-specific registration
        session_id = getattr(websocket.state, "session_id", None)
        if session_id and hasattr(websocket.app.state, "session_sockets"):
            session_sockets = websocket.app.state.session_sockets.get(session_id, set())
            session_sockets.discard(websocket)
            if not session_sockets:  # Remove empty session
                websocket.app.state.session_sockets.pop(session_id, None)
        
        try:
            await websocket.close()
        except Exception:
            pass
        log_with_context(
            logger, "info", "Dashboard client disconnected",
            operation="dashboard_disconnect",
            client_id=getattr(websocket.state, "client_id", None),
            total_clients=len(websocket.app.state.clients),
            api_version="v1",
        )


# ======================================================================
# Conversation websocket (browser)
def _is_local_development(websocket: WebSocket) -> bool:
    """Check if this is a local development environment."""
    if not websocket.client:
        return False
    
    client_host = getattr(websocket.client, 'host', '')
    return (
        client_host.startswith('127.0.0.1') or
        client_host.startswith('localhost') or
        client_host.startswith('::1') or  # IPv6 localhost
        'devtunnels.ms' in (websocket.headers.get('host', ''))  # VS Code dev tunnels
    )


# ======================================================================

@router.websocket("/conversation")
async def conversation_ws(
    websocket: WebSocket,
    orchestrator: Optional[Callable] = Depends(get_orchestrator),
):
    """
    Primary realtime socket for browser conversations.
    Requires ?session_id=; all outbound frames include session_id.
    Per-connection STT stream is created and closed with the socket.
    """
    session_id = websocket.query_params.get("session_id")
    if not session_id:
        # Fail fast if no session_id was supplied
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    # Accept and set per-socket state (never on app.state)
    await websocket.accept()
    websocket.state.session_id = session_id
    websocket.state.server_seq = 0
    websocket.state.is_synthesizing = False
    websocket.state.user_buffer = ""

    with tracer.start_as_current_span(
        "realtime.conversation.connect",
        kind=SpanKind.SERVER,
        attributes={
            "api.version": "v1",
            "realtime.endpoint": "conversation",
            "realtime.session_id": session_id,
            "orchestrator.name": getattr(orchestrator, "name", "default") if orchestrator else "default",
        },
    ) as connect_span:
        # Validate dependencies (warm clients living on app.state)
        await _validate_realtime_dependencies(websocket)

        # Optional auth - only for production environments, skip for local development
        if ENABLE_AUTH_VALIDATION and not _is_local_development(websocket):
            try:
                _ = await validate_acs_ws_auth(websocket)
            except AuthError as e:
                await websocket.close(code=4001, reason="Authentication failed")
                raise HTTPException(401, f"Authentication failed: {str(e)}")

        # Register socket under the session (race-safe)
        async with websocket.app.state.session_lock:
            websocket.app.state.session_sockets[session_id].add(websocket)

        # Send initial status & greeting
        await websocket.send_text(json.dumps({
            "type": "status",
            "message": GREETING,
            "session_id": session_id,
        }))

        # Build latency tool & per-connection STT stream
        websocket.state.lt = LatencyTool(None)
        stt_stream = _build_stt_stream(
            websocket.app,
            partial_cb=lambda txt, lang: asyncio.create_task(
                websocket.send_text(json.dumps({
                    "type": "assistant_streaming",
                    "content": txt,
                    "session_id": session_id,
                }))
            ),
            final_cb=lambda txt, lang: _buffer_user_text(websocket, txt),
        )
        websocket.state.stt_stream = stt_stream

        # Optional TTS welcome (non-blocking)
        try:
            await send_tts_audio(GREETING, websocket, latency_tool=websocket.state.lt)
        except Exception:
            # Don't fail the connection if TTS welcome has a hiccup
            pass

        connect_span.set_status(Status(StatusCode.OK))
        log_with_context(
            logger, "info", "Conversation connected",
            operation="conversation_connect",
            session_id=session_id,
            api_version="v1",
        )

    # ------------------ receive loop ------------------ #
    try:
        while True:
            msg = await websocket.receive()

            # Disconnect
            if msg.get("type") == "websocket.disconnect":
                break

            # Binary: PCM audio
            if msg.get("bytes") is not None:
                _stt_push_bytes(websocket.state.stt_stream, msg["bytes"])

                # When we have buffered text (from final callbacks), treat it as a user turn
                if websocket.state.user_buffer.strip():
                    prompt = websocket.state.user_buffer.strip()
                    websocket.state.user_buffer = ""

                    # Echo user to UI
                    await websocket.send_text(json.dumps({
                        "sender": "User",
                        "message": prompt,
                        "session_id": session_id,
                    }))

                    # Stopword check
                    if check_for_stopwords(prompt):
                        goodbye = "Thank you for using our service. Goodbye."
                        await websocket.send_text(json.dumps({
                            "type": "exit",
                            "message": goodbye,
                            "session_id": session_id,
                        }))
                        try:
                            await send_tts_audio(goodbye, websocket, latency_tool=websocket.state.lt)
                        except Exception:
                            pass
                        break

                    # Route a conversation turn via orchestrator
                    await route_turn(None, prompt, websocket, is_acs=False)

                continue  # back to receive loop

            # Text frame: must be JSON
            if msg.get("text") is not None:
                try:
                    payload = json.loads(msg["text"])
                except json.JSONDecodeError:
                    await websocket.send_text(json.dumps({
                        "type": "error",
                        "error": "invalid_json",
                        "detail": "Text frames must be JSON objects.",
                        "session_id": session_id,
                    }))
                    continue

                # Defensive: drop if a mismatching session_id is claimed
                claimed = payload.get("session_id")
                if claimed and claimed != session_id:
                    continue

                websocket.state.server_seq += 1
                # Example ack (extend as needed to handle client control messages)
                await websocket.send_text(json.dumps({
                    "type": "ack",
                    "ok": True,
                    "session_id": session_id,
                    "server_seq": websocket.state.server_seq,
                }))

    except WebSocketDisconnect:
        pass
    except Exception as e:
        # Attempt a terminal error if still connected
        try:
            await websocket.send_text(json.dumps({
                "type": "error",
                "error": "server_exception",
                "detail": str(e),
                "session_id": session_id,
            }))
        except Exception:
            pass
        raise
    finally:
        # Cleanup: unregister, close STT, close socket
        async with websocket.app.state.session_lock:
            sockets = websocket.app.state.session_sockets.get(session_id, set())
            sockets.discard(websocket)
            if not sockets and session_id in websocket.app.state.session_sockets:
                del websocket.app.state.session_sockets[session_id]

        await _stt_close(getattr(websocket.state, "stt_stream", None))

        try:
            await websocket.close()
        except Exception:
            pass

        log_with_context(
            logger, "info", "Conversation disconnected",
            operation="conversation_disconnect",
            session_id=session_id,
            api_version="v1",
        )


def _buffer_user_text(websocket: WebSocket, txt: str):
    """Append final ASR result to the per-socket buffer."""
    try:
        websocket.state.user_buffer += txt.strip() + "\n"
    except Exception:
        # If socket is gone, ignore
        pass


# ======================================================================
# Legacy compatibility endpoints (route through new handlers)
# ======================================================================

@router.websocket("/ws/relay")
async def legacy_dashboard_relay(websocket: WebSocket):
    # For compatibility; consider migrating clients to /realtime/dashboard/relay
    await dashboard_relay_ws(websocket)


@router.websocket("/ws/conversation")
async def legacy_conversation(
    websocket: WebSocket,
    orchestrator: Optional[Callable] = Depends(get_orchestrator),
):
    # For compatibility; requires ?session_id= as well
    await conversation_ws(websocket, orchestrator)
