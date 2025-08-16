from __future__ import annotations
# apps/rtagent/backend/api/v1/endpoints/media.py

"""
Media Management Endpoints - V1 Enterprise Architecture
======================================================

REST API endpoints for audio streaming, transcription, and media processing.
Provides enterprise-grade ACS media streaming with pluggable orchestrator support.

V1 Architecture Improvements:
- Clean separation of concerns with focused helper functions
- Consistent error handling and tracing patterns
- Modular dependency management and validation
- Enhanced session management with proper resource cleanup
- Integration with V1 ACS media handler and orchestrator system
- Production-ready WebSocket handling with graceful failure modes

Key V1 Features:
- Pluggable orchestrator support for different conversation engines
- Enhanced observability with OpenTelemetry tracing
- Robust error handling and resource cleanup
- Session-based media streaming with proper state management
- Clean abstractions for testing and maintenance

WebSocket Flow:
1. Accept connection and validate dependencies
2. Authenticate if required
3. Extract and validate call connection ID
4. Create appropriate media handler (Media/Transcription mode)
5. Process streaming messages with error handling
6. Clean up resources on disconnect/error
"""
# apps/rtagent/backend/api/v1/endpoints/media.py
"""
Media Management Endpoints - V1 Enterprise Architecture
======================================================

REST API endpoints for audio streaming, transcription, and media processing.
Provides enterprise-grade ACS media streaming with pluggable orchestrator support.

V1 Architecture Improvements:
- Clean separation of concerns with focused helper functions
- Consistent error handling and tracing patterns
- Modular dependency management and validation
- Enhanced session management with proper resource cleanup
- Integration with V1 ACS media handler and orchestrator system
- Production-ready WebSocket handling with graceful failure modes

Key V1 Features:
- Pluggable orchestrator support for different conversation engines
- Enhanced observability with OpenTelemetry tracing
- Robust error handling and resource cleanup
- Session-based media streaming with proper state management
- Clean abstractions for testing and maintenance

WebSocket Flow:
1. Accept connection and validate dependencies
2. Authenticate if required
3. Extract and validate call connection ID
4. Create appropriate media handler (Media/Transcription mode)
5. Process streaming messages with error handling
6. Clean up resources on disconnect/error
"""

import asyncio
import json
import uuid
from typing import Optional, Dict, Any
from datetime import datetime

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, status, Depends
from fastapi.websockets import WebSocketState
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode
from azure.communication.callautomation import PhoneNumberIdentifier

from apps.rtagent.backend.api.v1.schemas.media import (
    MediaSessionRequest,
    MediaSessionResponse,
    AudioStreamStatus,
)
from apps.rtagent.backend.settings import ACS_STREAMING_MODE, ENABLE_AUTH_VALIDATION
from src.enums.stream_modes import StreamMode
from src.stateful.state_managment import MemoManager
from apps.rtagent.backend.src.latency.latency_tool import LatencyTool
from apps.rtagent.backend.src.utils.auth import validate_acs_ws_auth, AuthError
from apps.rtagent.backend.src.utils.tracing import log_with_context
from utils.ml_logging import get_logger

# V1 components
from ..handlers.acs_media_lifecycle import ACSMediaHandler
from ..dependencies.orchestrator import get_orchestrator

logger = get_logger("api.v1.endpoints.media")
tracer = trace.get_tracer(__name__)

router = APIRouter(prefix="/media", tags=["Media Streaming"])


# ---------------------------------------------------------------------------
# Small broadcast helpers (reuse process-wide registries from app.state)
# ---------------------------------------------------------------------------
async def _broadcast_to_session(app, session_id: str, payload: Dict[str, Any]) -> int:
    """Send payload to all browser sockets registered for session_id."""
    data = json.dumps({**payload, "session_id": session_id})
    async with app.state.session_lock:
        targets = tuple(app.state.session_sockets.get(session_id, ()))

    sent = 0
    for ws in targets:
        if ws.application_state == WebSocketState.CONNECTED:
            try:
                await ws.send_text(data)
                sent += 1
            except Exception:
                pass
    return sent


async def _broadcast_dashboards(app, payload: Dict[str, Any]) -> int:
    """Best-effort broadcast to operator dashboards."""
    data = json.dumps(payload)
    sent = 0
    for ws in tuple(getattr(app.state, "clients", set())):
        if ws.application_state == WebSocketState.CONNECTED:
            try:
                await ws.send_text(data)
                sent += 1
            except Exception:
                pass
    return sent


async def _resolve_session_id(app, call_connection_id: str, timeout_s: float = 5.0) -> Optional[str]:
    """
    Wait (briefly) for call_id→session_id mapping to appear.
    Handles the race where the media WS connects before the API has registered the mapping.
    """
    if not call_connection_id:
        return None

    deadline = asyncio.get_event_loop().time() + timeout_s
    while True:
        async with app.state.session_lock:
            sid = app.state.call_session.get(call_connection_id)
        if sid:
            return sid

        if asyncio.get_event_loop().time() >= deadline:
            return None
        await asyncio.sleep(0.05)  # 50ms backoff


def _require_dependency(obj, name: str):
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"{name} not initialised",
        )


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------
@router.get("/status", response_model=dict, summary="Get Media Streaming Status")
async def get_media_status():
    return {
        "status": "available",
        "streaming_mode": str(ACS_STREAMING_MODE),
        "websocket_endpoint": "/api/v1/media/stream",
        "protocols_supported": ["WebSocket"],
        "features": {
            "real_time_audio": True,
            "transcription": True,
            "orchestrator_support": True,
            "session_management": True,
        },
        "version": "v1",
    }


@router.post("/sessions", response_model=MediaSessionResponse, summary="Create Media Session")
async def create_media_session(request: MediaSessionRequest):
    session_id = str(uuid.uuid4())
    return MediaSessionResponse(
        session_id=session_id,
        websocket_url=f"/api/v1/media/stream?call_connection_id={request.call_connection_id}",
        status=AudioStreamStatus.PENDING,
        call_connection_id=request.call_connection_id,
        created_at=datetime.utcnow(),
    )


@router.get("/sessions/{session_id}", response_model=dict, summary="Get Media Session Status")
async def get_media_session(session_id: str):
    # Placeholder (extend if you persist media session state)
    return {
        "session_id": session_id,
        "status": "active",
        "websocket_connected": False,
        "created_at": datetime.utcnow().isoformat(),
        "version": "v1",
    }


# ---------------------------------------------------------------------------
# WebSocket endpoint
# ---------------------------------------------------------------------------
@router.websocket("/stream")
async def acs_media_stream(
    websocket: WebSocket,
    orchestrator=Depends(get_orchestrator),
):
    """
    WebSocket endpoint for real-time ACS media streaming.
    - Resolves session_id from app.state.call_session using call_connection_id
    - Creates exactly one media handler per call
    - Broadcasts lifecycle status to dashboards & the correct browser tab
    """
    handler = None
    call_connection_id: Optional[str] = None
    session_id: Optional[str] = None

    try:
        # Accept first to allow ACS to proceed
        await websocket.accept()
        logger.info("Media WS accepted; extracting call_connection_id")

        # Extract call_connection_id (query → header)
        q = dict(websocket.query_params)
        call_connection_id = q.get("call_connection_id") or dict(websocket.headers).get("x-ms-call-connection-id")
        if not call_connection_id:
            await websocket.close(code=1002, reason="Missing call_connection_id")
            raise HTTPException(400, "Missing call_connection_id")

        # Resolve session_id from the call_session map (race-safe)
        session_id = await _resolve_session_id(websocket.app, call_connection_id) or call_connection_id
        websocket.state.session_id = session_id  # make available to handlers

        # Accept span
        with tracer.start_as_current_span(
            "api.v1.media.websocket_accept",
            kind=SpanKind.SERVER,
            attributes={
                "api.version": "v1",
                "media.session_id": session_id,
                "call.connection.id": call_connection_id,
                "network.protocol.name": "websocket",
            },
        ) as accept_span:
            # Dependencies
            await _validate_ws_dependencies(websocket)

            # Auth (if enabled) - differentiate between ACS and browser connections
            if ENABLE_AUTH_VALIDATION:
                await _validate_ws_auth_smart(websocket, call_connection_id)

            # Validate call exists
            await _validate_call_connection(websocket, call_connection_id)
            accept_span.set_status(Status(StatusCode.OK))

        # Initialize handler (ensure single active handler per call)
        with tracer.start_as_current_span(
            "api.v1.media.initialize_handler",
            kind=SpanKind.CLIENT,
            attributes={
                "api.version": "v1",
                "call.connection.id": call_connection_id,
                "orchestrator.name": getattr(orchestrator, "name", "unknown"),
                "stream.mode": str(ACS_STREAMING_MODE),
            },
        ):
            handler = await _create_media_handler(websocket, call_connection_id, session_id, orchestrator)

            await handler.start()
            logger.info(f"Media handler started for call {call_connection_id}")

            # Ack + broadcast
            ack = {
                "type": "media_status",
                "stage": "connected",
                "call_id": call_connection_id,
                "session_id": session_id,
                "message": "Media WebSocket connected",
            }
            try:
                await websocket.send_text(json.dumps({
                    "kind": "ConnectionEstablished",
                    "connectionId": call_connection_id,
                    "status": "ready",
                    "session_id": session_id,
                }))
            except Exception:
                pass
            await _broadcast_dashboards(websocket.app, ack)
            await _broadcast_to_session(websocket.app, session_id, ack)

        # Process stream
        await _process_media_stream(websocket, handler, call_connection_id)

    except WebSocketDisconnect as e:
        _log_ws_disconnect(e, session_id, call_connection_id)
    except Exception as e:
        _log_ws_error(e, session_id, call_connection_id)
        if not isinstance(e, WebSocketDisconnect):
            raise
    finally:
        await _cleanup_ws_resources(websocket, handler, call_connection_id, session_id)


# ======================================================================
# Helpers
# ======================================================================
async def _validate_ws_dependencies(websocket: WebSocket) -> None:
    app = websocket.app
    _require_dependency(getattr(app.state, "acs_caller", None), "ACS")
    _require_dependency(getattr(app.state, "stt_client", None), "STT")
    _require_dependency(getattr(app.state, "redis", None), "Redis")

    # Ensure media handler registry exists
    if not hasattr(app.state, "active_media_handlers"):
        app.state.active_media_handlers = {}  # type: ignore[attr-defined]


async def _validate_ws_auth_smart(websocket: WebSocket, call_connection_id: str) -> None:
    """
    Smart authentication that handles both ACS and browser WebSocket connections.
    ACS connections are authenticated via call_connection_id validation.
    Browser connections require Bearer token.
    """
    # Check if this is an ACS connection (from Azure IP ranges)
    client_host = getattr(websocket.client, 'host', '') if websocket.client else ''
    is_acs_connection = (
        client_host.startswith('52.') or  # Azure IP ranges
        client_host.startswith('40.') or
        client_host.startswith('13.') or
        call_connection_id and len(call_connection_id) == 36  # UUID format from ACS
    )
    
    if is_acs_connection:
        # For ACS connections, validate that call_connection_id exists in our system
        if not call_connection_id:
            await websocket.close(code=4001, reason="ACS connection missing call_connection_id")
            raise HTTPException(401, "ACS connection missing call_connection_id")
        logger.info(f"ACS WebSocket connection authenticated via call_connection_id: {call_connection_id}")
        return
    
    # For browser connections, require Bearer token
    try:
        _ = await validate_acs_ws_auth(websocket)
        logger.info("Browser WebSocket connection authenticated via Bearer token")
    except AuthError as e:
        await websocket.close(code=4001, reason="Authentication failed")
        raise HTTPException(401, f"Authentication failed: {str(e)}")


async def _validate_ws_auth(websocket: WebSocket) -> None:
    try:
        _ = await validate_acs_ws_auth(websocket)
        logger.info("Media WS authenticated")
    except AuthError as e:
        await websocket.close(code=4001, reason="Authentication failed")
        raise HTTPException(401, f"Authentication failed: {str(e)}")


async def _validate_call_connection(websocket: WebSocket, call_connection_id: str) -> None:
    conn = websocket.app.state.acs_caller.get_call_connection(call_connection_id)
    if not conn:
        await websocket.close(code=1000, reason="Call not found")
        raise HTTPException(404, f"Call connection {call_connection_id} not found")
    logger.info(f"Validated call connection {call_connection_id}")


async def _create_media_handler(
    websocket: WebSocket,
    call_connection_id: str,
    session_id: str,
    orchestrator,
):
    """
    Create appropriate handler and ensure only one active handler per call.
    """
    app = websocket.app
    # Stop/replace any existing handler
    existing = app.state.active_media_handlers.get(call_connection_id)
    if existing and getattr(existing, "is_running", False):
        logger.warning(f"Existing handler found for {call_connection_id}; stopping it")
        try:
            await existing.stop()
        except Exception as e:
            logger.error(f"Error stopping existing handler: {e}")
    app.state.active_media_handlers.pop(call_connection_id, None)

    # Load memory (never fail)
    redis_mgr = app.state.redis
    try:
        memory_manager = MemoManager.from_redis(call_connection_id, redis_mgr) or MemoManager(session_id=call_connection_id)
    except Exception as e:
        logger.error(f"Redis memory load failed for {call_connection_id}: {e}")
        memory_manager = MemoManager(session_id=call_connection_id)

    # Latency tool on WS state
    websocket.state.lt = LatencyTool(memory_manager)
    websocket.state.lt.start("greeting_ttfb")
    websocket.state._greeting_ttfb_stopped = False

    # Optional: attach target number context for convenience
    target_phone_number = memory_manager.get_context("target_number")
    if target_phone_number:
        app.state.target_participant = PhoneNumberIdentifier(target_phone_number)

    app.state.cm = memory_manager
    app.state.call_conn = app.state.acs_caller.get_call_connection(call_connection_id)

    if ACS_STREAMING_MODE == StreamMode.MEDIA:
        handler = ACSMediaHandler(
            websocket=websocket,
            orchestrator_func=orchestrator,
            call_connection_id=call_connection_id,
            recognizer=app.state.stt_client,
            memory_manager=memory_manager,
            session_id=session_id,
        )
        app.state.active_media_handlers[call_connection_id] = handler
        logger.info("Created V1 ACS media handler (MEDIA mode)")
        return handler

    if ACS_STREAMING_MODE == StreamMode.TRANSCRIPTION:
        from apps.rtagent.backend.src.handlers import TranscriptionHandler
        handler = TranscriptionHandler(websocket, cm=memory_manager)
        app.state.active_media_handlers[call_connection_id] = handler
        logger.info("Created transcription handler (TRANSCRIPTION mode)")
        return handler

    # Fallback error
    msg = f"Unknown streaming mode: {ACS_STREAMING_MODE}"
    logger.error(msg)
    await websocket.close(code=1000, reason="Invalid streaming mode")
    raise HTTPException(400, msg)


async def _process_media_stream(websocket: WebSocket, handler, call_connection_id: str) -> None:
    with tracer.start_as_current_span(
        "api.v1.media.process_stream",
        kind=SpanKind.SERVER,
        attributes={
            "api.version": "v1",
            "call.connection.id": call_connection_id,
            "stream.mode": str(ACS_STREAMING_MODE),
        },
    ) as span:
        logger.info(f"🚀 Processing media stream for {call_connection_id}")
        try:
            message_count = 0
            while (
                websocket.client_state == WebSocketState.CONNECTED
                and websocket.application_state == WebSocketState.CONNECTED
            ):
                msg = await websocket.receive_text()
                message_count += 1

                if ACS_STREAMING_MODE == StreamMode.MEDIA:
                    await handler.handle_media_message(msg)
                else:
                    await handler.handle_transcription_message(msg)

            span.set_attribute("messages.processed", message_count)
            span.set_status(Status(StatusCode.OK))
        except WebSocketDisconnect as e:
            # Normal lifecycle; re-raise for outer logging
            raise
        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, f"Stream error: {e}"))
            logger.error(f"Stream processing error for {call_connection_id}: {e}")
            raise


def _log_ws_disconnect(e: WebSocketDisconnect, session_id: Optional[str], call_connection_id: Optional[str]) -> None:
    lvl = "info" if e.code in (1000, 1001) else "warning"
    msg = "WS disconnect (normal)" if lvl == "info" else "WS disconnect (abnormal)"
    log_with_context(
        logger,
        lvl,
        msg,
        operation="media_ws_disconnect",
        session_id=session_id,
        call_connection_id=call_connection_id,
        disconnect_code=e.code,
        reason=e.reason,
        api_version="v1",
    )


def _log_ws_error(e: Exception, session_id: Optional[str], call_connection_id: Optional[str]) -> None:
    lvl = "info" if isinstance(e, asyncio.CancelledError) else "error"
    log_with_context(
        logger,
        lvl,
        "Media WS error",
        operation="media_ws_error",
        session_id=session_id,
        call_connection_id=call_connection_id,
        error=str(e),
        error_type=type(e).__name__,
        api_version="v1",
    )


async def _cleanup_ws_resources(websocket: WebSocket, handler, call_connection_id: Optional[str], session_id: Optional[str]) -> None:
    with tracer.start_as_current_span(
        "api.v1.media.cleanup_resources",
        kind=SpanKind.INTERNAL,
        attributes={
            "api.version": "v1",
            "session_id": session_id or "",
            "call.connection.id": call_connection_id or "",
        },
    ) as span:
        try:
            # Close WS if still open
            if (
                websocket.client_state == WebSocketState.CONNECTED
                and websocket.application_state == WebSocketState.CONNECTED
            ):
                await websocket.close()

            # Stop handler
            if handler:
                try:
                    await handler.stop()
                except Exception as e:
                    logger.error(f"Handler stop error: {e}")
                    span.set_status(Status(StatusCode.ERROR, f"Handler cleanup error: {e}"))

            # Drop from registry
            app = websocket.app
            if call_connection_id and hasattr(app.state, "active_media_handlers"):
                app.state.active_media_handlers.pop(call_connection_id, None)

            # Broadcast end state (best-effort)
            if session_id:
                payload = {
                    "type": "media_status",
                    "stage": "disconnected",
                    "call_id": call_connection_id,
                    "session_id": session_id,
                    "message": "Media WebSocket disconnected",
                }
                await _broadcast_dashboards(app, payload)
                await _broadcast_to_session(app, session_id, payload)

            span.set_status(Status(StatusCode.OK))
            log_with_context(
                logger,
                "info",
                "Media WS cleanup complete",
                operation="media_ws_cleanup",
                session_id=session_id,
                call_connection_id=call_connection_id,
                api_version="v1",
            )
        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, f"Cleanup error: {e}"))
            logger.error(f"Cleanup error: {e}")


