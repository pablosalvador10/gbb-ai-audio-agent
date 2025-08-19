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

from typing import Optional
from fastapi import (
    APIRouter,
    Depends,
    HTTPException,
    status,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.websockets import WebSocketState
import asyncio
import json
import uuid

from datetime import datetime

from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from apps.rtagent.backend.api.v1.schemas.media import (
    MediaSessionRequest,
    MediaSessionResponse,
    AudioStreamStatus,
)

from apps.rtagent.backend.settings import ACS_STREAMING_MODE, ENABLE_AUTH_VALIDATION
from src.speech.speech_recognizer import StreamingSpeechRecognizerFromBytes
from src.enums.stream_modes import StreamMode
from src.stateful.state_managment import MemoManager
from src.latency.tool_suite import LatencyTracker
from apps.rtagent.backend.src.utils.tracing import log_with_context
from apps.rtagent.backend.src.utils.auth import validate_acs_ws_auth, AuthError
from utils.ml_logging import get_logger
from azure.communication.callautomation import PhoneNumberIdentifier

# Import V1 components
from ..handlers.acs_media_lifecycle import ACSMediaHandler
from ..dependencies.orchestrator import get_orchestrator

logger = get_logger("api.v1.endpoints.media")
tracer = trace.get_tracer(__name__)

# Global registry to track active handlers per call connection ID
_active_handlers = {}

router = APIRouter()


@router.get("/status", response_model=dict, summary="Get Media Streaming Status")
async def get_media_status():
    """
    Get the current status of media streaming configuration.

    Returns:
        dict: Current media streaming configuration and status
    """
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


@router.post(
    "/sessions", response_model=MediaSessionResponse, summary="Create Media Session"
)
async def create_media_session(request: MediaSessionRequest):
    """
    Create a new media streaming session.

    Args:
        request: Media session configuration

    Returns:
        MediaSessionResponse: Session creation result with WebSocket connection details
    """
    session_id = str(uuid.uuid4())

    return MediaSessionResponse(
        session_id=session_id,
        websocket_url=f"/api/v1/media/stream?call_connection_id={request.call_connection_id}",
        status=AudioStreamStatus.PENDING,
        call_connection_id=request.call_connection_id,
        created_at=datetime.utcnow(),
    )


@router.get(
    "/sessions/{session_id}", response_model=dict, summary="Get Media Session Status"
)
async def get_media_session(session_id: str):
    """
    Get the status of a specific media session.

    Args:
        session_id: The unique session identifier

    Returns:
        dict: Session status and information
    """
    # This is a placeholder - in a real implementation, you'd query session state
    return {
        "session_id": session_id,
        "status": "active",
        "websocket_connected": False,  # Would check actual connection status
        "created_at": datetime.utcnow().isoformat(),
        "version": "v1",
    }


@router.websocket("/stream")
async def acs_media_stream(
    websocket: WebSocket,
):
    """
    WebSocket endpoint for real-time ACS media streaming.

    Provides enterprise-grade audio streaming with pluggable orchestrator support.
    Follows V1 architecture patterns with clean separation of concerns.

    Args:
        websocket: WebSocket connection from ACS
        orchestrator: Injected conversation orchestrator
    """
    handler = None
    call_connection_id = None
    session_id = None
    orchestrator = get_orchestrator()
    try:
        # Accept WebSocket connection first
        await websocket.accept()
        logger.info("WebSocket connection accepted, extracting call connection ID")

        # Extract call_connection_id from WebSocket query parameters or wait for first message
        query_params = dict(websocket.query_params)
        call_connection_id = query_params.get("call_connection_id")
        logger.info(f"🔍 Query params: {query_params}")

        # If not in query params, check headers
        if not call_connection_id:
            headers_dict = dict(websocket.headers)
            call_connection_id = headers_dict.get("x-ms-call-connection-id")
            logger.debug(f"🔍 Headers: {headers_dict}")

        session_id = call_connection_id
        logger.info(f"✅ Call connection ID determined: {call_connection_id}")

        # Start tracing with valid call connection ID
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
            # Validate dependencies first
            await _validate_websocket_dependencies(websocket)

            # Authenticate if required
            if ENABLE_AUTH_VALIDATION:
                await _validate_websocket_auth(websocket)

            # Validate call connection exists
            await _validate_call_connection(websocket, call_connection_id)

            accept_span.set_attribute("call.connection.id", call_connection_id)
            logger.info(
                f"WebSocket connection established for call: {call_connection_id}"
            )

        # Initialize media handler with V1 patterns
        with tracer.start_as_current_span(
            "api.v1.media.initialize_handler",
            kind=SpanKind.CLIENT,
            attributes={
                "api.version": "v1",
                "call.connection.id": call_connection_id,
                "orchestrator.name": getattr(orchestrator, "name", "unknown"),
                "stream.mode": str(ACS_STREAMING_MODE),
            },
        ) as init_span:
            handler = await _create_media_handler(
                websocket=websocket,
                call_connection_id=call_connection_id,
                session_id=session_id,
                orchestrator=orchestrator,
            )

            # Start the handler
            await handler.start()
            init_span.set_attribute("handler.initialized", True)
            logger.info(
                f"✅ Media handler initialized and started for call: {call_connection_id}"
            )

            # Track WebSocket connection for session metrics
            if hasattr(websocket.app.state, "session_metrics"):
                await websocket.app.state.session_metrics.increment_connected()

            # Send acknowledgment message to ACS to confirm connection is ready
            try:
                await websocket.send_text(
                    json.dumps(
                        {
                            "kind": "ConnectionEstablished",
                            "connectionId": call_connection_id,
                            "status": "ready",
                        }
                    )
                )
                logger.info("📡 Connection acknowledgment sent to ACS")
            except Exception as ack_error:
                logger.warning(f"Failed to send connection acknowledgment: {ack_error}")
                # Continue anyway - this is not critical

        # Process media messages with clean loop
        await _process_media_stream(websocket, handler, call_connection_id)

    except WebSocketDisconnect as e:
        _log_websocket_disconnect(e, session_id, call_connection_id)
        # Don't re-raise WebSocketDisconnect as it's a normal part of the lifecycle
    except Exception as e:
        _log_websocket_error(e, session_id, call_connection_id)
        # Only raise non-disconnect errors
        if not isinstance(e, WebSocketDisconnect):
            raise
    finally:
        await _cleanup_websocket_resources(
            websocket, handler, call_connection_id, session_id
        )


# ============================================================================
# V1 Architecture Helper Functions
# ============================================================================


async def _validate_websocket_dependencies(websocket: WebSocket) -> None:
    """Validate required app state dependencies."""
    if (
        not hasattr(websocket.app.state, "acs_caller")
        or not websocket.app.state.acs_caller
    ):
        logger.error("ACS caller not initialized")
        await websocket.close(code=1011, reason="ACS not initialized")
        raise HTTPException(503, "ACS caller not initialized")

    # Per-connection STT recognizer now created later; no global validation here


async def _validate_websocket_auth(websocket: WebSocket) -> None:
    """Validate WebSocket authentication if enabled."""
    try:
        _ = await validate_acs_ws_auth(websocket)
        logger.info("WebSocket authenticated successfully")
    except AuthError as e:
        logger.warning(f"WebSocket authentication failed: {str(e)}")
        await websocket.close(code=4001, reason="Authentication failed")
        raise HTTPException(401, f"Authentication failed: {str(e)}")


async def _validate_call_connection(
    websocket: WebSocket, call_connection_id: str
) -> None:
    """Validate that the call connection exists."""
    acs_caller = websocket.app.state.acs_caller
    call_connection = acs_caller.get_call_connection(call_connection_id)

    if not call_connection:
        logger.warning(f"Call connection {call_connection_id} not found")
        await websocket.close(code=1000, reason="Call not found")
        raise HTTPException(404, f"Call connection {call_connection_id} not found")

    logger.info(f"Call connection validated: {call_connection_id}")


async def _create_media_handler(
    websocket: WebSocket,
    call_connection_id: str,
    session_id: str,
    orchestrator: callable,
):
    """Create appropriate media handler based on streaming mode."""

    # Check if there's already an active handler for this call ID
    if call_connection_id in _active_handlers:
        existing_handler = _active_handlers[call_connection_id]
        if existing_handler.is_running:
            logger.warning(
                f"⚠️ Handler already exists for call {call_connection_id}, stopping existing handler"
            )
            try:
                await existing_handler.stop()
            except Exception as e:
                logger.error(f"Error stopping existing handler: {e}")
        # Remove from registry regardless
        del _active_handlers[call_connection_id]

    redis_mgr = websocket.app.state.redis

    # Load conversation memory - ensure we always have a valid memory manager
    try:
        memory_manager = MemoManager.from_redis(call_connection_id, redis_mgr)
        if memory_manager is None:
            logger.warning(
                f"Memory manager from Redis returned None for {call_connection_id}, creating new one"
            )
            memory_manager = MemoManager(
                session_id=call_connection_id,
                redis_mgr=redis_mgr,
                enable_minimal_tracking=True
            )
        else:
            # Ensure existing memory manager from Redis has proper minimal tracking
            if not hasattr(memory_manager, '_latency_tracker') or memory_manager._latency_tracker is None:
                memory_manager._redis_manager = redis_mgr
                # Initialize latency tracker for restored MemoManager
                memory_manager._latency_tracker = LatencyTracker(memory_manager.session_id)
                logger.info(f"Initialized latency tracker for restored MemoManager {memory_manager.session_id}")
    except Exception as e:
        logger.error(
            f"Failed to load memory manager from Redis for {call_connection_id}: {e}"
        )
        logger.info(f"Creating new memory manager for {call_connection_id}")
        memory_manager = MemoManager(
            session_id=call_connection_id,
            redis_mgr=redis_mgr,  # Enable minimal tracking with auto-persistence
            enable_minimal_tracking=True
        )

    # Initialize minimal latency tracking
    websocket.state.cm = memory_manager
    # The MemoManager now includes built-in LatencyTracker (Suite v2)
    # No need for separate LatencyTool - cm.track() provides the same functionality
    websocket.state.lt = None  # Deprecated - use cm.track() directly
    
    # Set up call context in websocket state (per-connection)
    target_phone_number = memory_manager.get_context("target_number")
    if target_phone_number:
        websocket.state.target_participant = PhoneNumberIdentifier(target_phone_number)

    websocket.state.cm = memory_manager
    websocket.state.call_conn = websocket.app.state.acs_caller.get_call_connection(call_connection_id)

    if ACS_STREAMING_MODE == StreamMode.MEDIA:
        # Use the V1 ACS media handler - acquire recognizer from pool
        per_conn_recognizer = await websocket.app.state.stt_pool.acquire()
        websocket.state.stt_client = per_conn_recognizer
        logger.info(f"Acquired STT recognizer from pool for ACS call {call_connection_id}")
        handler = ACSMediaHandler(
            websocket=websocket,
            orchestrator_func=orchestrator,
            call_connection_id=call_connection_id,
            recognizer=per_conn_recognizer,
            memory_manager=memory_manager,
            session_id=session_id,
        )
        # Register the handler in the global registry
        _active_handlers[call_connection_id] = handler
        logger.info("Created V1 ACS media handler for MEDIA mode")
        return handler

    elif ACS_STREAMING_MODE == StreamMode.TRANSCRIPTION:
        # Import and use transcription handler for non-media mode
        from apps.rtagent.backend.src.handlers import TranscriptionHandler

        handler = TranscriptionHandler(websocket, cm=memory_manager)
        # Register the handler in the global registry
        _active_handlers[call_connection_id] = handler
        logger.info("Created transcription handler for TRANSCRIPTION mode")
        return handler
    else:
        error_msg = f"Unknown streaming mode: {ACS_STREAMING_MODE}"
        logger.error(error_msg)
        await websocket.close(code=1000, reason="Invalid streaming mode")
        raise HTTPException(400, error_msg)


async def _process_media_stream(
    websocket: WebSocket, handler, call_connection_id: str
) -> None:
    """Process incoming WebSocket media messages with clean error handling."""
    with tracer.start_as_current_span(
        "api.v1.media.process_stream",
        kind=SpanKind.SERVER,
        attributes={
            "api.version": "v1",
            "call.connection.id": call_connection_id,
            "stream.mode": str(ACS_STREAMING_MODE),
        },
    ) as span:
        logger.info(
            f"🚀 Starting media stream processing for call: {call_connection_id}"
        )

        try:
            # Main message processing loop
            message_count = 0
            while (
                websocket.client_state == WebSocketState.CONNECTED
                and websocket.application_state == WebSocketState.CONNECTED
            ):
                logger.debug(f"📨 Waiting for message #{message_count + 1}")
                msg = await websocket.receive_text()
                message_count += 1

                if msg:
                    # logger.info(f"📨 Received message #{message_count} ({len(msg)} chars)")
                    # Handle message based on streaming mode
                    if ACS_STREAMING_MODE == StreamMode.MEDIA:
                        await handler.handle_media_message(msg)
                    elif ACS_STREAMING_MODE == StreamMode.TRANSCRIPTION:
                        await handler.handle_transcription_message(msg)

        except WebSocketDisconnect as e:
            # Handle WebSocket disconnects gracefully - this is normal when calls end
            if e.code == 1000:
                logger.info(
                    f"📞 Call ended normally for {call_connection_id} (WebSocket code 1000)"
                )
                span.set_status(Status(StatusCode.OK))
            else:
                logger.warning(
                    f"📞 Call disconnected abnormally for {call_connection_id} (WebSocket code {e.code}): {e.reason}"
                )
                span.set_status(
                    Status(
                        StatusCode.ERROR, f"Abnormal disconnect: {e.code} - {e.reason}"
                    )
                )
            # Re-raise so the outer handler can log it properly
            raise
        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, f"Stream processing error: {e}"))
            logger.error(f"❌ Error in media stream processing: {e}")
            raise


def _log_websocket_disconnect(
    e: WebSocketDisconnect, session_id: str, call_connection_id: Optional[str]
) -> None:
    """Log WebSocket disconnection with appropriate level."""
    if e.code == 1000:
        log_with_context(
            logger,
            "info",
            "📞 Call ended normally - healthy WebSocket disconnect",
            operation="websocket_disconnect_normal",
            session_id=session_id,
            call_connection_id=call_connection_id,
            disconnect_code=e.code,
            api_version="v1",
        )
    elif e.code == 1001:
        log_with_context(
            logger,
            "info",
            "📞 Call ended - endpoint going away (normal)",
            operation="websocket_disconnect_normal",
            session_id=session_id,
            call_connection_id=call_connection_id,
            disconnect_code=e.code,
            api_version="v1",
        )
    else:
        log_with_context(
            logger,
            "warning",
            "📞 Call disconnected abnormally",
            operation="websocket_disconnect_abnormal",
            session_id=session_id,
            call_connection_id=call_connection_id,
            disconnect_code=e.code,
            reason=e.reason,
            api_version="v1",
        )


def _log_websocket_error(
    e: Exception, session_id: str, call_connection_id: Optional[str]
) -> None:
    """Log WebSocket errors with full context."""
    if isinstance(e, asyncio.CancelledError):
        log_with_context(
            logger,
            "info",
            "WebSocket cancelled",
            operation="websocket_error",
            session_id=session_id,
            call_connection_id=call_connection_id,
            api_version="v1",
        )
    else:
        log_with_context(
            logger,
            "error",
            "WebSocket error",
            operation="websocket_error",
            session_id=session_id,
            call_connection_id=call_connection_id,
            error=str(e),
            error_type=type(e).__name__,
            api_version="v1",
        )


async def _cleanup_websocket_resources(
    websocket: WebSocket, handler, call_connection_id: Optional[str], session_id: str
) -> None:
    """Clean up WebSocket resources following V1 patterns."""
    with tracer.start_as_current_span(
        "api.v1.media.cleanup_resources",
        kind=SpanKind.INTERNAL,
        attributes={
            "api.version": "v1",
            "session_id": session_id,
            "call.connection.id": call_connection_id,
        },
    ) as span:
        try:
            # Close WebSocket if still connected
            if (
                websocket.client_state == WebSocketState.CONNECTED
                and websocket.application_state == WebSocketState.CONNECTED
            ):
                await websocket.close()
                logger.info("WebSocket connection closed")

            # Track WebSocket disconnection for session metrics
            if hasattr(websocket.app.state, "session_metrics"):
                await websocket.app.state.session_metrics.increment_disconnected()

            # Memory manager cleanup
            if hasattr(websocket.state, "cm") and websocket.state.cm:
                try:
                    logger.info("WebSocket session ended - memory manager cleanup complete")
                except Exception as e:
                    logger.error(f"Error during enhanced tracking cleanup: {e}")
                    
                    # Persist any pending latency data
                    if hasattr(websocket.state, "cm") and websocket.state.cm:
                        try:
                            await websocket.state.cm.persist(websocket.app.state.redis)
                        except Exception as e:
                            logger.debug(f"Latency data persistence: {e}")
                    logger.info(f"Enhanced tracking cleanup completed for session {session_id}")
                except Exception as e:
                    logger.error(f"Error during enhanced tracking cleanup: {e}")

            # Release STT recognizer back to pool
            if hasattr(websocket.state, "stt_client") and websocket.state.stt_client:
                try:
                    websocket.state.stt_client.stop()
                    await websocket.app.state.stt_pool.release(websocket.state.stt_client)
                    logger.info(f"Released STT recognizer back to pool for call {call_connection_id}")
                except Exception as e:
                    logger.error(f"Error releasing STT recognizer: {e}", exc_info=True)

            # Stop and cleanup handler
            if handler:
                try:
                    await handler.stop()
                    logger.info("Media handler stopped successfully")
                except Exception as e:
                    logger.error(f"Error stopping media handler: {e}")
                    span.set_status(
                        Status(StatusCode.ERROR, f"Handler cleanup error: {e}")
                    )

                # Remove handler from registry
                if call_connection_id and call_connection_id in _active_handlers:
                    del _active_handlers[call_connection_id]
                    logger.debug(
                        f"Removed handler for call {call_connection_id} from registry"
                    )

            span.set_status(Status(StatusCode.OK))
            log_with_context(
                logger,
                "info",
                "WebSocket cleanup complete",
                operation="websocket_cleanup",
                call_connection_id=call_connection_id,
                session_id=session_id,
                api_version="v1",
            )

        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, f"Cleanup error: {e}"))
            logger.error(f"Error during cleanup: {e}")
