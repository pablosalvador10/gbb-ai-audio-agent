"""
V1 Realtime API Endpoints - Enterprise Architecture
===================================================

Enhanced WebSocket endpoints for real-time communication with enterprise features.
Provides backward-compatible endpoints with enhanced observability and orchestrator support.

V1 Architecture Improvements:
- Comprehensive Swagger/OpenAPI documentation
- Advanced OpenTelemetry tracing and observability
- Pluggable orchestrator support for different conversation engines
- Enhanced session management with proper resource cleanup
- Production-ready error handling and recovery
- Clean separation of concerns with focused helper functions

Key V1 Features:
- Dashboard relay with advanced connection tracking
- Browser conversation with STT/TTS streaming
- Legacy compatibility endpoints for seamless migration
- Enhanced audio processing with interruption handling
- Comprehensive session state management
- Production-ready WebSocket handling

WebSocket Flow:
1. Accept connection and validate dependencies
2. Initialize session with proper state management
3. Process streaming audio/text with error handling
4. Route through pluggable orchestrator system
5. Stream responses with TTS and visual feedback
6. Clean up resources on disconnect/error
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Optional, Set
from datetime import datetime

from fastapi import (
    APIRouter,
    WebSocket,
    WebSocketDisconnect,
    Depends,
    HTTPException,
    Request,
    status,
)
from fastapi.websockets import WebSocketState
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

# Core application imports
from apps.rtagent.backend.settings import GREETING, ENABLE_AUTH_VALIDATION
from apps.rtagent.backend.src.helpers import check_for_stopwords, receive_and_filter
from src.tools.latency_tool import LatencyTool
from apps.rtagent.backend.src.orchestration.orchestrator import route_turn
from apps.rtagent.backend.src.ws_helpers.shared_ws import broadcast_message, send_tts_audio
from src.speech.speech_recognizer import StreamingSpeechRecognizerFromBytes
from src.postcall.push import build_and_flush
from src.stateful.state_managment import MemoManager
from utils.ml_logging import get_logger

# V1 components
from ..dependencies.orchestrator import get_orchestrator
from ..schemas.realtime import (
    RealtimeStatusResponse,
    DashboardConnectionResponse,
    ConversationSessionResponse,
)
from apps.rtagent.backend.src.utils.tracing import log_with_context
from apps.rtagent.backend.src.utils.auth import validate_acs_ws_auth, AuthError

logger = get_logger("api.v1.endpoints.realtime")
tracer = trace.get_tracer(__name__)

# Global registry for connection tracking
_active_dashboard_clients: Set[WebSocket] = set()
# Thread-safe session tracking - removed global dict
# Now using app.state.session_manager instead

router = APIRouter()


@router.get(
    "/status",
    response_model=RealtimeStatusResponse,
    summary="Get Realtime Service Status",
    description="""
    Get the current status of the realtime communication service.
    
    Returns information about:
    - Service availability and health
    - Supported protocols and features
    - Active connection counts
    - WebSocket endpoint configurations
    """,
    tags=["Realtime Status"],
    responses={
        200: {
            "description": "Service status retrieved successfully",
            "content": {
                "application/json": {
                    "example": {
                        "status": "available",
                        "websocket_endpoints": {
                            "dashboard_relay": "/api/v1/realtime/dashboard/relay",
                            "conversation": "/api/v1/realtime/conversation",
                        },
                        "features": {
                            "dashboard_broadcasting": True,
                            "conversation_streaming": True,
                            "orchestrator_support": True,
                            "session_management": True,
                        },
                        "active_connections": {
                            "dashboard_clients": 0,
                            "conversation_sessions": 0,
                        },
                        "version": "v1",
                    }
                }
            },
        }
    },
)
async def get_realtime_status(request: Request):
    """
    Retrieve comprehensive status and configuration of real-time communication services.

    This endpoint provides detailed information about WebSocket endpoints, active
    session counts, and service availability for dashboard relay and browser
    conversation capabilities within the voice agent system.

    :param request: FastAPI request object providing access to application state and session manager.
    :return: RealtimeStatusResponse containing service status, endpoints, and session information.
    :raises: None (endpoint designed to always return current service status).
    """
    # Get statistics from the unified session manager instead of session_statistics
    websocket_mgr = getattr(request.app.state, "websocket_manager", None)
    
    if websocket_mgr:
        # Get metrics from unified session manager
        session_metrics = await websocket_mgr.get_health_metrics()
        
        # Count dashboard connections
        dashboard_count = 0
        conversation_count = 0
        total_connections = session_metrics.get("local_connections", 0)
        
        # Try to get more detailed counts if available
        dashboard_count = len(_active_dashboard_clients)
        conversation_count = max(0, total_connections - dashboard_count)
        
    else:
        # Fallback to session statistics if unified manager not available
        session_stats = await request.app.state.session_statistics.get_statistics()
        dashboard_count = len(_active_dashboard_clients)
        conversation_count = session_stats["active_sessions"]["realtime"]
        total_connections = session_stats["active_sessions"]["total"]

    return RealtimeStatusResponse(
        status="available",
        websocket_endpoints={
            "dashboard_relay": "/api/v1/realtime/dashboard/relay",
            "conversation": "/api/v1/realtime/conversation",
            "legacy_relay": "/api/v1/realtime/ws/relay",
            "legacy_conversation": "/api/v1/realtime/ws/conversation",
        },
        features={
            "dashboard_broadcasting": True,
            "conversation_streaming": True,
            "orchestrator_support": True,
            "session_management": True,
            "audio_interruption": True,
            "legacy_compatibility": True,
        },
        active_connections={
            "dashboard_clients": dashboard_count,
            "conversation_sessions": conversation_count,
            "total_active_sessions": total_connections,
            "total_disconnected": session_metrics.get("total_connections_removed", 0) if websocket_mgr else session_stats.get("total_disconnected", 0),
        },
        protocols_supported=["WebSocket"],
        version="v1",
    )


@router.websocket("/dashboard/relay")
async def dashboard_relay_endpoint(websocket: WebSocket, session_id: Optional[str] = None):
    """Enhanced dashboard relay WebSocket endpoint with session-specific monitoring.

    :param websocket: WebSocket connection from dashboard client
    :param session_id: Optional session ID to monitor specific session (secure mode)
    :return: None
    :raises WebSocketDisconnect: When client disconnects from WebSocket
    :raises Exception: For any other errors during connection processing
    """
    client_id = None
    try:
        # Accept connection with tracing
        await websocket.accept()
        client_id = str(uuid.uuid4())[:8]

        with tracer.start_as_current_span(
            "api.v1.realtime.dashboard_relay_connect",
            kind=SpanKind.SERVER,
            attributes={
                "api.version": "v1",
                "realtime.client_id": client_id,
                "realtime.endpoint": "dashboard_relay",
                "realtime.session_id": session_id,
                "network.protocol.name": "websocket",
            },
        ) as connect_span:
            # Validate dependencies
            await _validate_realtime_dependencies(websocket)

            # Determine monitoring mode and session assignment
            if session_id:
                # Session-specific monitoring (secure mode)
                dashboard_session_id = session_id
                monitoring_mode = "session-specific"
            else:
                # Global monitoring (legacy mode - will show all messages)
                dashboard_session_id = "global-dashboard"
                monitoring_mode = "global"
                logger.warning(
                    f"Dashboard client {client_id} using GLOBAL monitoring mode. "
                    "This may cause cross-user data leakage in production."
                )

            # Session is already registered with the WebSocket manager above

            # Legacy global registry for backward compatibility
            if websocket not in _active_dashboard_clients:
                _active_dashboard_clients.add(websocket)
                connect_span.set_attribute(
                    "dashboard.clients.total", len(_active_dashboard_clients)
                )

            # Store client info using production WebSocket manager
            await websocket.app.state.websocket_manager.add_connection(
                websocket=websocket,
                session_id=dashboard_session_id,
                connection_type="dashboard",
                metadata={
                    "client_id": client_id,
                    "monitoring_mode": monitoring_mode,
                    "target_session_id": session_id if session_id else None
                }
            )

            connect_span.set_attribute("dashboard.monitoring_mode", monitoring_mode)
            connect_span.set_status(Status(StatusCode.OK))
            log_with_context(
                logger,
                "info",
                "Dashboard client connected successfully",
                operation="dashboard_connect",
                client_id=client_id,
                session_id=dashboard_session_id,
                monitoring_mode=monitoring_mode,
                total_clients=len(_active_dashboard_clients),
                api_version="v1",
            )

        # Process dashboard messages
        await _process_dashboard_messages(websocket, client_id)

    except WebSocketDisconnect as e:
        _log_dashboard_disconnect(e, client_id)
    except Exception as e:
        _log_dashboard_error(e, client_id)
        raise
    finally:
        await _cleanup_dashboard_connection(websocket, client_id)


@router.websocket("/conversation")
async def browser_conversation_endpoint(
    websocket: WebSocket, orchestrator: Optional[callable] = Depends(get_orchestrator)
):
    """Enhanced browser conversation WebSocket endpoint with orchestrator injection.

    :param websocket: WebSocket connection from browser client
    :param orchestrator: Injected conversation orchestrator (optional)
    :return: None
    :raises WebSocketDisconnect: When client disconnects from WebSocket
    :raises HTTPException: For authentication or dependency validation failures
    :raises Exception: For any other errors during conversation processing
    """
    session_id = None
    memory_manager = None

    try:
        # Accept connection and initialize session
        await websocket.accept()

        # Generate collision-resistant session ID with production normalization
        if websocket.headers.get("x-ms-call-connection-id"):
            # For ACS calls, use the full call-connection-id (already unique)
            raw_session_id = websocket.headers.get("x-ms-call-connection-id")
        else:
            # For realtime calls, use full UUID4 to prevent collisions
            raw_session_id = str(uuid.uuid4())
        
        # Normalize session ID using session manager
        session_mgr = getattr(websocket.app.state, "session_manager", None)
        if session_mgr:
            session_id = session_mgr.normalize_session_id(raw_session_id)
        else:
            session_id = raw_session_id
            logger.warning("Session manager not available, using raw session ID")

        with tracer.start_as_current_span(
            "api.v1.realtime.conversation_connect",
            kind=SpanKind.SERVER,
            attributes={
                "api.version": "v1",
                "realtime.session_id": session_id,
                "realtime.endpoint": "conversation",
                "network.protocol.name": "websocket",
                "orchestrator.name": getattr(orchestrator, "name", "unknown")
                if orchestrator
                else "default",
            },
        ) as connect_span:
            # Validate dependencies
            await _validate_realtime_dependencies(websocket)

            # Authenticate if required
            if ENABLE_AUTH_VALIDATION:
                await _validate_realtime_auth(websocket)

            # Initialize conversation session
            memory_manager = await _initialize_conversation_session(
                websocket, session_id, orchestrator
            )

            # Register with production WebSocket manager for unified tracking
            await websocket.app.state.websocket_manager.add_connection(
                websocket=websocket,
                session_id=session_id,
                connection_type="realtime_conversation",
                metadata={
                    "raw_session_id": raw_session_id,
                    "memory_manager_id": getattr(memory_manager, "session_id", None),
                    "orchestrator": getattr(orchestrator, "name", "unknown") if orchestrator else "default"
                }
            )

            # Register session with session statistics for monitoring
            await websocket.app.state.session_statistics.add_realtime_session(
                session_id, memory_manager, websocket
            )

            session_stats = await websocket.app.state.session_statistics.get_statistics()
            connect_span.set_attribute("conversation.sessions.active", session_stats["active_sessions"]["realtime"])
            connect_span.set_attribute("conversation.sessions.total_active", session_stats["active_sessions"]["total"])
            connect_span.set_status(Status(StatusCode.OK))

            log_with_context(
                logger,
                "info",
                "Conversation session initialized successfully",
                operation="conversation_connect",
                session_id=session_id,
                total_sessions=session_stats["active_sessions"]["total"],
                realtime_sessions=session_stats["active_sessions"]["realtime"],
                api_version="v1",
            )

        # Process conversation messages
        await _process_conversation_messages(
            websocket, session_id, memory_manager, orchestrator
        )

    except WebSocketDisconnect as e:
        _log_conversation_disconnect(e, session_id)
    except Exception as e:
        _log_conversation_error(e, session_id)
        raise
    finally:
        await _cleanup_conversation_session(websocket, session_id, memory_manager)


# ============================================================================
# Legacy Compatibility Endpoints
# ============================================================================


@router.websocket("/ws/relay")
async def legacy_dashboard_relay(websocket: WebSocket):
    """Legacy dashboard relay endpoint for backward compatibility.

    :param websocket: WebSocket connection from legacy dashboard client
    :return: None
    :raises WebSocketDisconnect: When client disconnects from WebSocket
    :raises Exception: For any other errors during connection processing
    """
    with tracer.start_as_current_span(
        "api.v1.realtime.legacy_dashboard_relay",
        attributes={"api.version": "v1", "endpoint.legacy": True},
    ):
        logger.info(
            "Legacy dashboard relay endpoint accessed - consider migrating to /api/v1/realtime/dashboard/relay"
        )
        await dashboard_relay_endpoint(websocket)


@router.websocket("/ws/conversation")
async def legacy_browser_conversation(
    websocket: WebSocket, orchestrator: Optional[callable] = Depends(get_orchestrator)
):
    """Legacy browser conversation endpoint for backward compatibility.

    :param websocket: WebSocket connection from legacy browser client
    :param orchestrator: Injected conversation orchestrator (optional)
    :return: None
    :raises WebSocketDisconnect: When client disconnects from WebSocket
    :raises Exception: For any other errors during conversation processing
    """
    with tracer.start_as_current_span(
        "api.v1.realtime.legacy_conversation",
        attributes={"api.version": "v1", "endpoint.legacy": True},
    ):
        logger.info(
            "Legacy conversation endpoint accessed - consider migrating to /api/v1/realtime/conversation"
        )
        await browser_conversation_endpoint(websocket, orchestrator)


# ============================================================================
# V1 Architecture Helper Functions
# ============================================================================


async def _validate_realtime_dependencies(websocket: WebSocket) -> None:
    """Validate required app state dependencies for realtime endpoints.

    :param websocket: WebSocket connection containing app state
    :return: None
    :raises HTTPException: If required dependencies are not initialized
    """
    # Check TTS pool
    if not hasattr(websocket.app.state, "tts_pool") or not websocket.app.state.tts_pool:
        logger.error("TTS pool not initialized")
        await websocket.close(code=1011, reason="TTS pool not initialized")
        raise HTTPException(503, "TTS pool not initialized")

    # Check STT pool
    if not hasattr(websocket.app.state, "stt_pool") or not websocket.app.state.stt_pool:
        logger.error("STT pool not initialized")
        await websocket.close(code=1011, reason="STT pool not initialized")
        raise HTTPException(503, "STT pool not initialized")

    # STT no longer validated globally here; each conversation WebSocket gets its own instance

    # Check Redis for session management
    if not hasattr(websocket.app.state, "redis") or not websocket.app.state.redis:
        logger.error("Redis client not initialized")
        await websocket.close(code=1011, reason="Redis not initialized")
        raise HTTPException(503, "Redis client not initialized")


async def _validate_realtime_auth(websocket: WebSocket) -> None:
    """Validate WebSocket authentication for realtime endpoints.

    :param websocket: WebSocket connection to authenticate
    :return: None
    :raises HTTPException: If authentication fails
    :raises AuthError: If WebSocket authentication validation fails
    """
    try:
        _ = await validate_acs_ws_auth(websocket)
        logger.info("Realtime WebSocket authenticated successfully")
    except AuthError as e:
        logger.warning(f"Realtime WebSocket authentication failed: {str(e)}")
        await websocket.close(code=4001, reason="Authentication failed")
        raise HTTPException(401, f"Authentication failed: {str(e)}")


async def _initialize_conversation_session(
    websocket: WebSocket, session_id: str, orchestrator: Optional[callable]
) -> MemoManager:
    """Initialize conversation session with proper state management.

    :param websocket: WebSocket connection for the conversation
    :param session_id: Unique identifier for the conversation session
    :param orchestrator: Optional orchestrator for conversation routing
    :return: Initialized MemoManager instance for conversation state
    :raises Exception: If session initialization fails
    """
    redis_mgr = websocket.app.state.redis
    memory_manager = MemoManager.from_redis(session_id, redis_mgr)

    # Acquire per-connection TTS synthesizer from pool
    websocket.state.tts_client = await websocket.app.state.tts_pool.acquire()
    logger.info(f"Acquired TTS synthesizer from pool for session {session_id}")

    # Set up WebSocket state
    websocket.state.cm = memory_manager
    websocket.state.session_id = session_id
    websocket.state.lt = LatencyTool(memory_manager)
    websocket.state.is_synthesizing = False
    websocket.state.user_buffer = ""

    # Send greeting message
    await websocket.send_text(json.dumps({"type": "status", "message": GREETING}))

    # Add greeting to conversation history
    auth_agent = websocket.app.state.auth_agent
    memory_manager.append_to_history(auth_agent.name, "assistant", GREETING)

    # Send TTS audio greeting
    await send_tts_audio(GREETING, websocket, latency_tool=websocket.state.lt)

    # Persist initial state to Redis
    await memory_manager.persist_to_redis_async(redis_mgr)

    # Set up STT callbacks
    def on_partial(txt: str, lang: str):
        logger.info(f"🗣️ User (partial) in {lang}: {txt}")
        if websocket.state.is_synthesizing:
            try:
                # Stop per-connection TTS instead of global
                if (
                    hasattr(websocket.state, "tts_client")
                    and websocket.state.tts_client
                ):
                    websocket.state.tts_client.stop_speaking()
                websocket.state.is_synthesizing = False
                logger.info("🛑 TTS interrupted due to user speech (server VAD)")
            except Exception as e:
                logger.error(f"Error stopping TTS: {e}", exc_info=True)
        asyncio.create_task(
            websocket.send_text(
                json.dumps({"type": "assistant_streaming", "content": txt})
            )
        )

    def on_final(txt: str, lang: str):
        logger.info(f"🧾 User (final) in {lang}: {txt}")
        websocket.state.user_buffer += txt.strip() + "\n"

    # Acquire per‑connection speech recognizer from pool
    websocket.state.stt_client = await websocket.app.state.stt_pool.acquire()
    websocket.state.stt_client.set_partial_result_callback(on_partial)
    websocket.state.stt_client.set_final_result_callback(on_final)
    websocket.state.stt_client.start()

    logger.info(f"STT recognizer started for session {session_id}")
    return memory_manager


async def _process_dashboard_messages(websocket: WebSocket, client_id: str) -> None:
    """Process incoming dashboard relay messages.

    :param websocket: WebSocket connection for dashboard client
    :param client_id: Unique identifier for the dashboard client
    :return: None
    :raises WebSocketDisconnect: When client disconnects normally
    :raises Exception: For any other errors during message processing
    """
    with tracer.start_as_current_span(
        "api.v1.realtime.process_dashboard_messages",
        attributes={"client_id": client_id},
    ):
        try:
            while (
                websocket.client_state == WebSocketState.CONNECTED
                and websocket.application_state == WebSocketState.CONNECTED
            ):
                # Keep connection alive and process any ping/pong messages
                await websocket.receive_text()

        except WebSocketDisconnect:
            # Normal disconnect - handled in the calling function
            raise
        except Exception as e:
            logger.error(
                f"Error processing dashboard messages for client {client_id}: {e}"
            )
            raise


async def _process_conversation_messages(
    websocket: WebSocket,
    session_id: str,
    memory_manager: MemoManager,
    orchestrator: Optional[callable],
) -> None:
    """Process incoming conversation messages with enhanced error handling.

    :param websocket: WebSocket connection for conversation client
    :param session_id: Unique identifier for the conversation session
    :param memory_manager: MemoManager instance for conversation state
    :param orchestrator: Optional orchestrator for conversation routing
    :return: None
    :raises WebSocketDisconnect: When client disconnects normally
    :raises Exception: For any other errors during message processing
    """
    with tracer.start_as_current_span(
        "api.v1.realtime.process_conversation_messages",
        attributes={"session_id": session_id},
    ) as span:
        try:
            message_count = 0
            while (
                websocket.client_state == WebSocketState.CONNECTED
                and websocket.application_state == WebSocketState.CONNECTED
            ):
                msg = await websocket.receive()
                message_count += 1

                # Handle audio bytes
                if (
                    msg.get("type") == "websocket.receive"
                    and msg.get("bytes") is not None
                ):
                    websocket.state.stt_client.write_bytes(msg["bytes"])

                    # Process accumulated user buffer
                    if websocket.state.user_buffer.strip():
                        prompt = websocket.state.user_buffer.strip()
                        websocket.state.user_buffer = ""

                        # Send user message to frontend immediately
                        await websocket.send_text(
                            json.dumps({"sender": "User", "message": prompt})
                        )

                        # Check for stopwords
                        if check_for_stopwords(prompt):
                            goodbye = "Thank you for using our service. Goodbye."
                            await websocket.send_text(
                                json.dumps({"type": "exit", "message": goodbye})
                            )
                            await send_tts_audio(
                                goodbye, websocket, latency_tool=websocket.state.lt
                            )
                            break

                        # Route through orchestrator
                        await route_turn(
                            memory_manager, prompt, websocket, is_acs=False
                        )

                # Handle disconnect
                elif msg.get("type") == "websocket.disconnect":
                    break

            span.set_attribute("messages.processed", message_count)
            span.set_status(Status(StatusCode.OK))

        except WebSocketDisconnect:
            span.set_status(Status(StatusCode.OK, "Normal disconnect"))
            raise
        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, f"Message processing error: {e}"))
            logger.error(
                f"Error processing conversation messages for session {session_id}: {e}"
            )
            raise


def _log_dashboard_disconnect(e: WebSocketDisconnect, client_id: Optional[str]) -> None:
    """Log dashboard client disconnection.

    :param e: WebSocketDisconnect exception containing disconnect details
    :param client_id: Optional unique identifier for the dashboard client
    :return: None
    :raises: None
    """
    if e.code == 1000:
        log_with_context(
            logger,
            "info",
            "Dashboard client disconnected normally",
            operation="dashboard_disconnect",
            client_id=client_id,
            disconnect_code=e.code,
            api_version="v1",
        )
    else:
        log_with_context(
            logger,
            "warning",
            "Dashboard client disconnected abnormally",
            operation="dashboard_disconnect",
            client_id=client_id,
            disconnect_code=e.code,
            reason=e.reason,
            api_version="v1",
        )


def _log_dashboard_error(e: Exception, client_id: Optional[str]) -> None:
    """Log dashboard client errors.

    :param e: Exception that occurred during dashboard operation
    :param client_id: Optional unique identifier for the dashboard client
    :return: None
    :raises: None
    """
    log_with_context(
        logger,
        "error",
        "Dashboard client error",
        operation="dashboard_error",
        client_id=client_id,
        error=str(e),
        error_type=type(e).__name__,
        api_version="v1",
    )


def _log_conversation_disconnect(
    e: WebSocketDisconnect, session_id: Optional[str]
) -> None:
    """Log conversation session disconnection.

    :param e: WebSocketDisconnect exception containing disconnect details
    :param session_id: Optional unique identifier for the conversation session
    :return: None
    :raises: None
    """
    if e.code == 1000:
        log_with_context(
            logger,
            "info",
            "Conversation session ended normally",
            operation="conversation_disconnect",
            session_id=session_id,
            disconnect_code=e.code,
            api_version="v1",
        )
    else:
        log_with_context(
            logger,
            "warning",
            "Conversation session ended abnormally",
            operation="conversation_disconnect",
            session_id=session_id,
            disconnect_code=e.code,
            reason=e.reason,
            api_version="v1",
        )


def _log_conversation_error(e: Exception, session_id: Optional[str]) -> None:
    """Log conversation session errors.

    :param e: Exception that occurred during conversation operation
    :param session_id: Optional unique identifier for the conversation session
    :return: None
    :raises: None
    """
    log_with_context(
        logger,
        "error",
        "Conversation session error",
        operation="conversation_error",
        session_id=session_id,
        error=str(e),
        error_type=type(e).__name__,
        api_version="v1",
    )


async def _cleanup_dashboard_connection(
    websocket: WebSocket, client_id: Optional[str]
) -> None:
    """Clean up dashboard connection resources.

    :param websocket: WebSocket connection to clean up
    :param client_id: Optional unique identifier for the dashboard client
    :return: None
    :raises Exception: If cleanup operations fail (logged but not re-raised)
    """
    with tracer.start_as_current_span(
        "api.v1.realtime.cleanup_dashboard", attributes={"client_id": client_id}
    ) as span:
        try:
            # Session cleanup is automatically handled by websocket_manager.remove_connection() above

            # Remove from global registry
            if websocket in _active_dashboard_clients:
                _active_dashboard_clients.remove(websocket)
                logger.info(
                    f"Dashboard client {client_id} removed. Remaining clients: {len(_active_dashboard_clients)}"
                )

            # Remove from production WebSocket manager
            await websocket.app.state.websocket_manager.remove_connection(websocket)

            # Close WebSocket if still connected
            if (
                websocket.client_state == WebSocketState.CONNECTED
                and websocket.application_state == WebSocketState.CONNECTED
            ):
                await websocket.close()

            span.set_status(Status(StatusCode.OK))
            log_with_context(
                logger,
                "info",
                "Dashboard connection cleanup complete",
                operation="dashboard_cleanup",
                client_id=client_id,
                api_version="v1",
            )

        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, f"Cleanup error: {e}"))
            logger.error(f"Error during dashboard cleanup: {e}")


async def _cleanup_conversation_session(
    websocket: WebSocket,
    session_id: Optional[str],
    memory_manager: Optional[MemoManager],
) -> None:
    """Clean up conversation session resources.

    :param websocket: WebSocket connection to clean up
    :param session_id: Optional unique identifier for the conversation session
    :param memory_manager: Optional MemoManager instance to persist
    :return: None
    :raises Exception: If cleanup operations fail (logged but not re-raised)
    """
    with tracer.start_as_current_span(
        "api.v1.realtime.cleanup_conversation", attributes={"session_id": session_id}
    ) as span:
        try:
            # Stop and release per-connection TTS synthesizer back to pool
            if hasattr(websocket.state, "tts_client") and websocket.state.tts_client:
                try:
                    websocket.state.tts_client.stop_speaking()
                    await websocket.app.state.tts_pool.release(
                        websocket.state.tts_client
                    )
                    logger.info(
                        f"Released TTS synthesizer back to pool for session {session_id}"
                    )
                except Exception as e:
                    logger.error(f"Error releasing TTS synthesizer: {e}", exc_info=True)

            # Stop and release per-connection STT recognizer back to pool
            if hasattr(websocket.state, "stt_client") and websocket.state.stt_client:
                try:
                    websocket.state.stt_client.stop()
                    await websocket.app.state.stt_pool.release(
                        websocket.state.stt_client
                    )
                    logger.info(
                        f"Released STT recognizer back to pool for session {session_id}"
                    )
                except Exception as e:
                    logger.error(f"Error releasing STT recognizer: {e}", exc_info=True)

            # Remove from session registry thread-safely
            if session_id:
                # Remove from session statistics
                removed_stats = await websocket.app.state.session_statistics.remove_realtime_session(
                    session_id
                )
                
                if removed_stats:
                    session_stats = await websocket.app.state.session_statistics.get_statistics()
                    logger.info(
                        f"Conversation session {session_id} removed. "
                        f"Active realtime sessions: {session_stats['active_sessions']['realtime']}, "
                        f"Total disconnected: {session_stats['total_disconnected']}"
                    )

            # Remove from production WebSocket manager
            await websocket.app.state.websocket_manager.remove_connection(websocket)

            # Close WebSocket if still connected
            if (
                websocket.client_state == WebSocketState.CONNECTED
                and websocket.application_state == WebSocketState.CONNECTED
            ):
                await websocket.close()

            # Persist analytics if possible
            if memory_manager and hasattr(websocket.app.state, "cosmos"):
                try:
                    build_and_flush(memory_manager, websocket.app.state.cosmos)
                except Exception as e:
                    logger.error(f"Error persisting analytics: {e}", exc_info=True)

            span.set_status(Status(StatusCode.OK))
            log_with_context(
                logger,
                "info",
                "Conversation session cleanup complete",
                operation="conversation_cleanup",
                session_id=session_id,
                api_version="v1",
            )

        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, f"Cleanup error: {e}"))
            logger.error(f"Error during conversation cleanup: {e}")
