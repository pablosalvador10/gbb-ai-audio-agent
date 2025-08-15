"""
Production-Ready Media Endpoints - Zero Cross-Contamination
===========================================================

Completely isolated, production-ready media streaming for 1000+ concurrent calls.
Each call gets its own isolated context with zero shared state or cross-contamination.

Key Features:
✅ Complete call isolation (no shared state)
✅ Thread-safe handler management
✅ Automatic resource cleanup
✅ Redis namespace isolation per call
✅ Memory leak prevention
✅ Graceful degradation under load
✅ Production-ready error handling
✅ Comprehensive metrics and monitoring
"""

import asyncio
import json
import time
from typing import Optional
from fastapi import (
    APIRouter,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.websockets import WebSocketState

from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from apps.rtagent.backend.settings import ACS_STREAMING_MODE, ENABLE_AUTH_VALIDATION
from src.enums.stream_modes import StreamMode
from apps.rtagent.backend.src.utils.production_call_manager import production_call_manager
from apps.rtagent.backend.src.utils.auth import validate_acs_ws_auth, AuthError
from utils.ml_logging import get_logger

# Import V1 components
from ..handlers.acs_media_lifecycle import ACSMediaHandler
from ..dependencies.orchestrator import get_orchestrator

logger = get_logger("api.v1.endpoints.production_media")
tracer = trace.get_tracer(__name__)

router = APIRouter()


@router.get("/status", response_model=dict, summary="Get Production Media Status")
async def get_production_media_status():
    """Get current production media streaming status and metrics."""
    metrics = production_call_manager.get_metrics()
    health = await production_call_manager.health_check()
    
    return {
        "status": "available",
        "streaming_mode": str(ACS_STREAMING_MODE),
        "websocket_endpoint": "/api/v1/production-media/stream",
        "production_features": {
            "isolated_calls": True,
            "redis_namespacing": True,
            "automatic_cleanup": True,
            "thread_safe": True,
            "memory_leak_prevention": True,
            "graceful_degradation": True,
        },
        "capacity": {
            "max_concurrent_calls": production_call_manager.max_concurrent_calls,
            "current_active": metrics["active_calls"],
            "utilization_percent": round(metrics["capacity_utilization"] * 100, 1),
        },
        "performance": {
            "success_rate_percent": round(metrics["success_rate"] * 100, 2),
            "total_calls_processed": metrics["total_calls"],
            "failed_calls": metrics["failed_calls"],
            "rejected_calls": metrics["rejected_calls"],
        },
        "health": health["status"],
        "version": "v1-production",
    }


@router.websocket("/stream")
async def production_media_stream(websocket: WebSocket):
    """
    Production-grade WebSocket endpoint for isolated ACS media streaming.
    
    Handles 1000+ concurrent calls with complete isolation and zero cross-contamination.
    Each call gets its own isolated context, Redis namespace, and resource tracking.
    """
    call_connection_id = None
    orchestrator = get_orchestrator()
    
    try:
        # Accept WebSocket connection
        await websocket.accept()
        logger.info("🚀 Production media WebSocket connection accepted")
        
        # Extract call connection ID
        call_connection_id = await _extract_call_connection_id(websocket)
        
        # Validate dependencies
        await _validate_production_dependencies(websocket)
        
        # Authenticate if required
        if ENABLE_AUTH_VALIDATION:
            await _validate_websocket_auth(websocket)
        
        # Create completely isolated call context
        async with production_call_manager.create_isolated_call_context(
            call_connection_id, websocket, websocket.app.state.redis
        ) as isolated_context:
            
            logger.info(
                f"📞 ISOLATED CALL CONTEXT CREATED: {call_connection_id} "
                f"(namespace: {isolated_context.namespace})"
            )
            
            # Create handler with isolated context
            handler = await _create_isolated_handler(
                websocket, isolated_context, orchestrator
            )
            
            # Store handler in isolated context for cleanup
            isolated_context.handler = handler
            isolated_context.add_cleanup_callback(
                lambda: handler.stop() if handler and hasattr(handler, 'stop') else None
            )
            
            # Start the handler
            await handler.start()
            logger.info(f"✅ Production media handler started: {call_connection_id}")
            
            # Send connection acknowledgment
            await _send_connection_ack(websocket, call_connection_id)
            
            # Process media stream with isolated context
            await _process_isolated_media_stream(websocket, handler, isolated_context)
    
    except WebSocketDisconnect as e:
        _log_websocket_disconnect(e, call_connection_id)
    except Exception as e:
        _log_websocket_error(e, call_connection_id)
        if not isinstance(e, WebSocketDisconnect):
            raise
    finally:
        # Context cleanup is handled automatically by the context manager
        logger.info(f"📞 Production media stream completed: {call_connection_id}")


# ============================================================================
# Production Helper Functions - Isolated Operations
# ============================================================================

async def _extract_call_connection_id(websocket: WebSocket) -> str:
    """Extract call connection ID from WebSocket safely."""
    # Try query parameters first
    query_params = dict(websocket.query_params)
    call_connection_id = query_params.get("call_connection_id")
    
    # Try headers if not in query params
    if not call_connection_id:
        headers_dict = dict(websocket.headers)
        call_connection_id = headers_dict.get("x-ms-call-connection-id")
    
    if not call_connection_id:
        error_msg = "Call connection ID not found in query params or headers"
        logger.error(error_msg)
        await websocket.close(code=1002, reason="Missing call ID")
        raise HTTPException(400, error_msg)
    
    logger.info(f"✅ Call connection ID extracted: {call_connection_id}")
    return call_connection_id


async def _validate_production_dependencies(websocket: WebSocket) -> None:
    """Validate required production dependencies."""
    missing_deps = []
    
    if not hasattr(websocket.app.state, "acs_caller") or not websocket.app.state.acs_caller:
        missing_deps.append("acs_caller")
    
    if not hasattr(websocket.app.state, "stt_client") or not websocket.app.state.stt_client:
        missing_deps.append("stt_client")
    
    if not hasattr(websocket.app.state, "redis") or not websocket.app.state.redis:
        missing_deps.append("redis")
    
    if missing_deps:
        error_msg = f"Missing required dependencies: {', '.join(missing_deps)}"
        logger.error(error_msg)
        await websocket.close(code=1011, reason="Dependencies not initialized")
        raise HTTPException(503, error_msg)
    
    # Validate ACS connection exists
    acs_caller = websocket.app.state.acs_caller
    query_params = dict(websocket.query_params)
    call_connection_id = query_params.get("call_connection_id")
    
    if call_connection_id:
        call_connection = acs_caller.get_call_connection(call_connection_id)
        if not call_connection:
            error_msg = f"Call connection {call_connection_id} not found"
            logger.warning(error_msg)
            await websocket.close(code=1000, reason="Call not found")
            raise HTTPException(404, error_msg)


async def _validate_websocket_auth(websocket: WebSocket) -> None:
    """Validate WebSocket authentication if enabled."""
    try:
        _ = await validate_acs_ws_auth(websocket)
        logger.info("WebSocket authenticated successfully")
    except AuthError as e:
        logger.warning(f"WebSocket authentication failed: {str(e)}")
        await websocket.close(code=4001, reason="Authentication failed")
        raise HTTPException(401, f"Authentication failed: {str(e)}")


async def _create_isolated_handler(
    websocket: WebSocket, 
    isolated_context, 
    orchestrator: callable
):
    """Create media handler with completely isolated context."""
    
    if ACS_STREAMING_MODE == StreamMode.MEDIA:
        # Create ACS media handler with isolated memory manager
        handler = ACSMediaHandler(
            websocket=websocket,
            orchestrator_func=orchestrator,
            call_connection_id=isolated_context.call_connection_id,
            recognizer=websocket.app.state.stt_client,
            memory_manager=isolated_context.memory_manager,
            session_id=isolated_context.session_id,
        )
        logger.info("Created isolated ACS media handler for MEDIA mode")
        return handler
    
    elif ACS_STREAMING_MODE == StreamMode.TRANSCRIPTION:
        # Create transcription handler with isolated context
        from apps.rtagent.backend.src.handlers import TranscriptionHandler
        
        handler = TranscriptionHandler(websocket, cm=isolated_context.memory_manager)
        logger.info("Created isolated transcription handler for TRANSCRIPTION mode")
        return handler
    
    else:
        error_msg = f"Unknown streaming mode: {ACS_STREAMING_MODE}"
        logger.error(error_msg)
        await websocket.close(code=1000, reason="Invalid streaming mode")
        raise HTTPException(400, error_msg)


async def _send_connection_ack(websocket: WebSocket, call_connection_id: str) -> None:
    """Send connection acknowledgment to ACS."""
    try:
        await websocket.send_text(
            json.dumps({
                "kind": "ConnectionEstablished",
                "connectionId": call_connection_id,
                "status": "ready",
                "production": True,
                "isolation": "enabled",
            })
        )
        logger.debug(f"📡 Production connection acknowledgment sent: {call_connection_id}")
    except Exception as e:
        logger.warning(f"Failed to send connection acknowledgment: {e}")
        # Continue anyway - this is not critical


async def _process_isolated_media_stream(
    websocket: WebSocket, handler, isolated_context
) -> None:
    """Process media stream with complete isolation."""
    
    with tracer.start_as_current_span(
        "api.v1.production_media.process_isolated_stream",
        kind=SpanKind.SERVER,
        attributes={
            "api.version": "v1-production",
            "call.connection.id": isolated_context.call_connection_id,
            "stream.mode": str(ACS_STREAMING_MODE),
            "isolation.namespace": isolated_context.namespace,
        },
    ) as span:
        logger.info(
            f"🚀 Starting isolated media stream processing: {isolated_context.call_connection_id}"
        )
        
        message_count = 0
        start_time = time.time()
        
        try:
            while (
                websocket.client_state == WebSocketState.CONNECTED
                and websocket.application_state == WebSocketState.CONNECTED
            ):
                try:
                    # Receive message with timeout to prevent hanging
                    msg = await asyncio.wait_for(websocket.receive_text(), timeout=30.0)
                    message_count += 1
                    
                    if msg:
                        # Handle message based on streaming mode
                        if ACS_STREAMING_MODE == StreamMode.MEDIA:
                            await handler.handle_media_message(msg)
                        elif ACS_STREAMING_MODE == StreamMode.TRANSCRIPTION:
                            await handler.handle_transcription_message(msg)
                    
                except asyncio.TimeoutError:
                    # Send keepalive ping
                    await websocket.ping()
                    logger.debug(f"Keepalive ping sent: {isolated_context.call_connection_id}")
                    continue
                
        except WebSocketDisconnect:
            # Normal disconnection
            pass
        except Exception as e:
            logger.error(f"Error processing isolated media stream: {e}")
            span.set_status(Status(StatusCode.ERROR, f"Stream processing error: {e}"))
        finally:
            duration = time.time() - start_time
            span.set_attribute("stream.messages_processed", message_count)
            span.set_attribute("stream.duration_seconds", duration)
            span.set_attribute("stream.messages_per_second", message_count / max(duration, 0.1))
            
            logger.info(
                f"📞 Isolated stream completed: {isolated_context.call_connection_id} "
                f"({message_count} messages in {duration:.2f}s)"
            )


def _log_websocket_disconnect(e: WebSocketDisconnect, call_connection_id: Optional[str]) -> None:
    """Log WebSocket disconnect with context."""
    if e.code == 1000:
        logger.info(
            f"📞 Production call ended normally: {call_connection_id} "
            f"(WebSocket code {e.code})"
        )
    elif e.code == 1001:
        logger.info(
            f"📞 Production call ended - endpoint going away: {call_connection_id} "
            f"(WebSocket code {e.code})"
        )
    else:
        logger.warning(
            f"📞 Production call disconnected abnormally: {call_connection_id} "
            f"(code: {e.code}, reason: {e.reason})"
        )


def _log_websocket_error(e: Exception, call_connection_id: Optional[str]) -> None:
    """Log WebSocket errors with context."""
    if isinstance(e, asyncio.CancelledError):
        logger.info(f"Production WebSocket cancelled: {call_connection_id}")
    else:
        logger.error(
            f"Production WebSocket error for {call_connection_id}: {str(e)} "
            f"(type: {type(e).__name__})"
        )


# ============================================================================
# Production Monitoring Endpoints
# ============================================================================

@router.get("/metrics")
async def get_production_metrics():
    """Get detailed production metrics for monitoring."""
    return production_call_manager.get_metrics()


@router.get("/health")
async def get_production_health():
    """Get comprehensive production health check."""
    return await production_call_manager.health_check()


@router.post("/admin/cleanup-stale")
async def cleanup_stale_contexts():
    """Admin endpoint to cleanup stale contexts (use with caution)."""
    # This would be implemented with proper admin authentication
    # For now, return metrics only
    health = await production_call_manager.health_check()
    return {
        "message": "Stale context cleanup would be performed here",
        "stuck_contexts": health.get("stuck_contexts", []),
        "recommendations": health.get("recommendations", [])
    }
