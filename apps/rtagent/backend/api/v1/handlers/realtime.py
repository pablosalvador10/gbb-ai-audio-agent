"""
V1 Realtime Handler
===================

Real-time communication handler for V1 API with clean tracing and simplified logging.
"""

from __future__ import annotations

import asyncio
import json
import uuid
import time
from typing import Optional, Dict, Any
from datetime import datetime

from fastapi import WebSocket, WebSocketDisconnect
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from apps.rtagent.backend.settings import GREETING
from apps.rtagent.backend.src.helpers import check_for_stopwords, receive_and_filter
from src.tools.latency_tool import LatencyTool
from apps.rtagent.backend.src.orchestration.orchestrator import route_turn
from apps.rtagent.backend.src.ws_helpers.shared_ws import broadcast_message, send_tts_audio
from src.postcall.push import build_and_flush
from src.stateful.state_managment import MemoManager
from utils.ml_logging import get_logger

# V1 API specific imports
from apps.rtagent.backend.src.utils.tracing import (
    trace_acs_operation,
    trace_acs_dependency,
)

logger = get_logger("v1.api.handlers.realtime")
tracer = trace.get_tracer(__name__)


class V1RealtimeHandler:
    """
    Real-time communication handler for V1 API.

    Features:
    - Dashboard relay broadcasting
    - Browser conversation handling with orchestrator support
    - Audio streaming with latency optimization
    - Clean tracing and simplified logging
    """

    def __init__(self, orchestrator: Optional[callable] = None):
        """
        Initialize V1 realtime handler.

        :param orchestrator: Optional orchestrator for conversation processing
        :type orchestrator: Optional[callable]
        """
        self.orchestrator = orchestrator
        self.logger = get_logger("api.v1.handlers.realtime")

    async def handle_dashboard_relay(self, websocket: WebSocket) -> None:
        """
        Handle dashboard relay WebSocket connections.

        :param websocket: WebSocket connection from dashboard client
        :type websocket: WebSocket
        :raises WebSocketDisconnect: When client disconnects
        :raises Exception: When connection handling fails
        """
        with trace_acs_operation(
            tracer,
            logger,
            "dashboard_relay",
        ) as op:
            clients: set[
                WebSocket
            ] = await websocket.app.state.websocket_manager.get_clients_snapshot()
            client_id = str(uuid.uuid4())[:8]

            op.log_info(f"Dashboard client connecting: {client_id}")

            try:
                if websocket not in clients:
                    await websocket.accept()
                    clients.add(websocket)
                    op.log_info(
                        f"Dashboard client connected: {client_id} (total: {len(clients)})"
                    )

                # Keep connection alive with ping/pong
                while True:
                    try:
                        # Receive any message to keep connection alive
                        await websocket.receive_text()
                    except WebSocketDisconnect:
                        break
                    except Exception as e:
                        logger.warning(f"Dashboard relay error: {e}")
                        break

            except WebSocketDisconnect:
                op.log_info(f"Dashboard client disconnected normally: {client_id}")
            except Exception as e:
                op.set_error(f"Dashboard relay error: {e}")
            finally:
                # Cleanup
                if websocket in clients:
                    clients.remove(websocket)

                try:
                    if (
                        websocket.application_state.name == "CONNECTED"
                        and websocket.client_state.name
                        not in ("DISCONNECTED", "CLOSED")
                    ):
                        await websocket.close()
                except Exception as e:
                    logger.warning(f"Error closing dashboard connection: {e}")

                op.log_info(
                    f"Dashboard client cleanup completed: {client_id} (remaining: {len(clients)})"
                )

    async def handle_browser_conversation(
        self, websocket: WebSocket, orchestrator: Optional[callable] = None
    ) -> None:
        """
        Handle browser conversation WebSocket with orchestrator support.

        :param websocket: WebSocket connection from browser client
        :type websocket: WebSocket
        :param orchestrator: Optional orchestrator for conversation processing
        :type orchestrator: Optional[callable]
        :raises WebSocketDisconnect: When client disconnects
        :raises Exception: When conversation handling fails
        """
        # Use provided orchestrator or fallback to instance orchestrator
        active_orchestrator = orchestrator or self.orchestrator
        orchestrator_name = (
            getattr(active_orchestrator, "name", "unknown")
            if active_orchestrator
            else "legacy"
        )

        session_id = None
        cm = None

        with trace_acs_operation(
            tracer, logger, "browser_conversation", orchestrator_name=orchestrator_name
        ) as op:
            try:
                await websocket.accept()

                # Generate session ID
                session_id = (
                    websocket.headers.get("x-ms-call-connection-id")
                    or uuid.uuid4().hex[:8]
                )

                op.log_info(
                    f"Browser conversation session started: {session_id} with {orchestrator_name}"
                )

                # Initialize session state and dependencies
                redis_mgr = websocket.app.state.redis
                cm = MemoManager.from_redis(session_id, redis_mgr)

                # Enhanced state initialization with V1 metadata
                websocket.state.cm = cm
                websocket.state.session_id = session_id
                websocket.state.lt = LatencyTool(cm)
                websocket.state.is_synthesizing = False
                websocket.state.user_buffer = ""
                websocket.state.orchestrator_name = orchestrator_name
                websocket.state.api_version = "v1"

                # Store orchestrator metadata in conversation memory
                cm.update_context("api_version", "v1")
                cm.update_context("orchestrator_name", orchestrator_name)
                cm.update_context("v1_features_enabled", True)

                if active_orchestrator:
                    cm.update_context(
                        "orchestrator_type", type(active_orchestrator).__name__
                    )
                    if hasattr(active_orchestrator, "get_config"):
                        cm.update_context(
                            "orchestrator_config", active_orchestrator.get_config()
                        )

                # Send initial greeting
                await websocket.send_text(
                    json.dumps(
                        {
                            "type": "status",
                            "message": GREETING,
                            "session_id": session_id,
                            "orchestrator": orchestrator_name,
                            "api_version": "v1",
                        }
                    )
                )

                # Initialize conversation with auth agent
                auth_agent = websocket.app.state.auth_agent
                cm.append_to_history(auth_agent.name, "assistant", GREETING)

                # Broadcast greeting to dashboard with Auth Agent label
                websocket_mgr = getattr(websocket.app.state, "websocket_manager", None)
                if websocket_mgr:
                    sent_count = await websocket_mgr.broadcast_to_session(
                        session_id, GREETING
                    )
                    logger.debug(f"WebSocket auth greeting broadcast sent to {sent_count} clients in session {session_id}")
                else:
                    logger.warning("Production session manager not available for auth greeting broadcast")

                # Send greeting audio
                with trace_acs_dependency(
                    tracer,
                    logger,
                    "tts_service",
                    "send_greeting",
                    session_id=session_id,
                ) as dep_op:
                    await send_tts_audio(
                        GREETING, websocket, latency_tool=websocket.state.lt
                    )

                await cm.persist_to_redis_async(redis_mgr)

                # Set up STT callbacks
                def on_partial(txt: str, lang: str):
                    logger.info(
                        f"🗣️ User (partial) in {lang}: {txt} [session: {session_id}]"
                    )

                    # Interruption handling
                    if websocket.state.is_synthesizing:
                        try:
                            # Stop per-connection TTS synthesizer if available
                            if (
                                hasattr(websocket.state, "tts_client")
                                and websocket.state.tts_client
                            ):
                                websocket.state.tts_client.stop_speaking()
                            websocket.state.is_synthesizing = False
                            logger.info(
                                f"🛑 TTS interrupted due to user speech [session: {session_id}]"
                            )
                        except Exception as e:
                            logger.error(
                                f"Error stopping TTS [session: {session_id}]: {e}"
                            )

                    # Send partial result
                    asyncio.create_task(
                        websocket.send_text(
                            json.dumps(
                                {
                                    "type": "assistant_streaming",
                                    "content": txt,
                                    "session_id": session_id,
                                    "language": lang,
                                    "api_version": "v1",
                                }
                            )
                        )
                    )

                def on_final(txt: str, lang: str):
                    logger.info(
                        f"🧾 User (final) in {lang}: {txt} [session: {session_id}]"
                    )
                    websocket.state.user_buffer += txt.strip() + "\n"

                # Configure STT client
                websocket.app.state.stt_client.set_partial_result_callback(on_partial)
                websocket.app.state.stt_client.set_final_result_callback(on_final)
                websocket.app.state.stt_client.start()

                logger.info(
                    f"STT recognizer started for V1 session {session_id} with orchestrator {orchestrator_name}"
                )

                # Main message processing loop
                while True:
                    msg = await websocket.receive()

                    # Handle audio bytes
                    if (
                        msg.get("type") == "websocket.receive"
                        and msg.get("bytes") is not None
                    ):
                        websocket.app.state.stt_client.write_bytes(msg["bytes"])

                        if websocket.state.user_buffer.strip():
                            prompt = websocket.state.user_buffer.strip()
                            websocket.state.user_buffer = ""

                            # Send user message to frontend
                            await websocket.send_text(
                                json.dumps(
                                    {
                                        "sender": "User",
                                        "message": prompt,
                                        "session_id": session_id,
                                        "timestamp": datetime.utcnow().isoformat()
                                        + "Z",
                                        "api_version": "v1",
                                    }
                                )
                            )

                            # Check for stop words
                            if check_for_stopwords(prompt):
                                goodbye = "Thank you for using our service. Goodbye."
                                await websocket.send_text(
                                    json.dumps(
                                        {
                                            "type": "exit",
                                            "message": goodbye,
                                            "session_id": session_id,
                                            "api_version": "v1",
                                        }
                                    )
                                )

                                # Send farewell audio
                                with trace_acs_dependency(
                                    tracer,
                                    logger,
                                    "tts_service",
                                    "send_farewell",
                                    session_id=session_id,
                                ) as dep_op:
                                    await send_tts_audio(
                                        goodbye,
                                        websocket,
                                        latency_tool=websocket.state.lt,
                                    )
                                break

                            # Route to conversation orchestrator
                            with trace_acs_dependency(
                                tracer,
                                logger,
                                "conversation_orchestrator",
                                "route_turn",
                                session_id=session_id,
                                orchestrator_name=orchestrator_name,
                            ) as dep_op:
                                dep_op.log_info(
                                    f"Routing user prompt: {prompt[:50]}..."
                                )
                                await route_turn(cm, prompt, websocket, is_acs=False)

                        continue

                    # Handle disconnect
                    if msg.get("type") == "websocket.disconnect":
                        break

            except WebSocketDisconnect:
                op.log_info(
                    f"Browser client disconnected normally: session {session_id}"
                )
            except Exception as e:
                op.set_error(f"Browser conversation error: {e}")
            finally:
                # Cleanup
                with trace_acs_dependency(
                    tracer,
                    logger,
                    "cleanup_service",
                    "session_cleanup",
                    session_id=session_id,
                ) as cleanup_op:
                    # Stop TTS
                    try:
                        # Stop per-connection TTS synthesizer if available
                        if (
                            hasattr(websocket.state, "tts_client")
                            and websocket.state.tts_client
                        ):
                            websocket.state.tts_client.stop_speaking()
                    except Exception as e:
                        logger.warning(f"Error stopping TTS during cleanup: {e}")

                    # Close WebSocket
                    try:
                        if (
                            websocket.application_state.name == "CONNECTED"
                            and websocket.client_state.name
                            not in ("DISCONNECTED", "CLOSED")
                        ):
                            await websocket.close()
                    except Exception as e:
                        logger.warning(f"WebSocket close error: {e}")

                    # Persist conversation analytics
                    try:
                        if (
                            cm
                            and hasattr(websocket.app.state, "cosmos")
                            and websocket.app.state.cosmos
                        ):
                            build_and_flush(cm, websocket.app.state.cosmos)
                            logger.info(f"Analytics persisted for session {session_id}")
                    except Exception as e:
                        logger.error(
                            f"Error persisting analytics for session {session_id}: {e}"
                        )

                    cleanup_op.log_info(
                        f"Browser conversation cleanup completed: session {session_id}"
                    )


def create_v1_realtime_handler(
    orchestrator: Optional[callable] = None,
) -> V1RealtimeHandler:
    """
    Factory function for creating V1 realtime handlers.

    :param orchestrator: Optional orchestrator for conversation processing
    :type orchestrator: Optional[callable]
    :return: Configured V1 realtime handler instance
    :rtype: V1RealtimeHandler
    """
    return V1RealtimeHandler(orchestrator=orchestrator)
