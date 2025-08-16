# apps/rtagent/backend/api/v1/handlers/realtime.py
"""
V1 Realtime Handler
===================

Real-time communication handler for V1 API with clean tracing and simplified logging.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Optional

from fastapi import WebSocket, WebSocketDisconnect
from fastapi.websockets import WebSocketState
from opentelemetry import trace
from opentelemetry.trace import SpanKind

from apps.rtagent.backend.settings import GREETING
from apps.rtagent.backend.src.helpers import check_for_stopwords
from apps.rtagent.backend.src.latency.latency_tool import LatencyTool
from apps.rtagent.backend.src.orchestration.orchestrator import route_turn
from apps.rtagent.backend.src.shared_ws import broadcast_message, send_tts_audio
from src.postcall.push import build_and_flush
from src.stateful.state_managment import MemoManager
from utils.ml_logging import get_logger

# V1 tracing helpers
from apps.rtagent.backend.src.utils.tracing import trace_acs_operation, trace_acs_dependency

logger = get_logger("v1.api.handlers.realtime")
tracer = trace.get_tracer(__name__)


class V1RealtimeHandler:
    """Real-time communication handler for V1 API."""

    def __init__(self, orchestrator: Optional[callable] = None):
        self.orchestrator = orchestrator
        self.logger = get_logger("api.v1.handlers.realtime")

    async def handle_dashboard_relay(self, websocket: WebSocket) -> None:
        """Handle dashboard relay WebSocket connections."""
        with trace_acs_operation(tracer, logger, "dashboard_relay") as op:
            clients: set[WebSocket] = websocket.app.state.clients
            client_id = str(uuid.uuid4())[:8]
            op.log_info(f"Dashboard client connecting: {client_id}")

            try:
                await websocket.accept()
                clients.add(websocket)
                op.log_info(f"Dashboard client connected: {client_id} (total: {len(clients)})")

                while (
                    websocket.client_state == WebSocketState.CONNECTED
                    and websocket.application_state == WebSocketState.CONNECTED
                ):
                    try:
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
                clients.discard(websocket)
                try:
                    if (
                        websocket.client_state == WebSocketState.CONNECTED
                        and websocket.application_state == WebSocketState.CONNECTED
                    ):
                        await websocket.close()
                except Exception:
                    pass
                op.log_info(f"Dashboard client cleanup completed: {client_id} (remaining: {len(clients)})")

    async def handle_browser_conversation(self, websocket: WebSocket, orchestrator: Optional[callable] = None) -> None:
        """Handle browser conversation with orchestrator support."""
        active_orchestrator = orchestrator or self.orchestrator
        orchestrator_name = getattr(active_orchestrator, "name", "legacy") if active_orchestrator else "legacy"

        session_id = None
        cm = None

        with trace_acs_operation(tracer, logger, "browser_conversation", orchestrator_name=orchestrator_name) as op:
            try:
                await websocket.accept()
                session_id = websocket.headers.get("x-ms-call-connection-id") or uuid.uuid4().hex[:8]
                op.log_info(f"Browser conversation started: {session_id} ({orchestrator_name})")

                redis_mgr = websocket.app.state.redis
                cm = MemoManager.from_redis(session_id, redis_mgr)

                websocket.state.cm = cm
                websocket.state.session_id = session_id
                websocket.state.lt = LatencyTool(cm)
                websocket.state.is_synthesizing = False
                websocket.state.user_buffer = ""
                websocket.state.orchestrator_name = orchestrator_name
                websocket.state.api_version = "v1"

                cm.update_context("api_version", "v1")
                cm.update_context("orchestrator_name", orchestrator_name)
                cm.update_context("v1_features_enabled", True)

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

                auth_agent = websocket.app.state.auth_agent
                cm.append_to_history(auth_agent.name, "assistant", GREETING)
                await broadcast_message(websocket.app.state.clients, GREETING, "Auth Agent")

                with trace_acs_dependency(tracer, logger, "tts_service", "send_greeting", session_id=session_id):
                    await send_tts_audio(GREETING, websocket, latency_tool=websocket.state.lt)

                await cm.persist_to_redis_async(redis_mgr)

                def on_partial(txt: str, lang: str):
                    if websocket.state.is_synthesizing:
                        try:
                            websocket.app.state.tts_client.stop_speaking()
                            websocket.state.is_synthesizing = False
                        except Exception:
                            pass
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
                    websocket.state.user_buffer += txt.strip() + "\n"

                websocket.app.state.stt_client.set_partial_result_callback(on_partial)
                websocket.app.state.stt_client.set_final_result_callback(on_final)
                websocket.app.state.stt_client.start()

                while True:
                    msg = await websocket.receive()

                    if msg.get("type") == "websocket.receive" and msg.get("bytes") is not None:
                        websocket.app.state.stt_client.write_bytes(msg["bytes"])

                        if websocket.state.user_buffer.strip():
                            prompt = websocket.state.user_buffer.strip()
                            websocket.state.user_buffer = ""

                            await websocket.send_text(
                                json.dumps(
                                    {
                                        "sender": "User",
                                        "message": prompt,
                                        "session_id": session_id,
                                        "api_version": "v1",
                                    }
                                )
                            )

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
                                with trace_acs_dependency(tracer, logger, "tts_service", "send_farewell", session_id=session_id):
                                    await send_tts_audio(goodbye, websocket, latency_tool=websocket.state.lt)
                                break

                            with trace_acs_dependency(
                                tracer, logger, "conversation_orchestrator", "route_turn", session_id=session_id, orchestrator_name=orchestrator_name
                            ):
                                await route_turn(cm, prompt, websocket, is_acs=False)

                        continue

                    if msg.get("type") == "websocket.disconnect":
                        break

            except WebSocketDisconnect:
                op.log_info(f"Browser client disconnected: {session_id}")
            except Exception as e:
                op.set_error(f"Browser conversation error: {e}")
            finally:
                with trace_acs_dependency(tracer, logger, "cleanup_service", "session_cleanup", session_id=session_id):
                    try:
                        websocket.app.state.tts_client.stop_speaking()
                    except Exception:
                        pass
                    try:
                        if (
                            websocket.client_state == WebSocketState.CONNECTED
                            and websocket.application_state == WebSocketState.CONNECTED
                        ):
                            await websocket.close()
                    except Exception:
                        pass
                    try:
                        if cm and hasattr(websocket.app.state, "cosmos") and websocket.app.state.cosmos:
                            build_and_flush(cm, websocket.app.state.cosmos)
                    except Exception:
                        pass


def create_v1_realtime_handler(orchestrator: Optional[callable] = None) -> V1RealtimeHandler:
    """Factory function for creating V1 realtime handlers."""
    return V1RealtimeHandler(orchestrator=orchestrator)
