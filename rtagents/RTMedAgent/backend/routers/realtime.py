"""
routers/realtime.py
===================
• `/relay`     – dashboard broadcast WebSocket
• `/realtime`  – browser/WebRTC conversation endpoint

Relies on:
    utils.helpers.receive_and_filter
    orchestration.gpt_flow.route_turn
"""

from __future__ import annotations

import json
import uuid

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from rtagents.RTMedAgent.backend.orchestration.conversation_state import (
    ConversationManager,
)
from rtagents.RTMedAgent.backend.latency.latency_tool import LatencyTool
from helpers import check_for_stopwords, receive_and_filter
from rtagents.RTMedAgent.backend.orchestration.orchestrator import route_turn
from shared_ws import send_tts_audio, broadcast_message
from rtagents.RTMedAgent.backend.postcall.push import build_and_flush
from utils.ml_logging import get_logger

logger = get_logger("realtime_router")


router = APIRouter()


# --------------------------------------------------------------------------- #
#  /relay  – simple fan-out to connected dashboards
# --------------------------------------------------------------------------- #
@router.websocket("/relay")
async def relay_ws(ws: WebSocket):
    """Dashboards connect here to receive broadcasted text."""
    clients: set[WebSocket] = ws.app.state.clients
    if ws not in clients:
        await ws.accept()
        clients.add(ws)

    try:
        while True:
            await ws.receive_text()  # keep ping/pong alive
    except WebSocketDisconnect:
        clients.remove(ws)


# --------------------------------------------------------------------------- #
#  /realtime  – browser conversation
# --------------------------------------------------------------------------- #
@router.websocket("/realtime")
async def realtime_ws(ws: WebSocket):
    """
    Browser/WebRTC client sends STT text; we stream GPT + TTS back.
    The shared `route_turn` handles auth vs. main dialog.
    """
    try:
        await ws.accept()
        session_id = ws.headers.get("x-ms-call-connection-id") or uuid.uuid4().hex[:8]

        redis_mgr = ws.app.state.redis
        cm = ConversationManager.from_redis(session_id, redis_mgr)
        ws.state.cm = cm
        ws.state.session_id = session_id
        ws.state.lt = LatencyTool(cm)

        # -------- send greeting once per WebSocket -----------------------------
        greeting = (
            "Hello from XMYX Healthcare Company! Before I can assist you, "
            "let’s verify your identity. How may I address you?"
        )
        await ws.send_text(json.dumps({"type": "status", "message": greeting}))
        await send_tts_audio(greeting, ws, latency_tool=ws.state.lt)
        await broadcast_message(ws.app.state.clients, greeting, "Assistant")
        cm.append_to_history("assistant", greeting)
        cm.persist_to_redis(redis_mgr)

        # ---------------- main loop -------------------------------------------
        while True:
            prompt = await receive_and_filter(ws)
            if prompt is None:
                continue

            if check_for_stopwords(prompt):
                goodbye = "Thank you for using our service. Goodbye."
                await ws.send_text(json.dumps({"type": "exit", "message": goodbye}))
                await send_tts_audio(goodbye, ws, latency_tool=ws.state.lt)
                break

            await route_turn(cm, prompt, ws, is_acs=False)

    finally:
        try:
            cm = getattr(ws.state, "cm", None)
            cosmos = getattr(ws.app.state, "cosmos", None)
            if cm and cosmos:
                build_and_flush(cm, cosmos)
        except Exception as e:
            logger.error(f"Error persisting analytics: {e}", exc_info=True)
