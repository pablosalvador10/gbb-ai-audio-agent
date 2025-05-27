"""
helpers.py
===========

Utility helpers shared by the browser_RTMedAgent backend and the new
modular routers.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from fastapi import WebSocket

from utils.ml_logging import get_logger
from rtagents.RTMedAgent.backend.settings import STOP_WORDS

logger = get_logger("helpers")


def check_for_stopwords(prompt: str) -> bool:
    """Return True iff the message contains an exit keyword."""
    return any(stop in prompt.lower() for stop in STOP_WORDS)


def check_for_interrupt(prompt: str) -> bool:
    """Return True iff the message is an interrupt control frame."""
    return "interrupt" in prompt.lower()


def add_space(text: str) -> str:
    """
    Ensure the chunk ends with a single space or newline.

    This prevents “...assistance.Could” from appearing when we flush on '.'.
    """
    if text and text[-1] not in [" ", "\n"]:
        return text + " "
    return text


async def receive_and_filter(ws: WebSocket) -> Optional[str]:
    """
    Read one frame from `ws`.

    • If the payload is JSON and indicates an *interrupt* we tell the TTS
      engine to stop speaking and return None so the caller loop can skip
      processing.

    • Otherwise we return the plain string (either the raw text or the
      `.text` field from a JSON envelope).
    """
    raw: str = await ws.receive_text()
    try:
        msg: Dict[str, Any] = json.loads(raw)
        if msg.get("type") == "interrupt":
            logger.info("🛑 interrupt received – stopping TTS playback")
            # The TTS synthesizer lives on FastAPI app state
            ws.app.state.tts_client.stop_speaking()  # type: ignore[attr-defined]
            return None
        return msg.get("text", raw)
    except json.JSONDecodeError:
        return raw.strip()
