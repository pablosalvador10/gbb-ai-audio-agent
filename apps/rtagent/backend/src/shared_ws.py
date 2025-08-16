# apps/rtagent/backend/src/shared_ws.py
"""
shared_ws.py
============
Helpers that BOTH realtime and ACS routers rely on:

    • send_tts_audio        – browser TTS
    • send_response_to_acs  – phone-call TTS
    • push_final            – “close bubble” helper
    • broadcast_message     – relay to /relay dashboards
"""

from __future__ import annotations

import asyncio
import base64
import json
from typing import Optional

from fastapi import WebSocket
from fastapi.websockets import WebSocketState

from apps.rtagent.backend.settings import ACS_STREAMING_MODE, GREETING_VOICE_TTS
from apps.rtagent.backend.src.latency.latency_tool import LatencyTool
from apps.rtagent.backend.src.services.acs.acs_helpers import (
    broadcast_message,            # re-exported for convenience
    play_response_with_queue,
)
# ✅ keep imports consistent with main.py/services package
from apps.rtagent.backend.src.services import SpeechSynthesizer

from src.enums.stream_modes import StreamMode
from utils.ml_logging import get_logger

logger = get_logger("shared_ws")


async def send_tts_audio(
    text: str,
    ws: WebSocket,
    latency_tool: Optional[LatencyTool] = None,
    voice_name: Optional[str] = None,
    voice_style: Optional[str] = None,
    rate: Optional[str] = None,
) -> None:
    """
    Synthesize speech and send audio data to browser WebSocket client.

    Uses the synthesizer cached on FastAPI `app.state.tts_client`.
    Adds latency tracking for TTS step and sends audio frames to React frontend.
    """
    # Validate WebSocket connection
    if ws.client_state != WebSocketState.CONNECTED or ws.application_state != WebSocketState.CONNECTED:
        logger.warning("WebSocket not connected; skip browser TTS send")
        return

    if not text or not text.strip():
        logger.warning("Empty text provided for TTS synthesis")
        return

    synth: SpeechSynthesizer = getattr(ws.app.state, "tts_client", None)
    if synth is None:
        logger.error("TTS client not initialized on app.state.tts_client")
        return

    if latency_tool:
        latency_tool.start("tts")
        latency_tool.start("tts:synthesis")

    ws.state.is_synthesizing = True  # type: ignore[attr-defined]
    voice_to_use = voice_name or GREETING_VOICE_TTS

    try:
        # Synthesize to PCM for browser playback
        pcm_bytes = synth.synthesize_to_pcm(
            text=text,
            voice=voice_to_use,
            sample_rate=16000,
            style=voice_style or "chat",
            rate=rate or "+3%",
        )
        if latency_tool:
            latency_tool.stop("tts:synthesis", ws.app.state.redis)

        frames = SpeechSynthesizer.split_pcm_to_base64_frames(pcm_bytes, sample_rate=16000)
        logger.debug(f"Browser TTS: sending {len(frames)} frames")

        for i, frame in enumerate(frames):
            if ws.client_state != WebSocketState.CONNECTED or ws.application_state != WebSocketState.CONNECTED:
                logger.info("WebSocket disconnected during browser TTS send; abort")
                break
            await ws.send_json(
                {
                    "type": "audio_data",
                    "data": frame,
                    "frame_index": i,
                    "total_frames": len(frames),
                    "sample_rate": 16000,
                    "is_final": i == len(frames) - 1,
                }
            )

    except Exception as e:
        logger.error(f"Browser TTS synthesis/send failed: {e}")
        try:
            await ws.send_json(
                {
                    "type": "tts_error",
                    "error": str(e),
                    "text": (text[:100] + "...") if len(text) > 100 else text,
                }
            )
        except Exception:
            pass
    finally:
        ws.state.is_synthesizing = False  # type: ignore[attr-defined]
        if latency_tool:
            try:
                latency_tool.stop("tts", ws.app.state.redis)
            except Exception:
                # don't let metrics failures bubble up
                pass


async def send_response_to_acs(
    ws: WebSocket,
    text: str,
    *,
    blocking: bool = False,
    latency_tool: Optional[LatencyTool] = None,
    stream_mode: StreamMode = ACS_STREAMING_MODE,
    voice_name: Optional[str] = None,
    voice_style: Optional[str] = None,
    rate: Optional[str] = None,
) -> Optional[asyncio.Task]:
    """
    Synthesizes speech and sends it as audio data to the ACS WebSocket.

    Returns:
        • MEDIA mode  → None (frames are sent inline)
        • TRANSCRIPTION mode → asyncio.Task (playback task)
    """
    if not text or not text.strip():
        return None

    if latency_tool:
        latency_tool.start("tts")
        latency_tool.start("tts:synthesis")

    async def _stop_latency(task):
        if latency_tool:
            try:
                latency_tool.stop("tts", ws.app.state.redis)
            except Exception:
                pass
        # task registry is optional; guard it
        if hasattr(ws.app.state, "tts_tasks"):
            try:
                ws.app.state.tts_tasks.discard(task)
            except Exception:
                pass

    if stream_mode == StreamMode.MEDIA:
        synth: SpeechSynthesizer = getattr(ws.app.state, "tts_client", None)
        if synth is None:
            if latency_tool:
                latency_tool.stop("tts", ws.app.state.redis)
            raise RuntimeError("TTS client not initialized")

        voice_to_use = voice_name or GREETING_VOICE_TTS

        try:
            pcm_bytes = synth.synthesize_to_pcm(
                text=text,
                voice=voice_to_use,
                sample_rate=16000,
                style=voice_style or "chat",
                rate=rate or "+3%",
            )
            frames = SpeechSynthesizer.split_pcm_to_base64_frames(pcm_bytes, sample_rate=16000)
            if latency_tool:
                latency_tool.stop("tts:synthesis", ws.app.state.redis)
        except Exception as e:
            if latency_tool:
                latency_tool.stop("tts", ws.app.state.redis)
            raise RuntimeError(f"TTS synthesis failed: {e}") from e

        try:
            for frame in frames:
                # first-byte TTFB stop (if set by media endpoint)
                if hasattr(ws.state, "lt") and ws.state.lt and not getattr(ws.state, "_greeting_ttfb_stopped", False):
                    try:
                        ws.state.lt.stop("greeting_ttfb", ws.app.state.redis)
                        ws.state._greeting_ttfb_stopped = True  # type: ignore[attr-defined]
                    except Exception:
                        pass

                # 🔁 Send ACS media frame (wire-format used across v1)
                await ws.send_json(
                    {
                        "kind": "AudioData",      # lower-case 'kind' is consistent everywhere
                        "AudioData": {"data": frame},
                        "StopAudio": None,
                    }
                )
        finally:
            if latency_tool:
                try:
                    latency_tool.stop("tts", ws.app.state.redis)
                except Exception:
                    pass

        return None

    # TRANSCRIPTION mode – play via Call Automation queue
    acs_caller = getattr(ws.app.state, "acs_caller", None)
    if not acs_caller:
        if latency_tool:
            latency_tool.stop("tts", ws.app.state.redis)
        raise RuntimeError("ACS caller is not initialized in WebSocket state")

    coro = play_response_with_queue(
        ws=ws,
        response_text=text,
        participants=[getattr(ws.app.state, "target_participant", None)],
    )

    if not hasattr(ws.app.state, "tts_tasks"):
        ws.app.state.tts_tasks = set()

    task = asyncio.create_task(coro)
    ws.app.state.tts_tasks.add(task)
    task.add_done_callback(_stop_latency)
    if blocking:
        await task
        return None
    return task


async def push_final(
    ws: WebSocket,
    role: str,
    content: str,
    *,
    is_acs: bool = False,  # kept for signature compatibility
) -> None:
    """Close the streaming bubble on the front-end (browser & ACS)."""
    try:
        await ws.send_text(json.dumps({"type": role, "content": content}))
    except Exception as e:
        logger.error(f"Failed to send final message to WebSocket: {e}")


__all__ = [
    "send_tts_audio",
    "send_response_to_acs",
    "push_final",
    "broadcast_message",
]
