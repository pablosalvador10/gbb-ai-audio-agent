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
from typing import Optional, Set

from fastapi import WebSocket
from fastapi.websockets import WebSocketState

from apps.rtagent.backend.settings import ACS_STREAMING_MODE, VOICE_TTS
from apps.rtagent.backend.src.latency.latency_tool import LatencyTool
from apps.rtagent.backend.src.services.acs.acs_helpers import (
    broadcast_message,
    play_response_with_queue,
)
from apps.rtagent.backend.src.services.speech_services import SpeechSynthesizer
from src.enums.stream_modes import StreamMode
from src.stateful.state_managment import MemoManager
from utils.ml_logging import get_logger

logger = get_logger("shared_ws")


async def send_tts_audio(
    text: str, ws: WebSocket, latency_tool: Optional[LatencyTool] = None
) -> None:
    """
    Synthesize speech and send audio data to browser WebSocket client.

    Contract with VAD (cooperative cancel):
      - Sets ws.state.is_synthesizing = True while frames are being sent.
      - Checks ws.state.tts_cancel each frame; VAD sets this True on barge-in.
      - Always resets flags in finally.
    """
    if ws.client_state != WebSocketState.CONNECTED:
        logger.error("WebSocket is not connected, cannot send TTS audio")
        return

    text = (text or "").strip()
    if not text:
        logger.warning("Empty text provided for TTS synthesis")
        return

    # init per-call state
    setattr(ws.state, "tts_cancel", False)
    setattr(ws.state, "is_synthesizing", True)

    if latency_tool:
        latency_tool.start("tts")
        latency_tool.start("tts:synthesis")

    try:
        synth: SpeechSynthesizer = ws.app.state.tts_client
        logger.debug("Synthesizing PCM for TTS...")
        pcm_bytes = synth.synthesize_to_pcm(text=text, voice=VOICE_TTS, sample_rate=16000)

        if latency_tool:
            latency_tool.stop("tts:synthesis", ws.app.state.redis)

        frames = SpeechSynthesizer.split_pcm_to_base64_frames(pcm_bytes, sample_rate=16000)
        logger.debug("Prepared %d frames for WebSocket transmission", len(frames))

        for i, frame in enumerate(frames):
            # Cooperative cancel: VAD sets ws.state.tts_cancel=True on barge-in
            if getattr(ws.state, "tts_cancel", False):
                logger.info("TTS canceled mid-stream (barge-in); dropping remaining frames")
                break
            if ws.client_state != WebSocketState.CONNECTED:
                logger.warning("WebSocket disconnected during audio transmission")
                break

            try:
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
                logger.error("Failed to send audio frame %d: %s", i, e)
                break

        logger.debug("TTS audio transmission completed")

    except Exception as e:
        logger.error("TTS synthesis failed: %s", e)
        # Send error message to frontend (best effort)
        try:
            await ws.send_json(
                {
                    "type": "tts_error",
                    "error": str(e),
                    "text": (text[:100] + "...") if len(text) > 100 else text,
                }
            )
        except Exception as send_error:
            logger.error("Failed to send error message to frontend: %s", send_error)
    finally:
        # Always release flags
        try:
            ws.state.is_synthesizing = False
            ws.state.tts_cancel = False
        except Exception:
            pass
        if latency_tool:
            try:
                latency_tool.stop("tts", ws.app.state.redis)
            except Exception:
                pass


async def send_response_to_acs(
    ws: WebSocket,
    text: str,
    *,
    blocking: bool = False,
    latency_tool: Optional[LatencyTool] = None,
    stream_mode: StreamMode = ACS_STREAMING_MODE,
) -> Optional[asyncio.Task]:
    """
    Synthesizes speech and sends it as audio data to the ACS WebSocket.

    Adds latency tracking for TTS step.
    """

    if latency_tool:
        latency_tool.start("tts")
        latency_tool.start("tts:synthesis")

    async def stop_latency(task):
        if latency_tool:
            latency_tool.stop("tts", ws.app.state.redis)
        ws.app.state.tts_tasks.discard(task)

    if stream_mode == StreamMode.MEDIA:
        synth: SpeechSynthesizer = ws.app.state.tts_client

        try:
            # Add timeout and retry logic for TTS synthesis
            pcm_bytes = synth.synthesize_to_pcm(
                text=text, voice=VOICE_TTS, sample_rate=16000
            )
            frames = SpeechSynthesizer.split_pcm_to_base64_frames(
                pcm_bytes, sample_rate=16000
            )
            latency_tool.stop("tts:synthesis", ws.app.state.redis)

        except asyncio.TimeoutError:
            logger.error(f"TTS synthesis timed out for text: {text[:50]}...")
            raise RuntimeError("TTS synthesis timed out")
        except Exception as e:
            logger.error(f"TTS synthesis failed: {e}")

        for frame in frames:
            if (
                hasattr(ws.state, "lt")
                and ws.state.lt
                and not getattr(ws.state, "_greeting_ttfb_stopped", False)
            ):
                ws.state.lt.stop("greeting_ttfb", ws.app.state.redis)
                ws.state._greeting_ttfb_stopped = True
            try:
                await ws.send_json(
                    {"kind": "AudioData", "AudioData": {"data": frame}, "StopAudio": None}
                )
            except Exception as e:
                logger.error(f"Failed to send ACS audio frame: {e}")
                break

        if latency_tool:
            latency_tool.stop("tts", ws.app.state.redis)

    elif stream_mode == StreamMode.TRANSCRIPTION:
        acs_caller = ws.app.state.acs_caller
        if not acs_caller:
            raise RuntimeError("ACS caller is not initialized in WebSocket state.")

        coro = play_response_with_queue(
            ws=ws, response_text=text, participants=[ws.app.state.target_participant]
        )

        if not hasattr(ws.app.state, "tts_tasks"):
            ws.app.state.tts_tasks = set()

        task = asyncio.create_task(coro)
        ws.app.state.tts_tasks.add(task)
        task.add_done_callback(stop_latency)

        return task


async def push_final(
    ws: WebSocket,
    role: str,
    content: str,
    *,
    is_acs: bool = False,
) -> None:
    """
    Close the streaming bubble on the front-end.

    • Browser/WebRTC – we already streamed TTS, just send the final JSON.
    • ACS            – same; streaming audio is finished, no repeat playback.
    """
    await ws.send_text(json.dumps({"type": role, "content": content}))


# --------------------------------------------------------------------------- #
# Re-export for convenience
# --------------------------------------------------------------------------- #
__all__ = [
    "send_tts_audio",
    "send_response_to_acs",
    "push_final",
    "broadcast_message",
]
