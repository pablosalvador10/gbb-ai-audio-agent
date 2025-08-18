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

from apps.rtagent.backend.settings import ACS_STREAMING_MODE, GREETING_VOICE_TTS
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
    text: str,
    ws: WebSocket,
    latency_tool: Optional[LatencyTool] = None,
    voice_name: Optional[str] = None,
    voice_style: Optional[str] = None,
    rate: Optional[str] = None,
) -> None:
    """
    Synthesize speech and send audio data to browser WebSocket client.

    Uses per-connection TTS synthesizer from `ws.state.tts_client` (acquired from pool).
    If not available, temporarily acquires one from the pool for this operation.
    Adds latency tracking for TTS step and sends audio frames to React frontend.

    Args:
        text: Text to synthesize
        ws: WebSocket connection
        latency_tool: Optional latency tracking tool
        voice_name: Optional agent-specific voice name (overrides default)
        voice_style: Optional agent-specific voice style
        rate: Optional speaking rate (e.g. "+3%")
    """
    # Validate WebSocket connection
    if ws.client_state != WebSocketState.CONNECTED:
        logger.error("WebSocket is not connected, cannot send TTS audio")
        return

    if not text or not text.strip():
        logger.warning("Empty text provided for TTS synthesis")
        return

    # Enhanced tracking using context managers
    tts_context = None
    synthesis_context = None
    
    # Get turn ID from WebSocket state for enhanced tracking
    turn_id = getattr(ws.state, "turn_id", None)
    
    # Use enhanced tracking if available, fallback to legacy
    if hasattr(ws.state, "cm") and ws.state.cm and hasattr(ws.state.cm, "track_stage"):
        tts_context = ws.state.cm.track_stage("tts")
        await tts_context.__aenter__()
        synthesis_context = ws.state.cm.track_stage("tts:synthesis")
        await synthesis_context.__aenter__()
    elif latency_tool:
        # Legacy tracking fallback
        latency_tool.start("tts")
        latency_tool.start("tts:synthesis")

    # Get per-connection TTS synthesizer or acquire one temporarily
    synth = None
    temp_synth = False
    try:
        if hasattr(ws.state, "tts_client") and ws.state.tts_client:
            synth = ws.state.tts_client
        else:
            # Fallback: temporarily acquire from pool
            synth = await ws.app.state.tts_pool.acquire()
            temp_synth = True
            logger.warning(f"Temporarily acquired TTS synthesizer from pool - session should have its own")

        # Per-connection flag lives on ws.state (was already) – keep isolated
        ws.state.is_synthesizing = True  # type: ignore[attr-defined]
        style = voice_style or "chat"
        eff_rate = rate or "+3%"
        voice_to_use = voice_name or GREETING_VOICE_TTS

        logger.debug(
            "tts.start voice=%s style=%s rate=%s text_preview=%r",
            voice_to_use,
            style,
            eff_rate,
            text[:60],
        )

        # Single synthesis (avoid duplicate start + synth cycle)
        pcm_bytes = synth.synthesize_to_pcm(
            text=text,
            voice=voice_to_use,
            sample_rate=48000,
            style=style,
            rate=eff_rate,
        )

        if synthesis_context:
            await synthesis_context.__aexit__(None, None, None)
        elif latency_tool:
            latency_tool.stop("tts:synthesis", ws.app.state.redis)

        frames = SpeechSynthesizer.split_pcm_to_base64_frames(pcm_bytes, sample_rate=48000)
        try:
            from opentelemetry import trace as _t
            _t.get_current_span().set_attribute("pipeline.stage", "tts -> websocket (browser)")
        except Exception:
            pass
        logger.debug("tts.frames_prepared count=%d", len(frames))

        for i, frame in enumerate(frames):
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
                        "sample_rate": 48000,
                        "is_final": i == len(frames) - 1,
                    }
                )
            except Exception as e:
                logger.error(f"Failed to send audio frame {i}: {e}")
                break
        logger.debug("tts.complete success=True frames_sent=%d", len(frames))

    except Exception as e:
        logger.error(f"TTS synthesis failed: {e}")
        # Cleanup enhanced tracking contexts on error
        if synthesis_context:
            try:
                await synthesis_context.__aexit__(type(e), e, e.__traceback__)
            except Exception:
                pass
        elif latency_tool:
            # Ensure synthesis sub-span is closed if failure before stop
            try:
                latency_tool.stop("tts:synthesis", ws.app.state.redis)
            except Exception:
                pass
        try:
            await ws.send_json(
                {
                    "type": "tts_error",
                    "error": str(e),
                    "text": text[:100] + "..." if len(text) > 100 else text,
                }
            )
        except Exception as send_error:
            logger.error(f"Failed to send error message to frontend: {send_error}")
    finally:
        # Cleanup enhanced tracking contexts
        if tts_context:
            try:
                await tts_context.__aexit__(None, None, None)
            except Exception:
                pass
        elif latency_tool:
            try:
                latency_tool.stop("tts", ws.app.state.redis)
            except Exception:
                pass
        try:
            ws.state.is_synthesizing = False  # type: ignore[attr-defined]
        except Exception:
            pass
        # Release temporary synthesizer back to pool if we acquired one
        if temp_synth and synth:
            try:
                await ws.app.state.tts_pool.release(synth)
            except Exception as e:
                logger.error(f"Error releasing temporary TTS synthesizer: {e}")


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
    Synthesize speech and send audio data to the ACS WebSocket.

    Uses per-connection TTS synthesizer from `ws.state.tts_client` (acquired from pool).
    In TRANSCRIPTION mode, audio playback is delegated via queue helper and may not
    perform local synthesis here; we stop the synthesis sub-timer immediately.

    Args:
        ws: WebSocket connection
        text: Text to synthesize
        blocking: Whether to wait for completion
        latency_tool: Optional latency tracking tool
        stream_mode: Streaming mode for ACS
        voice_name: Optional agent-specific voice name (overrides default)
        voice_style: Optional agent-specific voice style
        rate: Optional speaking rate
    """

    if latency_tool:
        latency_tool.start("tts")
        latency_tool.start("tts:synthesis")

    async def stop_latency(task):
        if latency_tool:
            latency_tool.stop("tts", ws.app.state.redis)
        # tts_tasks is per-connection; keep on ws.state
        if hasattr(ws.state, "tts_tasks"):
            ws.state.tts_tasks.discard(task)

    if stream_mode == StreamMode.MEDIA:
        # Get per-connection TTS synthesizer or acquire one temporarily
        synth = None
        temp_synth = False
        try:
            if hasattr(ws.state, "tts_client") and ws.state.tts_client:
                synth = ws.state.tts_client
            else:
                # Fallback: temporarily acquire from pool
                synth = await ws.app.state.tts_pool.acquire()
                temp_synth = True
                logger.warning(f"ACS MEDIA: Temporarily acquired TTS synthesizer from pool - session should have its own")

            # Use agent voice if provided, otherwise fallback to default
            voice_to_use = voice_name or GREETING_VOICE_TTS

            # Add timeout and retry logic for TTS synthesis
            pcm_bytes = synth.synthesize_to_pcm(
                text=text,
                voice=voice_to_use,
                sample_rate=16000,
                style=voice_style or "chat",
                rate=rate or "+3%",
            )
            frames = SpeechSynthesizer.split_pcm_to_base64_frames(
                pcm_bytes, sample_rate=16000
            )

            if latency_tool:
                latency_tool.stop("tts:synthesis", ws.app.state.redis)

        except asyncio.TimeoutError:
            logger.error(f"TTS synthesis timed out for text: {text[:50]}...")
            if latency_tool:
                latency_tool.stop("tts", ws.app.state.redis)
            raise RuntimeError("TTS synthesis timed out")
        except Exception as e:
            logger.error(f"TTS synthesis failed: {e}")
            if latency_tool:
                latency_tool.stop("tts", ws.app.state.redis)
            raise RuntimeError(f"TTS synthesis failed: {e}")
        finally:
            # Release temporary synthesizer back to pool if we acquired one
            if temp_synth and synth:
                try:
                    await ws.app.state.tts_pool.release(synth)
                except Exception as e:
                    logger.error(f"Error releasing temporary TTS synthesizer: {e}")

        # Send audio frames to ACS WebSocket
        try:
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
                        {
                            "kind": "AudioData",
                            "AudioData": {"data": frame},
                            "StopAudio": None,
                        }
                    )
                except Exception as e:
                    logger.error(f"Failed to send ACS audio frame: {e}")
                    break
        except Exception as e:
            logger.error(f"Failed to send audio frames to ACS: {e}")
        finally:
            if latency_tool:
                latency_tool.stop("tts", ws.app.state.redis)

        # Return None for MEDIA mode (synchronous completion)
        return None

    elif stream_mode == StreamMode.TRANSCRIPTION:
        acs_caller = ws.app.state.acs_caller
        if not acs_caller:
            raise RuntimeError("ACS caller is not initialized in WebSocket state.")

        # Fetch participant from per-connection state (moved off app.state)
        target_participant = getattr(ws.state, "target_participant", None)
        coro = play_response_with_queue(
            ws=ws, response_text=text, participants=[target_participant] if target_participant else None
        )

        if not hasattr(ws.state, "tts_tasks"):
            ws.state.tts_tasks = set()

        task = asyncio.create_task(coro)
        ws.state.tts_tasks.add(task)
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
    try:
        await ws.send_text(json.dumps({"type": role, "content": content}))
    except Exception as e:
        logger.error(f"Failed to send final message to WebSocket: {e}")


# --------------------------------------------------------------------------- #
# Re-export for convenience
# --------------------------------------------------------------------------- #
__all__ = [
    "send_tts_audio",
    "send_response_to_acs",
    "push_final",
    "broadcast_message",
]
