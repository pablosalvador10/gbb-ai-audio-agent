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
from src.tools.latency_tool import LatencyTool
from apps.rtagent.backend.src.services.acs.acs_helpers import (
    broadcast_message,
    play_response_with_queue,
)
from apps.rtagent.backend.src.services.speech_services import SpeechSynthesizer
from src.enums.stream_modes import StreamMode
from src.stateful.state_managment import MemoManager
from utils.ml_logging import get_logger

logger = get_logger("shared_ws")


# ------- small internal helpers (no API change) --------------------------------
def _lt_get_run_id(ws: WebSocket, lt: Optional[LatencyTool]) -> Optional[str]:
    """Try to get the current run_id from LatencyTool or CoreMemory (if available)."""
    try:
        if lt and hasattr(lt, "get_current_run") and callable(lt.get_current_run):
            rid = lt.get_current_run()
            if rid:
                return rid
    except Exception:
        pass
    try:
        cm = getattr(ws.state, "cm", None)
        if cm:
            return cm.get_value_from_corememory("current_run_id", None)
    except Exception:
        pass
    return None


def _lt_stop(lt: Optional[LatencyTool], stage: str, ws: WebSocket, meta: Optional[dict] = None):
    """Stop a latency stage with optional meta, falling back if the tool doesn't support it."""
    if not lt:
        return
    try:
        return lt.stop(stage, ws.app.state.redis, meta=meta or {})
    except TypeError:
        # older LatencyTool that doesn't accept meta
        return lt.stop(stage, ws.app.state.redis)
    except Exception as e:
        logger.error("Latency stop error for stage %s: %s", stage, e)


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

    :param text: Text to synthesize
    :type text: str
    :param ws: WebSocket connection
    :type ws: WebSocket
    :param latency_tool: Optional latency tracking tool
    :type latency_tool: Optional[LatencyTool]
    :param voice_name: Optional agent-specific voice name (overrides default)
    :type voice_name: Optional[str]
    :param voice_style: Optional agent-specific voice style
    :type voice_style: Optional[str]
    :param rate: Optional speaking rate (e.g. "+3%")
    :type rate: Optional[str]
    """
    # Validate WebSocket connection
    if ws.client_state != WebSocketState.CONNECTED:
        logger.error("WebSocket is not connected, cannot send TTS audio")
        return

    if not text or not text.strip():
        logger.warning("Empty text provided for TTS synthesis")
        return

    # Prepare metadata for latency samples
    run_id = _lt_get_run_id(ws, latency_tool)
    style = voice_style or "chat"
    eff_rate = rate or "+3%"
    voice_to_use = voice_name or GREETING_VOICE_TTS

    if latency_tool:
        try:
            latency_tool.start("tts")
            latency_tool.start("tts:synthesis")
        except Exception as e:
            logger.error("Latency start error (browser tts): %s", e)

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
            logger.warning(
                "Temporarily acquired TTS synthesizer from pool - session should have its own"
            )

        # Per-connection flag lives on ws.state (was already) – keep isolated
        ws.state.is_synthesizing = True  # type: ignore[attr-defined]

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

        _lt_stop(
            latency_tool,
            "tts:synthesis",
            ws,
            meta={"run_id": run_id, "mode": "browser", "voice": voice_to_use, "style": style, "rate": eff_rate},
        )

        frames = SpeechSynthesizer.split_pcm_to_base64_frames(
            pcm_bytes, sample_rate=48000
        )
        try:
            from opentelemetry import trace as _t

            _t.get_current_span().set_attribute(
                "pipeline.stage", "tts -> websocket (browser)"
            )
        except Exception:
            pass
        logger.debug("tts.frames_prepared count=%d", len(frames))

        # Optional: track sending loop as a sub-stage
        if latency_tool:
            try:
                latency_tool.start("tts:send_frames")
            except Exception:
                pass

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

        _lt_stop(
            latency_tool,
            "tts:send_frames",
            ws,
            meta={"run_id": run_id, "mode": "browser", "frames": len(frames)},
        )

        logger.debug("tts.complete success=True frames_sent=%d", len(frames))

    except Exception as e:
        logger.error(f"TTS synthesis failed: {e}")
        # Ensure synthesis sub-stage is closed if we failed before stopping it
        _lt_stop(
            latency_tool,
            "tts:synthesis",
            ws,
            meta={"run_id": run_id, "mode": "browser", "error": str(e)},
        )
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
        _lt_stop(
            latency_tool,
            "tts",
            ws,
            meta={"run_id": run_id, "mode": "browser", "voice": voice_to_use},
        )
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

    :param ws: WebSocket connection
    :type ws: WebSocket
    :param text: Text to synthesize
    :type text: str
    :param blocking: Whether to wait for completion
    :type blocking: bool
    :param latency_tool: Optional latency tracking tool
    :type latency_tool: Optional[LatencyTool]
    :param stream_mode: Streaming mode for ACS
    :type stream_mode: StreamMode
    :param voice_name: Optional agent-specific voice name (overrides default)
    :type voice_name: Optional[str]
    :param voice_style: Optional agent-specific voice style
    :type voice_style: Optional[str]
    :param rate: Optional speaking rate
    :type rate: Optional[str]
    :return: Task for async completion tracking in TRANSCRIPTION mode, None for MEDIA mode
    :rtype: Optional[asyncio.Task]
    :raises RuntimeError: When TTS synthesis fails or times out, or ACS caller not initialized
    """

    # Prepare meta
    run_id = _lt_get_run_id(ws, latency_tool)
    style = voice_style or "chat"
    eff_rate = rate or "+3%"
    voice_to_use = voice_name or GREETING_VOICE_TTS

    if latency_tool:
        try:
            latency_tool.start("tts")
            latency_tool.start("tts:synthesis")
        except Exception as e:
            logger.error("Latency start error (ACS tts): %s", e)

    async def stop_latency(task):
        _lt_stop(
            latency_tool,
            "tts",
            ws,
            meta={"run_id": run_id, "mode": "acs", "voice": voice_to_use},
        )
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
                logger.warning(
                    "ACS MEDIA: Temporarily acquired TTS synthesizer from pool - session should have its own"
                )

            # Add timeout and retry logic for TTS synthesis (if your synth supports it externally)
            pcm_bytes = synth.synthesize_to_pcm(
                text=text,
                voice=voice_to_use,
                sample_rate=16000,
                style=style,
                rate=eff_rate,
            )
            frames = SpeechSynthesizer.split_pcm_to_base64_frames(
                pcm_bytes, sample_rate=16000
            )

            _lt_stop(
                latency_tool,
                "tts:synthesis",
                ws,
                meta={
                    "run_id": run_id,
                    "mode": "acs",
                    "voice": voice_to_use,
                    "style": style,
                    "rate": eff_rate,
                    "frames": len(frames),
                },
            )

        except asyncio.TimeoutError:
            logger.error(f"TTS synthesis timed out for text: {text[:50]}...")
            _lt_stop(
                latency_tool,
                "tts",
                ws,
                meta={"run_id": run_id, "mode": "acs", "error": "timeout"},
            )
            raise RuntimeError("TTS synthesis timed out")
        except Exception as e:
            logger.error(f"TTS synthesis failed: {e}")
            _lt_stop(
                latency_tool,
                "tts",
                ws,
                meta={"run_id": run_id, "mode": "acs", "error": str(e)},
            )
            raise RuntimeError(f"TTS synthesis failed: {e}")
        finally:
            # Release temporary synthesizer back to pool if we acquired one
            if temp_synth and synth:
                try:
                    await ws.app.state.tts_pool.release(synth)
                except Exception as e:
                    logger.error(f"Error releasing temporary TTS synthesizer: {e}")

        # Optional: track sending loop as a sub-stage
        if latency_tool:
            try:
                latency_tool.start("tts:send_frames")
            except Exception:
                pass

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
            _lt_stop(
                latency_tool,
                "tts:send_frames",
                ws,
                meta={"run_id": run_id, "mode": "acs", "frames": len(frames)},
            )
            _lt_stop(
                latency_tool,
                "tts",
                ws,
                meta={"run_id": run_id, "mode": "acs", "voice": voice_to_use},
            )

        # Return None for MEDIA mode (synchronous completion)
        return None

    elif stream_mode == StreamMode.TRANSCRIPTION:
        acs_caller = ws.app.state.acs_caller
        if not acs_caller:
            _lt_stop(
                latency_tool,
                "tts",
                ws,
                meta={"run_id": run_id, "mode": "acs", "error": "no_acs_caller"},
            )
            raise RuntimeError("ACS caller is not initialized in WebSocket state.")

        # No local synthesis here; close the synthesis sub-stage immediately
        _lt_stop(
            latency_tool,
            "tts:synthesis",
            ws,
            meta={"run_id": run_id, "mode": "acs", "note": "delegated_to_queue"},
        )

        # Fetch participant from per-connection state (moved off app.state)
        target_participant = getattr(ws.state, "target_participant", None)
        coro = play_response_with_queue(
            ws=ws,
            response_text=text,
            participants=[target_participant] if target_participant else None,
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

    :param ws: WebSocket connection
    :type ws: WebSocket
    :param role: Role identifier for the message
    :type role: str
    :param content: Message content to send
    :type content: str
    :param is_acs: Whether this is an ACS call context
    :type is_acs: bool
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