from __future__ import annotations

"""
Shared websocket helpers.
"""

import asyncio
import json
import time
from typing import Optional

from fastapi import WebSocket
from fastapi.websockets import WebSocketState

from apps.rtagent.backend.settings import ACS_STREAMING_MODE, GREETING_VOICE_TTS
from apps.rtagent.backend.src.services.acs.acs_helpers import (
    broadcast_message,
    play_response_with_queue,
)
from apps.rtagent.backend.src.services.speech_services import SpeechSynthesizer
from src.enums.stream_modes import StreamMode
from src.stateful.state_managment import MemoManager
from utils.ml_logging import get_logger

logger = get_logger("shared_ws")


# ------------------------------- internal helpers -------------------------------

def _get_cm(ws: WebSocket) -> Optional[MemoManager]:
    """Return MemoManager from websocket state if present (`ws.state.cm`)."""
    return getattr(ws.state, "cm", None)


def _ensure_turn(ws: WebSocket, cm: Optional[MemoManager], *, agent: str) -> Optional[str]:
    """
    Ensure there is an active turn id, bind the tracker to it, and persist on ws.state.

    Priority:
      1) Use ws.state.turn_id if present and bind cm to that id.
      2) Else, call cm.start_turn(...) to create one and write it back to ws.state.turn_id.

    Returns the effective turn id, or None if no cm provided.
    """
    if not cm:
        return None

    # If a router upstream already created a turn id, reuse it.
    existing = getattr(ws.state, "turn_id", None)
    if existing:
        try:
            # Bind tracker correlation to the existing id (no-op if identical)
            cm.start_turn(existing, agent=agent)
        except Exception:
            logger.exception("Failed to bind cm to existing turn_id=%s", existing)
        return existing

    # Otherwise, create a new one via cm and stash it on ws.state
    try:
        new_id = cm.start_turn(agent=agent)  # auto-generates (<session>:t<epoch_ms>)
        # If implementation returned empty (shouldn't), synthesize a conservative id.
        if not new_id:
            new_id = f"{cm.session_id}:t{int(time.time() * 1000)}"
            cm.start_turn(new_id, agent=agent)
        ws.state.turn_id = new_id  # type: ignore[attr-defined]
        return new_id
    except Exception:
        logger.exception("Failed to start a new turn on cm")
        return None


# --------------------------------- public API ----------------------------------

async def send_tts_audio(
    text: str,
    ws: WebSocket,
    voice_name: Optional[str] = None,
    voice_style: Optional[str] = None,
    rate: Optional[str] = None,
) -> None:
    """
    Synthesize speech and send audio data to browser WebSocket client.

    Uses MemoManager's LatencyTracker (Suite v2) for comprehensive measurement:
      - "tts_e2e": Full pipeline from text to transmitted frames
      - "tts_synthesis": Speech synthesis only
      - "tts_streaming": Frame transmission to client
    
    Tracking precedence:
      1) If `cm` (MemoManager) is present, use Suite v2 async stages with turn correlation
      2) Else, log warning but continue (no measurement)
    """
    # Validate WebSocket connection
    if ws.client_state != WebSocketState.CONNECTED:
        logger.error("WebSocket is not connected, cannot send TTS audio")
        return

    if not text or not text.strip():
        logger.warning("Empty text provided for TTS synthesis")
        return

    cm = _get_cm(ws)
    turn_id = _ensure_turn(ws, cm, agent="tts")

    # Voice params
    style = voice_style or "chat"
    eff_rate = rate or "+3%"
    voice_to_use = voice_name or GREETING_VOICE_TTS

    # Acquire synthesizer (prefer per-connection)
    synth = getattr(ws.state, "tts_client", None)
    temp_synth = False

    try:
        if synth is None:
            synth = await ws.app.state.tts_pool.acquire()
            temp_synth = True
            logger.warning("Temporarily acquired TTS synthesizer from pool - session should have its own")

        ws.state.is_synthesizing = True  # type: ignore[attr-defined]
        logger.debug(
            "tts.start voice=%s style=%s rate=%s text_preview=%r turn_id=%s", 
            voice_to_use, style, eff_rate, text[:60], turn_id
        )

        # --- TRACK: tts_e2e (outer) + tts_synthesis (inner) ----------------
        if cm:
            async with cm.track("tts_e2e"):
                async with cm.track("tts_synthesis"):
                    pcm_bytes = synth.synthesize_to_pcm(
                        text=text,
                        voice=voice_to_use,
                        sample_rate=48000,
                        style=style,
                        rate=eff_rate,
                    )
                
                frames = SpeechSynthesizer.split_pcm_to_base64_frames(pcm_bytes, sample_rate=48000)

                # Track frame transmission separately
                async with cm.track("tts_streaming"):
                    await _send_audio_frames_to_browser(ws, frames)
        else:
            # No tracking available - proceed without measurement
            logger.warning("No MemoManager available for latency tracking in TTS")
            pcm_bytes = synth.synthesize_to_pcm(
                text=text,
                voice=voice_to_use,
                sample_rate=48000,
                style=style,
                rate=eff_rate,
            )
            frames = SpeechSynthesizer.split_pcm_to_base64_frames(pcm_bytes, sample_rate=48000)
            await _send_audio_frames_to_browser(ws, frames)

        logger.debug("tts.complete success=True frames_sent=%d turn_id=%s", len(frames), turn_id)

    except Exception as e:
        logger.error(f"TTS synthesis failed: {e}", extra={
            "turn_id": turn_id,
            "session_id": cm.session_id if cm else "unknown",
            "text_length": len(text),
            "voice": voice_to_use,
        })
        try:
            await ws.send_json(
                {
                    "type": "tts_error",
                    "error": str(e),
                    "text": text[:100] + "..." if len(text) > 100 else text,
                    "turn_id": turn_id,
                }
            )
        except Exception as send_error:
            logger.error(f"Failed to send error message to frontend: {send_error}")
    finally:
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


async def _send_audio_frames_to_browser(ws: WebSocket, frames: list) -> None:
    """Helper function to send audio frames to browser with proper error handling."""
    try:
        from opentelemetry import trace as _t  # optional
        _t.get_current_span().set_attribute("pipeline.stage", "tts -> websocket (browser)")
        _t.get_current_span().set_attribute("audio.frames_count", len(frames))
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


async def send_response_to_acs(
    ws: WebSocket,
    text: str,
    *,
    blocking: bool = False,
    stream_mode: StreamMode = ACS_STREAMING_MODE,
    voice_name: Optional[str] = None,
    voice_style: Optional[str] = None,
    rate: Optional[str] = None,
) -> Optional[asyncio.Task]:
    """
    Synthesize speech and send audio data to the ACS WebSocket.

    In MEDIA mode we synthesize locally and push frames.
    In TRANSCRIPTION mode we enqueue playback (no local synthesis here).

    Uses MemoManager's LatencyTracker (Suite v2) for measurement:
      - "acs_tts_e2e": Full ACS TTS pipeline 
      - "acs_tts_synthesis": Speech synthesis only
      - "acs_tts_streaming": Frame transmission to ACS
    """
    cm = _get_cm(ws)
    turn_id = _ensure_turn(ws, cm, agent="acs_tts")

    if stream_mode == StreamMode.MEDIA:
        # MEDIA: Synthesize locally then stream frames to ACS
        synth = getattr(ws.state, "tts_client", None)
        temp_synth = False

        try:
            if synth is None:
                synth = await ws.app.state.tts_pool.acquire()
                temp_synth = True
                logger.warning("ACS MEDIA: Temporarily acquired TTS synthesizer from pool - session should have its own")

            vt = voice_name or GREETING_VOICE_TTS
            st = voice_style or "chat"
            rr = rate or "+3%"

            if cm:
                async with cm.track("acs_tts_e2e"):
                    async with cm.track("acs_tts_synthesis"):
                        pcm_bytes = synth.synthesize_to_pcm(
                            text=text,
                            voice=vt,
                            sample_rate=16000,
                            style=st,
                            rate=rr,
                        )
                    
                    frames = SpeechSynthesizer.split_pcm_to_base64_frames(pcm_bytes, sample_rate=16000)
                    
                    # Track ACS frame transmission
                    async with cm.track("acs_tts_streaming"):
                        await _send_audio_frames_to_acs(ws, frames)
            else:
                # No tracking available - proceed without measurement
                logger.warning("No MemoManager available for latency tracking in ACS TTS")
                pcm_bytes = synth.synthesize_to_pcm(
                    text=text,
                    voice=vt,
                    sample_rate=16000,
                    style=st,
                    rate=rr,
                )
                frames = SpeechSynthesizer.split_pcm_to_base64_frames(pcm_bytes, sample_rate=16000)
                await _send_audio_frames_to_acs(ws, frames)

        except asyncio.TimeoutError:
            logger.error(f"TTS synthesis timed out for text: {text[:50]}...", extra={
                "turn_id": turn_id,
                "session_id": cm.session_id if cm else "unknown",
            })
            raise RuntimeError("TTS synthesis timed out")
        except Exception as e:
            logger.error(f"TTS synthesis failed: {e}", extra={
                "turn_id": turn_id,
                "session_id": cm.session_id if cm else "unknown",
            })
            raise RuntimeError(f"TTS synthesis failed: {e}")
        finally:
            if temp_synth and synth:
                try:
                    await ws.app.state.tts_pool.release(synth)
                except Exception as e:
                    logger.error(f"Error releasing temporary TTS synthesizer: {e}")

        # MEDIA mode completes synchronously
        return None

    # TRANSCRIPTION MODE
    acs_caller = ws.app.state.acs_caller
    if not acs_caller:
        raise RuntimeError("ACS caller is not initialized in WebSocket state.")

    # playback through queue (no local synth here)
    target_participant = getattr(ws.state, "target_participant", None)

    async def _done(task: asyncio.Task):
        # Clean up task tracking
        if hasattr(ws.state, "tts_tasks"):
            try:
                ws.state.tts_tasks.discard(task)
            except Exception:
                pass

    # Track transcription mode TTS if cm available
    if cm:
        async with cm.track("acs_tts_transcription"):
            coro = play_response_with_queue(
                ws=ws, response_text=text, participants=[target_participant] if target_participant else None
            )
    else:
        coro = play_response_with_queue(
            ws=ws, response_text=text, participants=[target_participant] if target_participant else None
        )

    if not hasattr(ws.state, "tts_tasks"):
        ws.state.tts_tasks = set()

    task = asyncio.create_task(coro)
    ws.state.tts_tasks.add(task)
    task.add_done_callback(lambda t: asyncio.create_task(_done(t)))
    return task


async def _send_audio_frames_to_acs(ws: WebSocket, frames: list) -> None:
    """Helper function to send audio frames to ACS WebSocket with proper error handling."""
    for frame in frames:
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
# Latency retrieval helpers (optional)
# --------------------------------------------------------------------------- #

def get_current_turn_id(ws: WebSocket) -> Optional[str]:
    """Convenience: current turn id on the connection (if any)."""
    return getattr(ws.state, "turn_id", None)


def get_latency_summary_from_cm(ws: WebSocket) -> Optional[dict]:
    """
    Return in-memory latency aggregates for this connection (per-session),
    including percentiles per component and per agent.
    """
    cm = _get_cm(ws)
    if not cm:
        return None
    return cm.get_latency_summary()  # aggregates only (no raw)


def get_latency_per_turn_from_cm(ws: WebSocket) -> Optional[dict]:
    """
    Build a per-turn breakdown from in-memory samples.
    NOTE: this uses in-memory raw measurements and does NOT hit Redis.
    """
    cm = _get_cm(ws)
    if not cm or not getattr(cm, "latency_tracker", None):
        return None

    s = cm.latency_tracker.get_summary(include_raw=True)  # type: ignore[attr-defined]
    ms = s.get("measurements", [])
    if not ms:
        return {"turns": {}, "total_turns": 0}

    turns: dict = {}
    for m in ms:
        tid = m.get("turn_id") or "unknown"
        comp = m.get("component") or "unknown"
        turns.setdefault(tid, {}).setdefault(comp, []).append(m["duration_ms"])

    def _reduce(vals):
        vs = sorted(vals)
        n = len(vs)
        p = lambda q: vs[int(q * (n - 1))] if n else 0.0
        return {
            "count": n,
            "avg_ms": round(sum(vs) / n, 2) if n else 0.0,
            "min_ms": round(vs[0], 2) if n else 0.0,
            "max_ms": round(vs[-1], 2) if n else 0.0,
            "p50_ms": round(p(0.5), 2),
            "p90_ms": round(p(0.9), 2),
            "p95_ms": round(p(0.95), 2),
            "p99_ms": round(p(0.99), 2),
        }

    out = {tid: {c: _reduce(vs) for c, vs in comps.items()} for tid, comps in turns.items()}
    return {"turns": out, "total_turns": len(out)}


def get_latency_summary_from_redis(redis_mgr, session_id: str) -> Optional[dict]:
    """
    Read persisted aggregates from Redis for a given session_id.
    This returns the same structure stored under key 'latency_data' in MemoManager.
    Per-turn breakdown is not persisted by default (to keep payload small).
    """
    try:
        key = MemoManager.build_redis_key(session_id)
        blob = redis_mgr.get_session_data(key)  # {'corememory': ..., 'chat_history': ..., 'latency_data': ...}
        if not blob or "latency_data" not in blob:
            return None
        return json.loads(blob["latency_data"])
    except Exception:
        logger.exception("Failed to fetch latency_data from Redis")
        return None


# --------------------------------------------------------------------------- #
# Re-exports
# --------------------------------------------------------------------------- #
__all__ = [
    "send_tts_audio",
    "send_response_to_acs",
    "push_final",
    "broadcast_message",
    "get_current_turn_id",
    "get_latency_summary_from_cm",
    "get_latency_per_turn_from_cm",
    "get_latency_summary_from_redis",
]
