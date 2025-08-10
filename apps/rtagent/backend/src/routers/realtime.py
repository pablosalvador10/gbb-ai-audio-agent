"""
routers/realtime.py
===================
• `/relay`     – dashboard broadcast WebSocket
• `/realtime`  – browser/WebRTC conversation endpoint

Relies on:
    utils.helpers.receive_and_filter
    orchestration.gpt_flow.route_turn
"""

# routers/realtime.py

from __future__ import annotations

import asyncio
import json
import time
import uuid
from typing import Iterator, List, Optional

import numpy as np
import torch
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from apps.rtagent.backend.settings import GREETING
from apps.rtagent.backend.src.helpers import check_for_stopwords, receive_and_filter
from apps.rtagent.backend.src.latency.latency_tool import LatencyTool
from apps.rtagent.backend.src.orchestration.orchestrator import route_turn
from apps.rtagent.backend.src.shared_ws import broadcast_message, send_tts_audio
from src.postcall.push import build_and_flush
from src.stateful.state_managment import MemoManager
from utils.ml_logging import get_logger

from src.vad.vad_iterator import VADIterator

logger = get_logger("realtime_router")
router = APIRouter()

# ============================ VAD INTEGRATION ============================ #
try:

    from silero_vad import load_silero_vad  # type: ignore
except Exception as exc:  # pragma: no cover
    logger.error("silero-vad not installed. `pip install silero-vad torch`")
    raise

VAD_THRESH: float = 0.66            # 0.60–0.70 typical; raise in noisy envs
VAD_START_FRAMES: int = 2           # need 2 × 32ms frames to confirm start (~64ms)
VAD_END_SIL_MS: int = 450           # min silence to decide end-of-speech
VAD_SPEECH_PAD_MS: int = 120        # padding used by Silero iterator
FRAME_BYTES: int = 1024             # 32ms @ 16kHz, mono, PCM16
BARGE_DEBOUNCE_MS: int = 250        # don't spam stop_speaking()
COOLDOWN_AFTER_BARGE_MS: int = 450  # drop STT bytes briefly after barge to absorb TTS tail
COOLDOWN_AFTER_END_MS: int = 120    # brief hold after end before pushing STT again
PARTIAL_FALLBACK: bool = False      # cut on partials too (optional, default off)


def _int16_to_float32(raw: bytes) -> torch.Tensor:
    """
    Convert PCM16 mono bytes -> float32 tensor in [-1, 1] with shape [T] or [1, T].
    """
    arr = np.frombuffer(raw, dtype=np.int16).astype("float32") * (1.0 / 32768.0)
    return torch.from_numpy(arr)


class _PCMFramer:
    """
    Accumulate arbitrary bytes and yield fixed-size frames (1024 bytes by default).
    1024 bytes == 512 samples @ 16 kHz == ~32 ms, which Silero accepts (>=512).
    """

    def __init__(self, frame_bytes: int = FRAME_BYTES) -> None:
        self._buf = bytearray()
        self._n = int(frame_bytes)

    def feed(self, chunk: bytes) -> Iterator[bytes]:
        if not chunk:
            return iter(())
        self._buf.extend(chunk)
        out: List[bytes] = []
        while len(self._buf) >= self._n:
            out.append(bytes(self._buf[: self._n]))
            del self._buf[: self._n]
        return iter(out)


class VadGate:
    """
    Half-duplex controller using Silero VAD.

    - While TTS is speaking (ws.state.is_synthesizing), we *mute* STT bytes until VAD confirms the user started.
    - On VAD start => stop TTS (debounced) + short cooldown (drops tail).
    - On VAD end   => clear latches so STT/TTS can resume.
    """

    def __init__(self, ws: WebSocket) -> None:
        """
        :param ws: Current websocket; expects ws.app.state.stt_client / tts_client and
                   ws.state.is_synthesizing bool to be maintained by TTS calls.
        """
        self.ws = ws
        self.framer = _PCMFramer(FRAME_BYTES)
        model = load_silero_vad()
        self.vad = VADIterator(
            model,
            threshold=VAD_THRESH,
            sampling_rate=16000,
            min_silence_duration_ms=VAD_END_SIL_MS,
            speech_pad_ms=VAD_SPEECH_PAD_MS,
        )
        self.vad_started: bool = False
        self.vad_trigs: int = 0
        self.cooldown_until_ms: int = 0
        self.last_barge_ms: int = 0

    def _now_ms(self) -> int:
        return int(time.time() * 1000)

    def _stop_tts_if_needed(self) -> None:
        """
        Debounced, idempotent cut of current TTS.
        """
        now = self._now_ms()
        if now - self.last_barge_ms < BARGE_DEBOUNCE_MS:
            return
        self.last_barge_ms = now

        if getattr(self.ws.state, "is_synthesizing", False):
            try:
                self.ws.app.state.tts_client.stop_speaking()
                self.ws.state.is_synthesizing = False
                logger.info("🛑 TTS interrupted (VAD barge-in)")
            except Exception as e:  # pragma: no cover
                logger.error("TTS stop failed: %s", e, exc_info=True)

        # Drop a bit of mic->STT right after the barge to avoid feeding TTS tail to STT
        self.cooldown_until_ms = now + COOLDOWN_AFTER_BARGE_MS

    def process_bytes(self, data: bytes) -> None:
        """
        Main hot path. Call this instead of directly writing to STT.

        :param data: Raw PCM16 mono @ 16kHz bytes from client mic.
        """
        now = self._now_ms()

        # Decide whether to forward to STT this chunk (half-duplex gating)
        push_ok = True
        if (
            getattr(self.ws.state, "is_synthesizing", False)
            and not self.vad_started
            and now >= self.cooldown_until_ms
        ):
            # TTS speaking, no barge yet → mute STT feed
            push_ok = False

        if now < self.cooldown_until_ms:
            push_ok = False

        if push_ok:
            # forward to Azure STT
            try:
                self.ws.app.state.stt_client.write_bytes(data)
            except Exception as e:  # pragma: no cover
                logger.warning("STT write_bytes failed: %s", e, exc_info=True)

        # Always run local VAD on 32ms subframes
        for frame in self.framer.feed(data):
            x = _int16_to_float32(frame).unsqueeze(0)  # [1, T]
            try:
                seg = self.vad(x)  # updates self.vad.triggered internally
            except Exception as e:  # pragma: no cover
                logger.debug("VAD error (skipping frame): %s", e)
                continue

            if getattr(self.vad, "triggered", False) and not self.vad_started:
                self.vad_trigs += 1
                if self.vad_trigs >= VAD_START_FRAMES:
                    self.vad_started = True
                    self.vad_trigs = 0
                    self._stop_tts_if_needed()
                    logger.info("VAD start")
            elif not getattr(self.vad, "triggered", False):
                self.vad_trigs = 0

            # END-of-speech: Silero returns a segment right after it decides "end"
            if seg is not None and self.vad_started:
                self.vad_started = False
                # short hold before letting STT fully flow again
                self.cooldown_until_ms = self._now_ms() + COOLDOWN_AFTER_END_MS
                logger.info("VAD end")

# ======================== END VAD INTEGRATION ======================== #


@router.websocket("/ws/relay")
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
    finally:
        if ws.application_state.name == "CONNECTED" and ws.client_state.name not in (
            "DISCONNECTED",
            "CLOSED",
        ):
            await ws.close()


@router.websocket("/realtime")
async def realtime_ws(ws: WebSocket):
    """
    Browser/WebRTC client sends audio bytes + STT events; we stream GPT + TTS back.
    VAD (server-side) controls barge-in and end-of-speech.
    """
    try:
        await ws.accept()
        session_id = uuid.uuid4().hex[:8]

        redis_mgr = ws.app.state.redis
        cm = MemoManager.from_redis(session_id, redis_mgr)
        ws.state.cm = cm
        ws.state.session_id = session_id
        ws.state.lt = LatencyTool(cm)
        ws.state.is_synthesizing = False  
        ws.state.user_buffer = ""

        # Initialize VAD for this session
        ws.state.vad_gate = VadGate(ws)

        await ws.send_text(json.dumps({"type": "status", "message": GREETING}))
        auth_agent = ws.app.state.auth_agent
        cm.append_to_history(auth_agent.name, "assistant", GREETING)

        # Mark synthesizing around TTS, so VAD can gate STT
        ws.state.is_synthesizing = True
        await send_tts_audio(GREETING, ws, latency_tool=ws.state.lt)
        ws.state.is_synthesizing = False

        await cm.persist_to_redis_async(redis_mgr)

        # ---------------- STT callbacks ---------------- #
        def on_partial(txt: str, lang: str):
            logger.info(f"🗣️ User (partial) in {lang}: {txt}")
            if PARTIAL_FALLBACK and ws.state.is_synthesizing:
                # Optional fallback in case VAD missed the start
                try:
                    ws.app.state.tts_client.stop_speaking()
                    ws.state.is_synthesizing = False
                    logger.info("🛑 TTS interrupted via STT partial (fallback)")
                except Exception as e:
                    logger.error(f"Error stopping TTS: {e}", exc_info=True)
            asyncio.create_task(
                ws.send_text(json.dumps({"type": "assistant_streaming", "content": txt}))
            )

        ws.app.state.stt_client.set_partial_result_callback(on_partial)

        def on_final(txt: str, lang: str):
            logger.info(f"🧾 User (final) in {lang}: {txt}")
            ws.state.user_buffer += (txt or "").strip() + "\n"

        ws.app.state.stt_client.set_final_result_callback(on_final)
        ws.app.state.stt_client.start()
        logger.info("STT recognizer started for session %s", session_id)

        # ---------------- main ws loop ---------------- #
        while True:
            msg = await ws.receive()  # can be text or bytes
            if msg.get("type") == "websocket.receive" and msg.get("bytes") is not None:
                try:
                    ws.state.vad_gate.process_bytes(msg["bytes"])
                except Exception as e:
                    logger.warning("VAD gate error: %s", e, exc_info=True)

                # When a final result arrives (from STT callbacks), run a turn
                if ws.state.user_buffer.strip():
                    prompt = ws.state.user_buffer.strip()
                    ws.state.user_buffer = ""

                    await ws.send_text(json.dumps({"sender": "User", "message": prompt}))

                    if check_for_stopwords(prompt):
                        goodbye = "Thank you for using our service. Goodbye."
                        await ws.send_text(json.dumps({"type": "exit", "message": goodbye}))
                        ws.state.is_synthesizing = True
                        await send_tts_audio(goodbye, ws, latency_tool=ws.state.lt)
                        ws.state.is_synthesizing = False
                        break

                    # Orchestrate GPT+TTS; ensure is_synthesizing toggles are respected inside that flow
                    await route_turn(cm, prompt, ws, is_acs=False)
                continue

            if msg.get("type") == "websocket.disconnect":
                break

    finally:
        # Best effort cleanup
        try:
            ws.app.state.tts_client.stop_speaking()
            ws.state.is_synthesizing = False
        except Exception:
            pass
        try:
            if (
                ws.application_state.name == "CONNECTED"
                and ws.client_state.name not in ("DISCONNECTED", "CLOSED")
            ):
                await ws.close()
        except Exception as e:
            logger.warning(f"WebSocket close error: {e}", exc_info=True)
        try:
            cm = getattr(ws.state, "cm", None)
            cosmos = getattr(ws.app.state, "cosmos", None)
            if cm and cosmos:
                build_and_flush(cm, cosmos)
        except Exception as e:
            logger.error(f"Error persisting analytics: {e}", exc_info=True)

