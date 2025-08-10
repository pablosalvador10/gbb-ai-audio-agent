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
    voice: Optional[str] = None
) -> None:
    """
    Synthesize speech and send audio data to browser WebSocket client.

    Args:
        text: Text to synthesize
        ws: WebSocket connection
        latency_tool: Optional latency tracking tool
        voice: Optional voice name to use (defaults to global GREETING_VOICE_TTS)

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
        logger.debug("Synthesizing audio for TTS...")
        logger.info(f"is_synthesizing flag set: {ws.state.is_synthesizing}")
        
        # Use provided voice or fall back to global setting
        tts_voice = voice or GREETING_VOICE_TTS
        logger.debug(f"Using voice: {tts_voice}")
        
        # Check for cancellation before synthesis
        if getattr(ws.state, "tts_cancel", False):
            logger.info("TTS canceled before synthesis started")
            return
        
        # For browser/realtime: use synthesize_to_pcm and convert to WAV in memory
        logger.info("Synthesizing high-quality audio for browser playback...")
        
        # Get raw PCM at 24kHz (good quality for browser)
        pcm_bytes = synth.synthesize_to_pcm(text=text, voice=tts_voice, sample_rate=24000)
        
        # Check for cancellation after synthesis
        if getattr(ws.state, "tts_cancel", False):
            logger.info("TTS canceled after synthesis, skipping transmission")
            return
        
        # Create proper WAV header for the PCM data
        import struct
        import io
        
        # WAV file parameters
        sample_rate = 24000
        bits_per_sample = 16
        channels = 1
        byte_rate = sample_rate * channels * bits_per_sample // 8
        block_align = channels * bits_per_sample // 8
        data_size = len(pcm_bytes)
        file_size = 36 + data_size
        
        # Create WAV file in memory
        wav_buffer = io.BytesIO()
        
        # WAV header
        wav_buffer.write(b'RIFF')                          # Chunk ID
        wav_buffer.write(struct.pack('<L', file_size))     # File size - 8
        wav_buffer.write(b'WAVE')                          # Format
        wav_buffer.write(b'fmt ')                          # Subchunk1 ID
        wav_buffer.write(struct.pack('<L', 16))            # Subchunk1 size
        wav_buffer.write(struct.pack('<H', 1))             # Audio format (PCM)
        wav_buffer.write(struct.pack('<H', channels))      # Number of channels
        wav_buffer.write(struct.pack('<L', sample_rate))   # Sample rate
        wav_buffer.write(struct.pack('<L', byte_rate))     # Byte rate
        wav_buffer.write(struct.pack('<H', block_align))   # Block align
        wav_buffer.write(struct.pack('<H', bits_per_sample)) # Bits per sample
        wav_buffer.write(b'data')                          # Subchunk2 ID
        wav_buffer.write(struct.pack('<L', data_size))     # Subchunk2 size
        wav_buffer.write(pcm_bytes)                        # Audio data
        
        wav_bytes = wav_buffer.getvalue()
        
        if latency_tool:
            latency_tool.stop("tts:synthesis", ws.app.state.redis)
        
        # Convert WAV to base64 for WebSocket transmission
        wav_base64 = base64.b64encode(wav_bytes).decode('utf-8')
        
        # Send complete audio as single payload (much better for browsers)
        try:
            # Calculate audio duration for proper is_synthesizing timing
            sample_rate = 24000
            bytes_per_sample = 2  # 16-bit PCM
            audio_duration_seconds = len(pcm_bytes) / (sample_rate * bytes_per_sample)
            
            await ws.send_json({
                "type": "audio",
                "format": "base64",
                "data": wav_base64,
                "mimeType": "audio/wav",
                "voice": tts_voice,
                "duration": audio_duration_seconds,
                "text_preview": text[:50] + "..." if len(text) > 50 else text
            })
            logger.debug(f"Sent WAV audio to browser ({len(wav_bytes)} bytes, {sample_rate}Hz, {audio_duration_seconds:.2f}s)")
            
            # IMPORTANT: Keep is_synthesizing=True for the duration of audio playback
            # Schedule flag reset after audio completes in browser
            async def reset_synthesizing_after_playback():
                # Add generous buffer: network latency + browser processing + actual playback
                total_wait_time = audio_duration_seconds + 2.0  # Increased buffer for reliable timing
                await asyncio.sleep(total_wait_time)
                try:
                    # Only reset if not already canceled by VAD
                    if not getattr(ws.state, "tts_cancel", False):
                        ws.state.is_synthesizing = False
                        logger.info(f"✅ TTS playback completed naturally, is_synthesizing reset to False after {total_wait_time:.2f}s")
                    else:
                        logger.info("✅ TTS playback timer completed, but was already canceled by VAD")
                except Exception as e:
                    logger.warning(f"Error resetting is_synthesizing flag: {e}")
            
            # Don't reset is_synthesizing in finally block - let the timer handle it
            asyncio.create_task(reset_synthesizing_after_playback())
            
        except Exception as e:
            logger.error("Failed to send WAV audio to browser: %s", e)
            # Reset flag immediately on error
            ws.state.is_synthesizing = False
            raise

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
        # Don't reset is_synthesizing here - let the timer handle it for proper VAD timing
        # Only reset tts_cancel and latency tracking
        try:
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
    voice: Optional[str] = None,
) -> Optional[asyncio.Task]:
    """
    Synthesizes speech and sends it as audio data to the ACS WebSocket.

    Args:
        ws: WebSocket connection
        text: Text to synthesize
        blocking: Whether to block on completion
        latency_tool: Optional latency tracking tool
        stream_mode: Stream mode for ACS
        voice: Optional voice name to use (defaults to global GREETING_VOICE_TTS)

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
            # Use provided voice or fall back to global setting
            tts_voice = voice or GREETING_VOICE_TTS
            logger.debug(f"Using voice for ACS: {tts_voice}")
            
            pcm_bytes = synth.synthesize_to_pcm(
                text=text, voice=tts_voice, sample_rate=16000
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
