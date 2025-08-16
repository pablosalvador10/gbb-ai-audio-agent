# apps/rtagent/backend/api/v1/handlers/acs_media_lifecycle.py
"""
V1 ACS Media Handler - Three-Thread Architecture
================================================

Low-latency media orchestration with:
- Speech SDK thread (never blocks)
- Route-Turn thread (blocks only on queue)
- Main event loop (never blocks)
"""

from __future__ import annotations

import asyncio
import base64
import json
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable, Union, Set

from fastapi import WebSocket
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from apps.rtagent.backend.settings import GREETING
from apps.rtagent.backend.src.shared_ws import send_response_to_acs
from apps.rtagent.backend.src.orchestration.orchestrator import route_turn
from apps.rtagent.backend.src.services import StreamingSpeechRecognizerFromBytes
from src.enums.stream_modes import StreamMode
from src.stateful.state_managment import MemoManager
from utils.ml_logging import get_logger

logger = get_logger("v1.handlers.acs_media_lifecycle")
tracer = trace.get_tracer(__name__)


# -----------------------------------------------------------------------------
# Speech Event Model
# -----------------------------------------------------------------------------
class SpeechEventType(Enum):
    PARTIAL = "partial"
    FINAL = "final"
    ERROR = "error"
    GREETING = "greeting"
    ANNOUNCEMENT = "announcement"
    STATUS_UPDATE = "status"
    ERROR_MESSAGE = "error_msg"


@dataclass
class SpeechEvent:
    event_type: SpeechEventType
    text: str
    language: Optional[str] = None
    speaker_id: Optional[str] = None
    confidence: Optional[float] = None
    timestamp: float = field(default_factory=time.time)


# -----------------------------------------------------------------------------
# Thread bridge (cross-thread scheduling)
# -----------------------------------------------------------------------------
class ThreadBridge:
    def __init__(self, main_loop: Optional[asyncio.AbstractEventLoop] = None):
        self.main_loop = main_loop

    def set_main_loop(self, loop: asyncio.AbstractEventLoop):
        self.main_loop = loop

    def schedule_barge_in(self, handler_func: Callable):
        if not self.main_loop or self.main_loop.is_closed():
            logger.warning("No main event loop available for barge-in scheduling")
            return
        try:
            asyncio.run_coroutine_threadsafe(handler_func(), self.main_loop)
        except Exception as e:
            logger.error(f"Failed to schedule barge-in: {e}")

    def queue_speech_result(self, speech_queue: asyncio.Queue, event: SpeechEvent):
        try:
            speech_queue.put_nowait(event)
            return
        except asyncio.QueueFull:
            pass
        if not self.main_loop or self.main_loop.is_closed():
            logger.error("No main loop for speech result queuing")
            return
        try:
            fut = asyncio.run_coroutine_threadsafe(speech_queue.put(event), self.main_loop)
            fut.result(timeout=0.1)
        except Exception as e:
            logger.error(f"Failed to queue speech result: {e}")


# -----------------------------------------------------------------------------
# Speech SDK thread (never blocks)
# -----------------------------------------------------------------------------
class SpeechSDKThread:
    def __init__(
        self,
        recognizer: StreamingSpeechRecognizerFromBytes,
        thread_bridge: ThreadBridge,
        barge_in_handler: Callable,
        speech_queue: asyncio.Queue,
    ):
        self.recognizer = recognizer
        self.thread_bridge = thread_bridge
        self.barge_in_handler = barge_in_handler
        self.speech_queue = speech_queue

        self.thread_obj: Optional[threading.Thread] = None
        self.thread_running = False
        self.recognizer_started = False
        self.stop_event = threading.Event()
        self._stopped = False

        self._setup_callbacks()

    def _setup_callbacks(self):
        def on_partial(text: str, lang: str, speaker_id: Optional[str] = None):
            self.thread_bridge.schedule_barge_in(self.barge_in_handler)

        def on_final(text: str, lang: str, speaker_id: Optional[str] = None):
            self.thread_bridge.queue_speech_result(
                self.speech_queue,
                SpeechEvent(event_type=SpeechEventType.FINAL, text=text, language=lang, speaker_id=speaker_id),
            )

        def on_error(error: str):
            self.thread_bridge.queue_speech_result(
                self.speech_queue, SpeechEvent(event_type=SpeechEventType.ERROR, text=error)
            )

        self.recognizer.set_partial_result_callback(on_partial)
        self.recognizer.set_final_result_callback(on_final)
        self.recognizer.set_cancel_callback(on_error)

    def prepare_thread(self):
        if self.thread_running:
            return

        def _thread():
            self.thread_running = True
            try:
                while self.thread_running and not self.stop_event.is_set():
                    self.stop_event.wait(0.1)
            finally:
                self.thread_running = False

        self.thread_obj = threading.Thread(target=_thread, daemon=True)
        self.thread_obj.start()

    def start_recognizer(self):
        if self.recognizer_started or not self.thread_running:
            return
        try:
            self.recognizer.start()
            self.recognizer_started = True
        except Exception as e:
            logger.error(f"Failed to start recognizer: {e}")
            raise

    def stop(self):
        if self._stopped:
            return
        self._stopped = True
        self.thread_running = False
        self.recognizer_started = False
        self.stop_event.set()
        try:
            self.recognizer.stop()
        except Exception:
            pass
        if self.thread_obj and self.thread_obj.is_alive():
            self.thread_obj.join(timeout=2.0)


# -----------------------------------------------------------------------------
# Route Turn thread (blocks only on queue)
# -----------------------------------------------------------------------------
class RouteTurnThread:
    def __init__(
        self,
        speech_queue: asyncio.Queue,
        orchestrator_func: Callable,
        memory_manager: Optional[MemoManager],
        websocket: WebSocket,
    ):
        self.speech_queue = speech_queue
        self.orchestrator_func = orchestrator_func
        self.memory_manager = memory_manager
        self.websocket = websocket
        self.processing_task: Optional[asyncio.Task] = None
        self.current_response_task: Optional[asyncio.Task] = None
        self.running = False
        self._stopped = False

    async def start(self):
        if self.running:
            return
        self.running = True
        self.processing_task = asyncio.create_task(self._processing_loop())

    async def _processing_loop(self):
        with tracer.start_as_current_span("v1.route_turn_thread.processing_loop", kind=SpanKind.INTERNAL):
            while self.running:
                try:
                    speech_event: SpeechEvent = await asyncio.wait_for(self.speech_queue.get(), timeout=1.0)
                    if speech_event.event_type == SpeechEventType.FINAL:
                        await self._process_final_speech(speech_event)
                    elif speech_event.event_type in (
                        SpeechEventType.GREETING,
                        SpeechEventType.ANNOUNCEMENT,
                        SpeechEventType.STATUS_UPDATE,
                        SpeechEventType.ERROR_MESSAGE,
                    ):
                        await self._process_direct_text_playback(speech_event, speech_event.event_type.value)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Route Turn loop error: {e}")
                    break

    async def _process_final_speech(self, event: SpeechEvent):
        with tracer.start_as_current_span(
            "v1.route_turn_thread.process_speech",
            kind=SpanKind.CLIENT,
            attributes={"speech.language": event.language or "unknown"},
        ):
            if not self.memory_manager:
                logger.error("Memory manager is None")
                return
            try:
                if self.orchestrator_func:
                    await self.orchestrator_func(
                        cm=self.memory_manager,
                        transcript=event.text,
                        ws=self.websocket,
                        call_id=getattr(self.websocket, "_call_connection_id", None),
                        is_acs=True,
                    )
                else:
                    await route_turn(cm=self.memory_manager, transcript=event.text, ws=self.websocket, is_acs=True)
            except Exception as e:
                logger.error(f"Orchestrator error: {e}")

    async def _process_direct_text_playback(self, event: SpeechEvent, playback_type: str):
        with tracer.start_as_current_span(
            "v1.route_turn_thread.process_direct_text_playback",
            kind=SpanKind.CLIENT,
            attributes={"playback.type": playback_type},
        ):
            try:
                lt = getattr(self.websocket.state, "lt", None)
                self.current_response_task = asyncio.create_task(
                    send_response_to_acs(
                        ws=self.websocket,
                        text=event.text,
                        blocking=False,
                        latency_tool=lt,
                        stream_mode=StreamMode.MEDIA,
                        voice_name=None,
                        voice_style=None,
                    )
                )
                await self.current_response_task
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Playback error: {e}")
            finally:
                self.current_response_task = None

    async def cancel_current_processing(self):
        try:
            while not self.speech_queue.empty():
                try:
                    self.speech_queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
            if self.current_response_task and not self.current_response_task.done():
                self.current_response_task.cancel()
                try:
                    await self.current_response_task
                except asyncio.CancelledError:
                    pass
        except Exception as e:
            logger.error(f"cancel_current_processing error: {e}")

    async def stop(self):
        if self._stopped:
            return
        self._stopped = True
        self.running = False
        await self.cancel_current_processing()
        if self.processing_task and not self.processing_task.done():
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass


# -----------------------------------------------------------------------------
# Main event loop helper (never blocks)
# -----------------------------------------------------------------------------
class MainEventLoop:
    def __init__(
        self,
        websocket: WebSocket,
        call_connection_id: str,
        route_turn_thread: Optional["RouteTurnThread"] = None,
    ):
        self.websocket = websocket
        self.call_connection_id = call_connection_id
        self.route_turn_thread = route_turn_thread
        self.current_playback_task: Optional[asyncio.Task] = None
        self.barge_in_active = threading.Event()
        self.greeting_played = False
        self.active_audio_tasks: Set[asyncio.Task] = set()
        self.max_concurrent_audio_tasks = 50
        self.last_queue_health_log = 0.0

    async def handle_barge_in(self):
        with tracer.start_as_current_span("v1.main_event_loop.handle_barge_in", kind=SpanKind.INTERNAL):
            if self.barge_in_active.is_set():
                return
            self.barge_in_active.set()
            try:
                await self._cancel_current_playback()
                if self.route_turn_thread:
                    await self.route_turn_thread.cancel_current_processing()
                await self._send_stop_audio_command()
            finally:
                asyncio.create_task(self._reset_barge_in_state())

    async def _cancel_current_playback(self):
        if self.current_playback_task and not self.current_playback_task.done():
            self.current_playback_task.cancel()
            try:
                await self.current_playback_task
            except asyncio.CancelledError:
                pass

    async def _send_stop_audio_command(self):
        try:
            await self.websocket.send_text(json.dumps({"Kind": "StopAudio", "AudioData": None, "StopAudio": {}}))
        except Exception as e:
            logger.error(f"Failed to send stop audio: {e}")

    async def _reset_barge_in_state(self):
        await asyncio.sleep(0.1)
        self.barge_in_active.clear()

    async def handle_media_message(self, stream_data: str, recognizer, acs_handler):
        try:
            data = json.loads(stream_data)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in media message: {e}")
            return

        kind = data.get("kind")
        if kind == "AudioMetadata":
            if acs_handler and acs_handler.speech_sdk_thread:
                acs_handler.speech_sdk_thread.start_recognizer()
            if not self.greeting_played:
                await self._play_greeting_when_ready(acs_handler)
            return

        if kind != "AudioData":
            return

        audio_section = data.get("audioData", {})
        if audio_section.get("silent", True):
            return

        audio_bytes = audio_section.get("data")
        if not audio_bytes:
            return

        if not recognizer or not hasattr(recognizer, "write_bytes"):
            logger.error("Recognizer not available")
            return

        if len(self.active_audio_tasks) >= self.max_concurrent_audio_tasks:
            return

        task = asyncio.create_task(self._process_audio_chunk_async(audio_bytes, recognizer))
        self.active_audio_tasks.add(task)
        task.add_done_callback(lambda t: self.active_audio_tasks.discard(t))

    async def _process_audio_chunk_async(self, audio_bytes: Union[str, bytes], recognizer) -> None:
        try:
            if isinstance(audio_bytes, str):
                audio_bytes = base64.b64decode(audio_bytes)
            await asyncio.wait_for(
                asyncio.get_running_loop().run_in_executor(None, recognizer.write_bytes, audio_bytes),
                timeout=0.05,
            )
        except asyncio.TimeoutError:
            logger.error("recognizer.write_bytes timeout")
        except Exception as e:
            logger.error(f"Audio chunk processing error: {e}")

    async def _play_greeting_when_ready(self, acs_handler=None):
        if self.greeting_played or not acs_handler:
            return
        greeting_text = getattr(acs_handler, "greeting_text", None)
        if not greeting_text:
            self.greeting_played = True
            return
        try:
            acs_handler.thread_bridge.queue_speech_result(
                acs_handler.speech_queue,
                SpeechEvent(event_type=SpeechEventType.GREETING, text=greeting_text, language="en-US"),
            )
        finally:
            self.greeting_played = True


# -----------------------------------------------------------------------------
# Coordinator
# -----------------------------------------------------------------------------
class ACSMediaHandler:
    """Three-thread architecture coordinator."""

    def __init__(
        self,
        websocket: WebSocket,
        orchestrator_func: Callable,
        call_connection_id: str,
        recognizer: Optional[StreamingSpeechRecognizerFromBytes] = None,
        memory_manager: Optional[MemoManager] = None,
        session_id: Optional[str] = None,
        greeting_text: str = GREETING,
    ):
        self.websocket = websocket
        self.orchestrator_func = orchestrator_func
        self.call_connection_id = call_connection_id
        self.session_id = session_id or call_connection_id
        self.memory_manager = memory_manager
        self.greeting_text = greeting_text

        self.recognizer = recognizer or StreamingSpeechRecognizerFromBytes(
            candidate_languages=["en-US"],
            vad_silence_timeout_ms=800,
            audio_format="pcm",
        )

        self.speech_queue = asyncio.Queue(maxsize=10)
        self.thread_bridge = ThreadBridge()

        self.route_turn_thread = RouteTurnThread(
            speech_queue=self.speech_queue,
            orchestrator_func=orchestrator_func,
            memory_manager=memory_manager,
            websocket=websocket,
        )

        self.main_event_loop = MainEventLoop(websocket, call_connection_id, self.route_turn_thread)

        self.speech_sdk_thread = SpeechSDKThread(
            recognizer=self.recognizer,
            thread_bridge=self.thread_bridge,
            barge_in_handler=self.main_event_loop.handle_barge_in,
            speech_queue=self.speech_queue,
        )

        self.running = False
        self._stopped = False

    async def start(self):
        with tracer.start_as_current_span(
            "v1.acs_media_handler.start",
            kind=SpanKind.INTERNAL,
            attributes={
                "call.connection.id": self.call_connection_id,
                "session.id": self.session_id,
                "architecture": "three_thread_coordinated",
            },
        ) as span:
            try:
                self.running = True
                self.thread_bridge.set_main_loop(asyncio.get_running_loop())
                self.websocket._acs_media_handler = self  # optional convenience

                self.speech_sdk_thread.prepare_thread()
                await self.route_turn_thread.start()

                span.set_status(Status(StatusCode.OK))
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                await self.stop()
                raise

    async def handle_media_message(self, stream_data: str):
        try:
            await self.main_event_loop.handle_media_message(stream_data, self.recognizer, self)
        except Exception as e:
            logger.error(f"handle_media_message error: {e}")

    async def stop(self):
        if self._stopped:
            return
        with tracer.start_as_current_span("v1.acs_media_handler.stop", kind=SpanKind.INTERNAL) as span:
            try:
                self._stopped = True
                self.running = False
                await self.route_turn_thread.stop()
                self.speech_sdk_thread.stop()
                await self.main_event_loop._cancel_current_playback()
                span.set_status(Status(StatusCode.OK))
            except Exception as e:
                span.set_status(Status(StatusCode.ERROR, str(e)))

    @property
    def is_running(self) -> bool:
        return self.running

    # convenience for external callers
    def queue_direct_text_playback(
        self,
        text: str,
        playback_type: SpeechEventType = SpeechEventType.ANNOUNCEMENT,
        language: str = "en-US",
    ) -> bool:
        if not self.running:
            return False
        if playback_type not in {
            SpeechEventType.GREETING,
            SpeechEventType.ANNOUNCEMENT,
            SpeechEventType.STATUS_UPDATE,
            SpeechEventType.ERROR_MESSAGE,
        }:
            return False
        try:
            self.thread_bridge.queue_speech_result(
                self.speech_queue, SpeechEvent(event_type=playback_type, text=text, language=language)
            )
            return True
        except Exception as e:
            logger.error(f"queue_direct_text_playback error: {e}")
            return False
