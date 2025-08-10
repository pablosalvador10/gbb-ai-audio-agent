import asyncio
import json
import threading
from typing import Optional

from azure.communication.callautomation import TextSource
from fastapi import WebSocket
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode

from apps.rtagent.backend.settings import GREETING
from apps.rtagent.backend.src.orchestration.orchestrator import route_turn
from apps.rtagent.backend.src.shared_ws import broadcast_message, send_response_to_acs
from apps.rtagent.backend.src.utils.tracing_utils import (
    create_service_handler_attrs,
    create_service_dependency_attrs,
    log_with_context,
)
from src.enums.monitoring import SpanAttr
from src.enums.stream_modes import StreamMode
from src.speech.speech_recognizer import StreamingSpeechRecognizerFromBytes
from src.stateful.state_managment import MemoManager
from utils.ml_logging import get_logger

logger = get_logger("handlers.acs_media_handler")

# Get OpenTelemetry tracer
tracer = trace.get_tracer(__name__)


class ACSMediaHandler:
    def __init__(
        self,
        ws: WebSocket,
        call_connection_id: str,
        recognizer: StreamingSpeechRecognizerFromBytes = None,
        cm: MemoManager = None,
        session_id: str = None,
        enable_tracing: bool = True,
    ):
        self.recognizer = recognizer or StreamingSpeechRecognizerFromBytes(
            candidate_languages=["en-US", "fr-FR", "de-DE", "es-ES", "it-IT"],
            vad_silence_timeout_ms=800,
            audio_format="pcm",
        )
        self.incoming_websocket = ws
        self.cm = cm
        self.route_turn_queue = asyncio.Queue()
        self.main_loop = None
        self.playback_task = None
        self.route_turn_task = None
        self.stopped = False
        self.latency_tool = getattr(ws.state, "lt", None)
        self.redis_mgr = getattr(ws.app.state, "redis", None)
        self._barge_in_event = threading.Event()
        self._recognizer_started = False

        # Extract call_connection_id from argument or websocket state/headers
        self.call_connection_id = (
            call_connection_id
            or getattr(ws.state, "call_connection_id", None)
            or (ws.headers.get("x-ms-call-connection-id") if hasattr(ws, "headers") else None)
        )

        # session_id: use argument, else fallback to call_connection_id
        self.session_id = session_id or self.call_connection_id

        self.enable_tracing = enable_tracing

        log_with_context(
            logger,
            "info",
            "ACSMediaHandler initialized",
            operation="handler_initialization",
            call_connection_id=self.call_connection_id,
            session_id=self.session_id,
            tracing_enabled=self.enable_tracing,
        )

    def _get_trace_metadata(self, operation: str, **kwargs) -> dict:
        base_metadata = {
            SpanAttr.OPERATION_NAME: operation,
            SpanAttr.SERVICE_NAME: "acs_media_handler",
            "rt.call.connection_id": self.call_connection_id,
            "rt.session.id": self.session_id,
        }
        if kwargs:
            filtered_kwargs = {k: v for k, v in kwargs.items() if v is not None}
            base_metadata.update(filtered_kwargs)
        return base_metadata

    async def start_recognizer(self):
        """Initialize and start the speech recognizer with proper event loop handling."""
        try:
            if self.enable_tracing:
                with tracer.start_as_current_span(
                    "acs_media_handler.start_recognizer",
                    kind=SpanKind.INTERNAL,
                    attributes=create_service_handler_attrs(
                        service_name="acs_media_handler",
                        operation="recognizer_initialization",
                        call_connection_id=self.call_connection_id,
                        session_id=self.session_id,
                    ),
                ):
                    await self._start_recognizer_internal()
            else:
                await self._start_recognizer_internal()
        except Exception as e:
            log_with_context(
                logger, "error", "Failed to start recognizer",
                operation="start_recognizer",
                error=str(e),
                call_connection_id=self.call_connection_id
            )
            raise

    async def _start_recognizer_internal(self):
        log_with_context(logger, "info", "Starting speech recognizer", operation="start_recognizer")
        self.main_loop = asyncio.get_running_loop()
        log_with_context(
            logger,
            "info",
            "Captured main event-loop",
            operation="start_recognizer",
            loop_id=id(self.main_loop)
        )

        self.recognizer.set_partial_result_callback(self.on_partial)
        self.recognizer.set_final_result_callback(self.on_final)
        self.recognizer.set_cancel_callback(self.on_cancel)

        self.recognizer.start()
        log_with_context(logger, "info", "Speech recognizer started", operation="start_recognizer")

        self.route_turn_task = asyncio.create_task(self.route_turn_loop())
        log_with_context(
            logger,
            "info",
            "route_turn_loop task created",
            operation="start_recognizer",
            task_id=str(self.route_turn_task)
        )

        log_with_context(
            logger,
            "info",
            "Playing greeting",
            operation="start_recognizer",
            greeting=GREETING
        )
        await broadcast_message(
            connected_clients=self.incoming_websocket.app.state.clients,
            message=GREETING,
            sender="Assistant",
        )
        self.play_greeting()

    async def handle_media_message(self, stream_data):
        """Process incoming WebSocket message from ACS (AudioMetadata/AudioData)."""
        if self.enable_tracing:
            with tracer.start_as_current_span(
                "acs_media_handler.handle_media_message",
                kind=SpanKind.INTERNAL,
                attributes=self._get_trace_metadata("audio_processing", lightweight=True),
            ):
                await self._handle_media_message_internal(stream_data)
        else:
            await self._handle_media_message_internal(stream_data)

    async def _handle_media_message_internal(self, stream_data):
        try:
            data = json.loads(stream_data)
            kind = data.get("kind")
            if kind == "AudioMetadata":
                log_with_context(
                    logger,
                    "info",
                    "Received AudioMetadata - ACS is ready for audio streaming",
                    operation="handle_media_message",
                )
                if not self._recognizer_started:
                    log_with_context(
                        logger,
                        "info",
                        "Starting speech recognizer on first AudioMetadata event",
                        operation="handle_media_message",
                    )

                    if self.enable_tracing:
                        with tracer.start_as_current_span(
                            "acs_media_handler.recognizer_initialization_on_audio_metadata",
                            kind=SpanKind.INTERNAL,
                            attributes=self._get_trace_metadata("recognizer_initialization_timing"),
                        ) as span:
                            try:
                                await self.start_recognizer()
                                self._recognizer_started = True
                                span.set_attribute("recognizer.started_on_audio_metadata", True)
                                span.set_attribute("recognizer.initialization_timing", "optimal")

                                log_with_context(
                                    logger,
                                    "info",
                                    "Speech recognizer started successfully on AudioMetadata",
                                    operation="handle_media_message",
                                )
                            except Exception as e:
                                span.set_status(Status(StatusCode.ERROR, str(e)))
                                log_with_context(
                                    logger,
                                    "error",
                                    "Failed to start recognizer on AudioMetadata",
                                    operation="handle_media_message",
                                    error=str(e),
                                )
                                raise
                    else:
                        await self.start_recognizer()
                        self._recognizer_started = True
                        log_with_context(
                            logger,
                            "info",
                            "Speech recognizer started successfully on AudioMetadata",
                            operation="handle_media_message",
                        )

            elif kind == "AudioData":
                audio_data_section = data.get("audioData", {})
                if not audio_data_section.get("silent", True):
                    audio_bytes = audio_data_section.get("data")
                    if audio_bytes:
                        if isinstance(audio_bytes, str):
                            import base64
                            audio_bytes = base64.b64decode(audio_bytes)
                        self.recognizer.write_bytes(audio_bytes)
        except Exception as e:
            log_with_context(
                logger,
                "error",
                "Error processing WebSocket message",
                operation="handle_media_message",
                error=str(e),
            )

    def play_greeting(
        self,
        greeting_text: str = GREETING,
        voice: Optional[str] = None,
    ):
        """Send a greeting message to ACS using TTS (CLIENT).
        
        Args:
            greeting_text: Text to speak
            voice: Optional voice name to use for TTS
        """
        if self.enable_tracing:
            with tracer.start_as_current_span(
                "acs_media_handler.play_greeting",
                kind=SpanKind.CLIENT,
                attributes=self._get_trace_metadata(
                    "greeting_playback", greeting_length=len(greeting_text)
                ),
            ):
                self._play_greeting_internal(greeting_text, voice)
        else:
            self._play_greeting_internal(greeting_text, voice)

    def _play_greeting_internal(self, greeting_text: str, voice: Optional[str] = None):
        try:
            self.playback_task = asyncio.create_task(
                send_response_to_acs(
                    ws=self.incoming_websocket,
                    text=greeting_text,
                    blocking=False,
                    latency_tool=self.latency_tool,
                    stream_mode=StreamMode.MEDIA,
                    voice=voice,
                )
            )
        except Exception as e:
            log_with_context(
                logger,
                "error",
                "Failed to play greeting",
                operation="play_greeting",
                error=str(e),
            )

    def on_partial(self, text, lang, speaker_id=None):
        speaker_info = f" (Speaker: {speaker_id})" if speaker_id else ""
        logger.info(f"🗣️ User (partial) in {lang}: {text}{speaker_info}")

        if self._barge_in_event.is_set():
            logger.info("⏭️ Barge-in already detected. Continuing...")
            return

        self._barge_in_event.set()

        if self.main_loop and not self.main_loop.is_closed():
            try:
                latency_tool = self.latency_tool
                latency_tool.start("barge_in")
                asyncio.run_coroutine_threadsafe(
                    self._handle_barge_in_async(), self.main_loop
                )
                logger.info("🚨 Barge-in handling scheduled successfully")
            except Exception as e:
                logger.error(f"❌ Failed to schedule barge-in handling: {e}")
        else:
            logger.warning("⚠️ No main event loop available for barge-in handling")

    def on_final(self, text, lang, speaker_id=None):
        speaker_info = f" (Speaker: {speaker_id})" if speaker_id else ""
        logger.info(f"🧾 User (final) in {lang}: {text}{speaker_info}")

        self._barge_in_event.clear()
        if self.main_loop and not self.main_loop.is_closed():
            try:
                asyncio.run_coroutine_threadsafe(
                    self._handle_final_async(text), self.main_loop
                )
                logger.info("📋 Final result handling scheduled successfully")
            except Exception as e:
                logger.error(f"❌ Failed to schedule final result handling: {e}")
        else:
            logger.warning("⚠️ No main event loop available for final result handling")

    def on_cancel(self, evt):
        logger.warning(f"🚫 Recognition canceled: {evt}")
        self.stopped = True

    async def _handle_barge_in_async(self):
        if self.enable_tracing:
            with tracer.start_as_current_span(
                "acs_media_handler.handle_barge_in",
                kind=SpanKind.INTERNAL,
                attributes=self._get_trace_metadata("barge_in_processing"),
            ):
                await self._handle_barge_in_internal()
        else:
            await self._handle_barge_in_internal()

    async def _handle_barge_in_internal(self):
        try:
            log_with_context(
                logger,
                "info",
                "User barge-in detected, stopping playback and clearing queue",
                operation="handle_barge_in",
            )

            while not self.route_turn_queue.empty():
                try:
                    self.route_turn_queue.get_nowait()
                    self.route_turn_queue.task_done()
                except asyncio.QueueEmpty:
                    break

            log_with_context(
                logger,
                "info",
                "Queue cleared",
                operation="handle_barge_in",
                queue_size=self.route_turn_queue.qsize(),
            )

            if self.playback_task and not self.playback_task.done():
                log_with_context(
                    logger, "info", "Cancelling playback task due to barge-in", operation="handle_barge_in"
                )
                try:
                    self.playback_task.cancel()
                    log_with_context(
                        logger, "info", "Playback task cancellation requested", operation="handle_barge_in"
                    )
                except Exception as e:
                    log_with_context(
                        logger,
                        "warning",
                        "Error cancelling playback task",
                        operation="handle_barge_in",
                        error=str(e),
                    )
                finally:
                    self.playback_task = None

            await self.send_stop_audio()
            if self.latency_tool:
                self.latency_tool.stop("barge_in", self.redis_mgr)

        except Exception as e:
            log_with_context(
                logger,
                "error",
                "Error in barge-in handling",
                operation="handle_barge_in",
                error=str(e),
            )

    async def _handle_final_async(self, text: str):
        if self.enable_tracing:
            with tracer.start_as_current_span(
                "acs_media_handler.handle_final_result",
                kind=SpanKind.INTERNAL,
                attributes=self._get_trace_metadata(
                    "final_speech_processing", text_length=len(text)
                ),
            ):
                await self._handle_final_internal(text)
        else:
            await self._handle_final_internal(text)

    async def _handle_final_internal(self, text: str):
        try:
            await self.route_turn_queue.put(("final", text))

            log_with_context(
                logger,
                "info",
                "Added to queue",
                operation="handle_final_result",
                text=text,
                queue_size=self.route_turn_queue.qsize(),
            )

        except Exception as e:
            log_with_context(
                logger,
                "error",
                "Error in final result handling",
                operation="handle_final_result",
                error=str(e),
            )

    async def route_turn_loop(self):
        if self.enable_tracing:
            with tracer.start_as_current_span(
                "acs_media_handler.route_turn_loop",
                kind=SpanKind.INTERNAL,
                attributes=self._get_trace_metadata("background_processing_loop"),
            ):
                await self._route_turn_loop_internal()
        else:
            await self._route_turn_loop_internal()

    async def _route_turn_loop_internal(self):
        log_with_context(logger, "info", "Route turn loop started", operation="route_turn_loop")

        try:
            while not self.stopped:
                try:
                    kind, text = await asyncio.wait_for(
                        self.route_turn_queue.get(),
                        timeout=0.1,
                    )

                    log_with_context(
                        logger,
                        "info",
                        "Processing turn",
                        operation="route_turn_loop",
                        kind=kind,
                        text=text,
                    )

                    if self.playback_task and not self.playback_task.done():
                        log_with_context(
                            logger, "info", "Cancelling previous playback task", operation="route_turn_loop"
                        )
                        self.playback_task.cancel()
                        try:
                            await asyncio.wait_for(self.playback_task, timeout=1.0)
                        except asyncio.TimeoutError:
                            log_with_context(
                                logger,
                                "warning",
                                "Playback task cancellation timed out, moving on",
                                operation="route_turn_loop",
                            )
                        except asyncio.CancelledError:
                            log_with_context(
                                logger, "info", "Previous playback task cancelled", operation="route_turn_loop"
                            )

                    self.playback_task = asyncio.create_task(self.route_and_playback(kind, text))
                    log_with_context(
                        logger,
                        "info",
                        "Started new playback task",
                        operation="route_turn_loop",
                        task_id=str(self.playback_task),
                    )

                    self.route_turn_queue.task_done()

                except asyncio.TimeoutError:
                    await asyncio.sleep(0)
                    continue
                except Exception as e:
                    log_with_context(
                        logger,
                        "error",
                        "Error in route turn loop",
                        operation="route_turn_loop",
                        error=str(e),
                    )
                    await asyncio.sleep(0.1)

        except Exception as e:
            log_with_context(
                logger, "error", "Route turn loop failed", operation="route_turn_loop", error=str(e)
            )
        finally:
            log_with_context(logger, "info", "Route turn loop ended", operation="route_turn_loop")

    async def route_and_playback(self, kind, text):
        if self.enable_tracing:
            with tracer.start_as_current_span(
                "acs_media_handler.route_and_playback",
                kind=SpanKind.INTERNAL,
                attributes=create_service_dependency_attrs(
                    source_service="acs_media_handler",
                    target_service="orchestration",
                    operation="orchestrator_processing",
                    call_connection_id=self.call_connection_id,
                    session_id=self.session_id,
                    kind=kind,
                    text_length=len(text),
                    queue_size=self.route_turn_queue.qsize(),
                ),
            ):
                await self._route_and_playback_internal(kind, text)
        else:
            await self._route_and_playback_internal(kind, text)

    async def _route_and_playback_internal(self, kind, text):
        try:
            log_with_context(
                logger,
                "info",
                "Routing turn with orchestrator",
                operation="route_and_playback",
                kind=kind,
                text=text,
            )

            if self._barge_in_event.is_set():
                log_with_context(
                    logger,
                    "info",
                    "Barge-in detected during route_turn, skipping processing",
                    operation="route_and_playback",
                )
                return

            await asyncio.wait_for(
                route_turn(
                    cm=self.cm,
                    transcript=text,
                    ws=self.incoming_websocket,
                    is_acs=True,
                ),
                timeout=30.0,
            )
            log_with_context(logger, "info", "Route turn completed successfully", operation="route_and_playback")

        except asyncio.CancelledError:
            log_with_context(logger, "info", "Route and playback cancelled", operation="route_and_playback")
            raise
        except asyncio.TimeoutError:
            log_with_context(
                logger, "error", "Route turn timed out after 30 seconds", operation="route_and_playback"
            )
            await broadcast_message(
                connected_clients=self.incoming_websocket.app.state.clients,
                message="I'm sorry, I'm experiencing some delays. Please try again.",
                sender="Assistant",
            )
        except Exception as e:
            log_with_context(
                logger,
                "error",
                "Error in route and playback",
                operation="route_and_playback",
                error=str(e),
            )

    async def send_stop_audio(self):
        """Send a stop-audio event to ACS to interrupt current playback (CLIENT)."""
        if self.enable_tracing:
            with tracer.start_as_current_span(
                "acs_media_handler.send_stop_audio",
                kind=SpanKind.CLIENT,
                attributes=self._get_trace_metadata("stop_audio_command"),
            ):
                await self._send_stop_audio_internal()
        else:
            await self._send_stop_audio_internal()

    async def _send_stop_audio_internal(self):
        try:
            stop_audio_data = {
                "Kind": "StopAudio",
                "AudioData": None,
                "StopAudio": {},
            }
            json_data = json.dumps(stop_audio_data)
            await self.incoming_websocket.send_text(json_data)
            log_with_context(logger, "info", "Sent stop audio command to ACS", operation="send_stop_audio")
        except Exception as e:
            log_with_context(
                logger,
                "warning",
                "Failed to send stop audio",
                operation="send_stop_audio",
                error=str(e),
            )

    async def stop(self):
        if self.enable_tracing:
            with tracer.start_as_current_span(
                "acs_media_handler.stop",
                kind=SpanKind.INTERNAL,
                attributes=self._get_trace_metadata("cleanup_and_shutdown"),
            ):
                await self._stop_internal()
        else:
            await self._stop_internal()

    async def _stop_internal(self):
        log_with_context(logger, "info", "Stopping ACS Media Handler", operation="stop")
        self.stopped = True

        try:
            if self.recognizer:
                self.recognizer.stop()
                log_with_context(logger, "info", "Speech recognizer stopped", operation="stop")

            if self.playback_task and not self.playback_task.done():
                try:
                    self.playback_task.cancel()
                    await self.playback_task
                    log_with_context(logger, "info", "Playback task cancelled", operation="stop")
                except Exception:
                    pass
        finally:
            log_with_context(logger, "info", "Handler stopped", operation="stop")