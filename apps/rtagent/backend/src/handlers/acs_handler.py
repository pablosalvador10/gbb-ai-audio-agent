"""
handlers/acs_handler.py
=======================
Business logic handlers for Azure Communication Services operations.

This module contains the core business logic extracted from the ACS router,
following the separation of concerns principle. The router handles HTTP/WebSocket
routing while this handler manages the actual ACS business operations.
"""

from __future__ import annotations

import asyncio
from asyncio import events
import json
import logging
import time
from typing import Any, Dict, Optional

from azure.communication.callautomation import TextSource, PhoneNumberIdentifier
from azure.core.exceptions import HttpResponseError
from azure.core.messaging import CloudEvent
from fastapi import HTTPException, WebSocket
from fastapi.responses import JSONResponse

from apps.rtagent.backend.settings import ACS_STREAMING_MODE, GREETING, GREETING_VOICE_TTS
from apps.rtagent.backend.src.shared_ws import broadcast_message
from src.enums.stream_modes import StreamMode
from src.stateful.state_managment import MemoManager
from utils.ml_logging import (
    get_logger,
    log_with_correlation,
    set_span_correlation_attributes,
)
from utils.trace_context import TraceContext

logger = get_logger("handlers.acs_handler")


class ACSHandler:
    """
    Handles Azure Communication Services business logic operations.

    This class encapsulates the core business logic for:
    - Call initiation and management
    - Event processing from ACS callbacks
    - WebSocket transcription handling
    - Media event processing
    """

    @staticmethod
    async def initiate_call(
        acs_caller, target_number: str, redis_mgr, call_id: str = None
    ) -> Dict[str, Any]:
        """
        Initiate an outbound call through Azure Communication Services.

        Args:
            acs_caller: The ACS caller instance
            target_number: The phone number to call
            redis_mgr: Redis manager instance
            call_id: Optional call ID for tracking

        Returns:
            Dict containing call initiation result

        Raises:
            HTTPException: If call initiation fails
        """
        if not acs_caller:
            raise HTTPException(503, "ACS Caller not initialised")

        with TraceContext(
            "acs_handler.initiate_call",
            component="acs",
            metadata={"target_number": target_number},
        ) as ctx:
            try:
                # Set initial trace attributes
                ctx.set_attribute("acs.target_number", target_number)
                ctx.set_attribute("acs.initial_call_id", call_id or "auto_generated")
                ctx.set_attribute("acs.streaming_mode", str(ACS_STREAMING_MODE))
                ctx.set_attribute("operation.type", "outbound_call_initiation")
                ctx.set_attribute("operation.name", "acs_handler.initiate_call")

                # Add correlation attributes for Application Insights
                set_span_correlation_attributes(
                    operation_name="acs_handler.initiate_call",
                    custom_attributes={
                        "target.phone.number": target_number,
                        "call.direction": "outbound",
                        "acs.operation": "initiate_call",
                    },
                )

                # Log operation start with correlation
                log_with_correlation(
                    logger,
                    logging.INFO,
                    f"Initiating outbound call to {target_number}",
                    operation_name="acs_handler.initiate_call",
                    custom_attributes={"target_number": target_number},
                )

                # TODO: Add logic to reject multiple requests for the same target number

                start_time = time.perf_counter()
                result = await acs_caller.initiate_call(
                    target_number, stream_mode=ACS_STREAMING_MODE
                )
                latency = time.perf_counter() - start_time
                ctx.set_attribute("call_initiation_latency_sec", latency)

                if result.get("status") != "created":
                    ctx.set_attribute("acs.call_initiation.status", "failed")
                    ctx.set_attribute("acs.call_initiation.result", str(result))
                    return {"status": "failed", "message": "Call initiation failed"}

                logger.info(
                    f"Call initiation latency: {latency:.3f} seconds for target {target_number}"
                )

                call_id = result["call_id"]

                # Update span with the actual call connection ID (KEY CORRELATION)
                ctx.set_attribute("call.connection.id", call_id)
                ctx.set_attribute("acs.call_initiation.status", "success")
                ctx.set_attribute("acs.call_id", call_id)

                # Update correlation attributes with call connection ID
                set_span_correlation_attributes(
                    call_connection_id=call_id,
                    session_id=call_id,
                    operation_name="acs_handler.initiate_call",
                    custom_attributes={
                        "target.phone.number": target_number,
                        "call.direction": "outbound",
                        "acs.operation": "initiate_call",
                    },
                )

                # Initialize conversation state
                cm = MemoManager.from_redis(
                    session_id=call_id,
                    redis_mgr=redis_mgr,
                )

                cm.update_context("target_number", target_number)
                cm.persist_to_redis(redis_mgr)

                # Log success with correlation
                log_with_correlation(
                    logger,
                    logging.INFO,
                    f"Call initiated successfully – ID={call_id}, Target={target_number}",
                    call_connection_id=call_id,
                    session_id=call_id,
                    operation_name="acs_handler.initiate_call",
                    custom_attributes={
                        "target_number": target_number,
                        "conversation_state_initialized": True,
                    },
                )

                ctx.set_attribute("conversation.state.initialized", True)
                ctx.set_attribute("operation.success", True)

                return {
                    "status": "success",
                    "message": "Call initiated",
                    "callId": call_id,
                }

            except (HttpResponseError, RuntimeError) as exc:
                ctx.set_attribute("error.occurred", True)
                ctx.set_attribute(
                    "error.message", f"ACS error during call initiation: {exc}"
                )
                log_with_correlation(
                    logger,
                    logging.ERROR,
                    f"ACS error during call initiation to {target_number}: {exc}",
                    operation_name="acs_handler.initiate_call",
                    custom_attributes={
                        "target_number": target_number,
                        "error_type": type(exc).__name__,
                        "acs_streaming_mode": str(ACS_STREAMING_MODE),
                        "call_id": call_id,
                    },
                )
                logger.error("ACS error: %s", exc, exc_info=True)
                # Provide more context in the HTTPException detail for easier debugging
                raise HTTPException(
                    500,
                    detail={
                        "error": str(exc),
                        "target_number": target_number,
                        "call_id": call_id,
                        "acs_streaming_mode": str(ACS_STREAMING_MODE),
                        "exception_type": type(exc).__name__,
                    },
                ) from exc
            except Exception as exc:
                ctx.set_attribute("error.occurred", True)
                ctx.set_attribute(
                    "error.message", f"Unexpected error during call initiation: {exc}"
                )
                log_with_correlation(
                    logger,
                    logging.ERROR,
                    f"Unexpected error during call initiation to {target_number}: {exc}",
                    operation_name="acs_handler.initiate_call",
                    custom_attributes={
                        "target_number": target_number,
                        "error_type": type(exc).__name__,
                        "acs_streaming_mode": str(ACS_STREAMING_MODE),
                        "call_id": call_id,
                    },
                )
                logger.error("Unexpected error: %s", exc, exc_info=True)
                raise HTTPException(
                    400,
                    detail={
                        "error": str(exc),
                        "target_number": target_number,
                        "call_id": call_id,
                        "acs_streaming_mode": str(ACS_STREAMING_MODE),
                        "exception_type": type(exc).__name__,
                    },
                ) from exc

    @staticmethod
    async def handle_inbound_call(
        request_body: Dict[str, Any], acs_caller
    ) -> JSONResponse:
        """
        Handle inbound call events and subscription validation.

        Args:
            request_body: The request body containing events
            acs_caller: The ACS caller instance

        Returns:
            JSONResponse with appropriate status
        """
        with TraceContext("acs_handler.handle_inbound_call") as trace:
            # Set initial span attributes
            trace.set_attribute("acs.event_count", len(request_body))
            trace.set_attribute("acs.streaming_mode", str(ACS_STREAMING_MODE))
            trace.set_attribute("operation.type", "inbound_call_handling")
            trace.set_attribute("operation.name", "acs_handler.handle_inbound_call")

            # Add correlation attributes for Application Insights
            set_span_correlation_attributes(
                operation_name="acs_handler.handle_inbound_call",
                custom_attributes={
                    "call.direction": "inbound",
                    "acs.operation": "handle_inbound_call",
                    "event.count": len(request_body),
                },
            )

            if not acs_caller:
                trace.set_attribute("error.occurred", True)
                trace.set_attribute("error.message", "ACS Caller not initialised")
                raise HTTPException(503, "ACS Caller not initialised")

            try:
                for idx, event in enumerate(request_body):
                    event_type = event.get("eventType")
                    trace.set_attribute(f"acs.event.{idx}.type", event_type)

                    if event_type == "Microsoft.EventGrid.SubscriptionValidationEvent":
                        # Handle subscription validation event
                        validation_code = event.get("data", {}).get("validationCode")

                        trace.set_attribute(
                            "acs.event.validation.code_present", bool(validation_code)
                        )
                        trace.set_attribute(
                            "operation.subtype", "subscription_validation"
                        )

                        if validation_code:
                            trace.set_attribute("operation.success", True)
                            return JSONResponse(
                                {"validationResponse": validation_code}, status_code=200
                            )
                        else:
                            trace.set_attribute("error.occurred", True)
                            trace.set_attribute(
                                "error.message",
                                "Validation code not found in event data",
                            )
                            raise HTTPException(
                                400, "Validation code not found in event data"
                            )
                    elif event_type == "Microsoft.Communication.IncomingCall":
                        # Convert CloudEvent to dict for processing
                        event = dict(event)
                        event_data = event.get("data", {})

                        # Extract caller information
                        if event_data["from"]["kind"] == "phoneNumber":
                            caller_id = event_data["from"]["phoneNumber"]["value"]
                        else:
                            caller_id = event_data["from"]["rawId"]

                        incoming_call_context = event_data["incomingCallContext"]

                        # Update span with incoming call details
                        trace.set_attribute("call.caller_id", caller_id)
                        trace.set_attribute(
                            "call.from.kind", event_data["from"]["kind"]
                        )
                        trace.set_attribute(
                            "acs.incoming_call_context",
                            (
                                incoming_call_context[:50] + "..."
                                if len(incoming_call_context) > 50
                                else incoming_call_context
                            ),
                        )
                        trace.set_attribute("operation.subtype", "incoming_call_answer")

                        # Update correlation attributes with caller information
                        set_span_correlation_attributes(
                            operation_name="acs_handler.handle_inbound_call",
                            custom_attributes={
                                "caller.id": caller_id,
                                "incoming.call.context": (
                                    incoming_call_context[:50] + "..."
                                    if len(incoming_call_context) > 50
                                    else incoming_call_context
                                ),
                                "call.direction": "inbound",
                            },
                        )

                        log_with_correlation(
                            logger,
                            logging.INFO,
                            f"Processing incoming call from caller: {caller_id}",
                            operation_name="acs_handler.handle_inbound_call",
                            custom_attributes={
                                "caller_id": caller_id,
                                "from_kind": event_data["from"]["kind"],
                            },
                        )

                        # Answer the incoming call
                        answer_call_result = await acs_caller.answer_incoming_call(
                            incoming_call_context=incoming_call_context,
                            stream_mode=ACS_STREAMING_MODE,
                        )

                        if answer_call_result:
                            call_connection_id = getattr(
                                answer_call_result, "call_connection_id", None
                            )

                            if call_connection_id:
                                # Update span with call connection ID (KEY CORRELATION)
                                trace.set_attribute(
                                    "call.connection.id", call_connection_id
                                )
                                trace.set_attribute("acs.call_answered", True)
                                trace.set_attribute("operation.success", True)

                                # Update correlation attributes with call connection ID
                                set_span_correlation_attributes(
                                    call_connection_id=call_connection_id,
                                    session_id=call_connection_id,
                                    operation_name="acs_handler.handle_inbound_call",
                                    custom_attributes={
                                        "caller.id": caller_id,
                                        "call.direction": "inbound",
                                    },
                                )

                                log_with_correlation(
                                    logger,
                                    logging.INFO,
                                    f"Incoming call answered successfully: {call_connection_id} from caller {caller_id}",
                                    call_connection_id=call_connection_id,
                                    session_id=call_connection_id,
                                    operation_name="acs_handler.handle_inbound_call",
                                    custom_attributes={
                                        "caller_id": caller_id,
                                        "call_answered": True,
                                    },
                                )
                            else:
                                trace.set_attribute("acs.call_answered", True)
                                trace.set_attribute("call.connection.id.missing", True)
                                logger.warning(
                                    "Call answered but no call_connection_id available: %s",
                                    answer_call_result,
                                )
                    else:
                        # Handle unhandled events
                        trace.set_attribute(f"acs.event.{idx}.unhandled", True)
                        trace.set_attribute("operation.subtype", "unhandled_event")
                        logger.info(f"Received event of type {event_type}: {event}")

                trace.set_attribute("operation.success", True)
                return JSONResponse({"status": "call answered"}, status_code=200)

            except (HttpResponseError, RuntimeError) as exc:
                trace.set_attribute("error.occurred", True)
                trace.set_attribute(
                    "error.message", f"ACS error during inbound call handling: {exc}"
                )
                logger.error("ACS error: %s", exc, exc_info=True)
                raise HTTPException(500, str(exc)) from exc
            except Exception as exc:
                trace.set_attribute("error.occurred", True)
                trace.set_attribute(
                    "error.message", f"Unexpected error processing inbound call: {exc}"
                )
                logger.error("Error processing inbound call: %s", exc, exc_info=True)
                raise HTTPException(400, "Invalid request body") from exc

    @staticmethod
    async def process_callback_events(events: list, request) -> Dict[str, str]:
        """
        Process callback events from Azure Communication Services.

        Args:
            events: List of ACS events to process
            request: FastAPI request object containing app state

        Returns:
            Dict with processing status
        """
        # Derive dependencies from request app state
        acs_caller = request.app.state.acs_caller
        stt_client = request.app.state.stt_client
        redis_mgr = request.app.state.redis
        clients = request.app.state.clients

        # Event handler mapping for cleaner code organization
        event_handlers = {
            "Microsoft.Communication.ParticipantsUpdated": ACSHandler._handle_participants_updated,
            "Microsoft.Communication.CallConnected": ACSHandler._handle_call_connected,
            "Microsoft.Communication.TranscriptionFailed": ACSHandler._handle_transcription_failed,
            "Microsoft.Communication.CallDisconnected": ACSHandler._handle_call_disconnected,
            "Microsoft.Communication.MediaStreamingStarted": ACSHandler._handle_media_streaming_started,
            "Microsoft.Communication.ContinuousDtmfRecognitionStarted": ACSHandler._handle_dtmf_started,
            "Microsoft.Communication.ContinuousDtmfRecognitionToneReceived": ACSHandler._handle_dtmf_tone_received,
        }
        try:
            for raw in events:
                event = CloudEvent.from_dict(raw)
                etype = event.type
                cid = event.data.get("callConnectionId")
                cm = MemoManager.from_redis(cid, redis_mgr)
                # Log event type and call connection ID for observability
                logger.info(f"Processing event: {etype} for callConnectionId: {cid}")
                # Handle specific events with dedicated handlers

                if etype in event_handlers:
                    handler = event_handlers[etype]
                    if etype == "Microsoft.Communication.CallConnected":
                        await handler(event, cm, redis_mgr, clients, cid, acs_caller, stt_client)
                    elif etype == "Microsoft.Communication.MediaStreamingStarted":
                        await handler(event, cm, redis_mgr, cid, acs_caller)
                    elif etype in ["Microsoft.Communication.ParticipantsUpdated"]:
                        await handler(event, cm, redis_mgr, clients, cid, acs_caller)  # pass acs_caller here
                    elif etype == "Microsoft.Communication.TranscriptionFailed":
                        await handler(event, cm, redis_mgr, cid, acs_caller)
                    elif etype == "Microsoft.Communication.CallDisconnected":
                        await handler(event, cm, redis_mgr, cid)
                    elif etype == "Microsoft.Communication.ContinuousDtmfRecognitionToneReceived":
                        await handler(event, cm, redis_mgr, cid)
                    else:
                        await handler(event, cm, redis_mgr, cid)


                # # Handle media events that affect bot_speaking state
                # elif etype in media_events:
                #     cm.update_context("bot_speaking", media_events[etype])
                #     action = "Set" if media_events[etype] else "Set"
                #     logger.info(
                #         f"{etype.split('.')[-1]}: {action} bot_speaking={media_events[etype]} for call {cm.session_id}"
                #     )

                #     # Log errors for failed events
                #     if "Failed" in etype:
                #         reason = event.data.get("resultInformation", "Unknown reason")
                #         logger.error(
                #             f"⚠️ {etype.split('.')[-1]} for call {cid}: {reason}"
                #         )

                # # Handle other failed events
                # elif "Failed" in etype:
                #     reason = event.data.get("resultInformation", "Unknown reason")
                #     logger.error("⚠️ %s for call %s: %s", etype, cid, reason)

                # # Log unhandled events
                # else:
                #     logger.info("Unhandled event: %s for call %s", etype, cid)

                # cm.persist_to_redis(redis_mgr)

            return {"status": "callback received"}

        except Exception as exc:
            logger.error("Callback error: %s", exc, exc_info=True)
            return {"error": str(exc)}

    @staticmethod
    def _get_participant_phone(event: CloudEvent, cm: MemoManager) -> Optional[str]:
        """
        Try to resolve the PSTN participant phone number to listen for DTMF.
        Priority:
          1) From event.data.participants where kind == 'phoneNumber' (AWS Connect leg)
          2) Fallback to memo 'target_number' (for outbound calls)
        """
        try:
            parts = event.data.get("participants", []) or []
            for p in parts:
                ident = p.get("identifier", {})
                if ident.get("kind") == "phoneNumber":
                    phone = (ident.get("phoneNumber") or {}).get("value")
                    if phone:
                        return phone
        except Exception:
            pass
        # Fallback (outbound case)
        return cm.get_context("target_number")

    # @staticmethod
    # async def _handle_participants_updated(
    #     event: CloudEvent, cm: MemoManager, redis_mgr, clients: list, cid: str
    # ) -> None:
    #     """Handle participant updates in the call."""
    #     participants = event.data.get("participants", [])
    #     target_number = cm.get_context("target_number")
    #     target_joined = (
    #         any(
    #             p.get("identifier", {}).get("rawId", "").endswith(target_number or "")
    #             for p in participants
    #         )
    #         if target_number
    #         else False
    #     )
    #     cm.update_context("target_participant_joined", target_joined)
    #     cm.persist_to_redis(redis_mgr)

    #     logger.info(f"Target participant joined: {target_joined} for call {cid}")
    @staticmethod
    def _get_participant_phone(event: CloudEvent, cm: MemoManager) -> Optional[str]:
        """Return the PSTN phone (E.164) from participants, preferring a match to target_number."""
        def digits_tail(s: Optional[str], n: int = 10) -> str:
            return "".join(ch for ch in (s or "") if ch.isdigit())[-n:]

        participants = event.data.get("participants", []) or []
        target_number = cm.get_context("target_number")
        target_tail = digits_tail(target_number) if target_number else ""

        pstn_candidates = []
        for p in participants:
            ident = p.get("identifier", {}) or {}
            # prefer explicit phone number
            phone = (ident.get("phoneNumber") or {}).get("value")
            # fallback: rawId like "4:+12246234441"
            if not phone:
                raw = ident.get("rawId")
                if isinstance(raw, str) and raw.startswith("4:"):
                    phone = raw[2:]
            if phone:
                pstn_candidates.append(phone)

        if not pstn_candidates:
            return None

        if target_tail:
            for ph in pstn_candidates:
                if digits_tail(ph) == target_tail:
                    return ph

        # fallback to first PSTN participant
        return pstn_candidates[0]

    @staticmethod
    async def _handle_participants_updated(
        event: CloudEvent, cm: MemoManager, redis_mgr, clients: list, cid: str, acs_caller
    ) -> None:
        """Enable DTMF when the PSTN participant is present."""
        # Resolve PSTN phone and use its presence as 'joined'
        target_phone = ACSHandler._get_participant_phone(event, cm)
        target_joined = bool(target_phone)

        cm.update_context("target_participant_joined", target_joined)
        cm.persist_to_redis(redis_mgr)
        logger.info("Target participant joined: %s for call %s", target_joined, cid)
        
        # If the PSTN participant has joined and DTMF hasn't started, kick off DTMF recognition in the background.
        if target_joined and not cm.get_context("dtmf_started"):
            cm.update_context("dtmf_sequence", "")
            cm.update_context("dtmf_validated", False)
            
            async def start_dtmf():
                try:
                    # Get call connection in the background task
                    call_conn = acs_caller.get_call_connection(cid)
                    if not call_conn:
                        logger.error("Call connection not found for %s", cid)
                        return
                    
                    # Wrap the potentially blocking call in run_in_executor
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None,  # Use default executor
                        lambda: call_conn.start_continuous_dtmf_recognition(
                            target_participant=PhoneNumberIdentifier(value=target_phone),
                            operation_context="ivr",
                        )
                    )
                    
                    # Update state only after successful start
                    cm.update_context("dtmf_started", True)
                    cm.persist_to_redis(redis_mgr)
                    logger.info("📲 DTMF recognition ON after participant join for %s (participant=%s)", cid, target_phone)
                except Exception as e:
                    logger.error("Failed to start DTMF recognition for call %s: %s", cid, e, exc_info=True)
                    # Optionally set a flag to retry later
                    cm.update_context("dtmf_start_failed", True)
                    cm.persist_to_redis(redis_mgr)

            # Fire and forget: don't await, so the handler returns immediately
            asyncio.create_task(start_dtmf())
        elif not target_joined:
            logger.warning("ParticipantsUpdated: unable to resolve PSTN participant for DTMF on call %s", cid)


    @staticmethod
    async def _handle_call_connected(
        event: CloudEvent,
        cm: MemoManager,
        redis_mgr,
        clients: list,
        cid: str,
        acs_caller,
        stt_client,
        stream_mode: StreamMode = ACS_STREAMING_MODE,
    ) -> None:
        """Handle call connected event and prepare for media streaming or transcription."""
        await broadcast_message(clients, f"Call Connected: {cid}", "System")

        # 1) Resolve the AWS Connect/PSTN leg and start DTMF recognition immediately
        call_conn = acs_caller.get_call_connection(cid)
        if stream_mode == StreamMode.TRANSCRIPTION:
            # Delay greeting very slightly to avoid clipping first tones
            await ACSHandler._play_greeting(cm, redis_mgr, cid, acs_caller, stream_mode, delay_seconds=0.2)
        elif stream_mode == StreamMode.MEDIA:
            logger.info("Call connected for media streaming mode. Waiting for WebSocket connection: %s", cid)

    # New: optional readiness log for DTMF recognizer
    @staticmethod
    async def _handle_dtmf_started(event: CloudEvent, cm: MemoManager, redis_mgr, cid: str) -> None:
        """Handle DTMF recognition started event."""
        logger.info("✅ DTMF recognition started for call %s", cid)
        cm.update_context("dtmf_ready", True)
        cm.persist_to_redis(redis_mgr)

    # @staticmethod
    # async def _handle_call_connected(
    #     event: CloudEvent,
    #     cm: MemoManager,
    #     redis_mgr,
    #     clients: list,
    #     cid: str,
    #     acs_caller,
    #     stt_client,
    #     stream_mode: StreamMode = ACS_STREAMING_MODE,
    # ) -> None:
    #     """Handle call connected event and prepare for media streaming or transcription."""
    #     await broadcast_message(clients, f"Call Connected: {cid}", "System")
    #     call_conn = acs_caller.get_call_connection(cid)
    #     # await wait_for_call_connected(call_conn, poll_interval=0.05)  # Poll every 50ms
    #     # Get the target participant from event data
    #     target_participant = PhoneNumberIdentifier(value=cm.get_context("target_number"))
    #     target = call_conn.get_participant(
    #         target_participant=target_participant
    #     )

    #     call_conn.start_continuous_dtmf_recognition(
    #         target_participant=target_participant,
    #         operation_context="ivr"
    #     )
    #     logger.info("📲 DTMF subscription ON for %s to target participant %s", cid, target_participant)

    #     # Store call connection state
    #     await cm.set_live_context_value(redis_mgr, "call_connected", True)
    #     await cm.set_live_context_value(redis_mgr, "greeted", False)

    #     # For TRANSCRIPTION mode, play greeting immediately since WebSocket is not required
    #     if stream_mode == StreamMode.TRANSCRIPTION:
    #         await ACSHandler._play_greeting(cm, redis_mgr, cid, acs_caller, stream_mode)

    #     # For MEDIA mode, mark that we're ready for WebSocket connection
    #     elif stream_mode == StreamMode.MEDIA:
    #         logger.info(
    #             f"Call connected for media streaming mode. Waiting for WebSocket connection: {cid}"
    #         )

    @staticmethod
    async def _play_greeting(
        cm: MemoManager,
        redis_mgr,
        cid: str,
        acs_caller,
        stream_mode: StreamMode,
        delay_seconds: float = 0.5,
    ) -> None:
        """
        Play greeting with optional delay to ensure media connection is ready.

        Args:
            cm: MemoManager instance
            redis_mgr: Redis manager
            cid: Call connection ID
            acs_caller: ACS caller instance
            stream_mode: Current streaming mode
            delay_seconds: Delay before playing greeting (for media mode stability)
        """
        # Check if greeting has already been played
        greeted = await cm.get_live_context_value(redis_mgr, "greeted")
        if greeted:
            logger.info(f"Greeting already played for call {cid}")
            return

        greeting = GREETING

        try:
            # Add delay for media mode to ensure WebSocket is stable
            if stream_mode == StreamMode.MEDIA and delay_seconds > 0:
                logger.info(
                    f"Waiting {delay_seconds}s before playing greeting for media mode"
                )
                await asyncio.sleep(delay_seconds)

            if stream_mode == StreamMode.TRANSCRIPTION:
                # Use ACS TTS for transcription mode
                text_source = TextSource(
                    text=greeting, source_locale="en-US", voice_name=GREETING_VOICE_TTS
                )
                call_conn = acs_caller.get_call_connection(cid)
                call_conn.play_media(play_source=text_source)
                logger.info(f"Greeting played via ACS TTS for call {cid}")

            elif stream_mode == StreamMode.MEDIA:
                # For media mode, the greeting will be handled by the media handler
                # via the send_response_to_acs function which uses the established WebSocket
                logger.info(
                    f"Greeting prepared for media streaming mode for call {cid}"
                )
                # The actual greeting will be triggered by the WebSocket connection
                # in the ACSMediaHandler.play_greeting() method

            await cm.set_live_context_value(redis_mgr, "greeted", True)
            await cm.set_live_context_value(
                redis_mgr, "ready_for_media_greeting", False
            )

        except Exception as e:
            logger.error(f"Error playing greeting for call {cid}: {e}", exc_info=True)

    @staticmethod
    async def _handle_media_streaming_started(
        event: CloudEvent, cm: MemoManager, redis_mgr, cid: str, acs_caller
    ) -> None:
        """
        Handle media streaming started event.
        This indicates the WebSocket connection is ready for media streaming.
        """
        logger.info(f"📡 Media streaming started for call {cid}")

        try:
            # Signal that media streaming is ready
            await cm.set_live_context_value(redis_mgr, "media_streaming_ready", True)

            # Check if we're ready for greeting
            ready_for_greeting = await cm.get_live_context_value(
                redis_mgr, "ready_for_media_greeting"
            )
            already_greeted = await cm.get_live_context_value(redis_mgr, "greeted")

            if ready_for_greeting and not already_greeted:
                logger.info(
                    f"🎤 Media streaming ready, WebSocket can now play greeting for call {cid}"
                )
                # The greeting will be handled by the ACSMediaHandler when it detects this state
            else:
                logger.info(
                    f"Media streaming started but greeting not needed for call {cid} - "
                    f"ready_for_greeting: {ready_for_greeting}, already_greeted: {already_greeted}"
                )

        except Exception as e:
            logger.error(
                f"Error handling media streaming started for call {cid}: {e}",
                exc_info=True,
            )

    @staticmethod
    async def _handle_dtmf_tone_received(event, cm, redis_mgr, cid):
        """
        Handle DTMF tone received events.
        Accumulate tones in app memory and validate against a sample value.
        """
        tone = event.data.get("tone")
        sequence_id = event.data.get("sequenceId")
        logger.info("🔢 DTMF seq=%s tone=%s call=%s", sequence_id, tone, cid)

        # Retrieve current DTMF sequence (default empty)
        dtmf_sequence = cm.get_context("dtmf_sequence") or ""

        # Normalize tones from ACS/AWS (can be 'one', '1', 'pound', '#', 'star', '*')
        tone_numeric = None
        tone_map = {
            "zero": "0", "0": "0",
            "one": "1", "1": "1",
            "two": "2", "2": "2",
            "three": "3", "3": "3",
            "four": "4", "4": "4",
            "five": "5", "5": "5",
            "six": "6", "6": "6",
            "seven": "7", "7": "7",
            "eight": "8", "8": "8",
            "nine": "9", "9": "9",
            "star": "*", "*": "*",
            "asterisk": "*",
            "pound": "#", "#": "#",
            "hash": "#",
        }
        if isinstance(tone, str):
            tone_lower = tone.lower()
            if tone_lower in tone_map:
                tone_numeric = tone_map[tone_lower]

        if tone_numeric:
            # Maintain correct ordering by sequenceId if present
            seq_index = sequence_id - 1 if isinstance(sequence_id, int) else len(dtmf_sequence)
            dtmf_list = list(dtmf_sequence)
            while len(dtmf_list) <= seq_index:
                dtmf_list.append("")
            dtmf_list[seq_index] = tone_numeric
            dtmf_sequence = "".join(dtmf_list)

            cm.update_context("dtmf_sequence", dtmf_sequence)
            cm.persist_to_redis(redis_mgr)

            # Example check: first 3 digits
            sample_value = "123"
            if len([c for c in dtmf_sequence if c]) >= 3:
                candidate = "".join(d for d in dtmf_sequence if d)[:3]
                if candidate == sample_value:
                    logger.info("DTMF validated for call %s: %s (match)", cid, candidate)
                    cm.update_context("dtmf_validated", True)
                else:
                    logger.info("DTMF validation failed for call %s: %s (no match)", cid, candidate)
                    cm.update_context("dtmf_validated", False)
                cm.persist_to_redis(redis_mgr)
        else:
            logger.info("DTMF tone ignored (unmapped): %s", tone)

    # @staticmethod
    # async def _handle_dtmf_tone_received(event, cm, redis_mgr, cid):
    #     """
    #     Handle DTMF tone received events.
    #     Accumulate tones in app memory and validate against a sample value.
    #     """
    #     tone = event.data.get("tone")
    #     sequence_id = event.data.get("sequenceId")
    #     logger.info("🔢 DTMF seq=%s tone=%s call=%s", sequence_id, tone, cid)

    #     # Retrieve current DTMF sequence from memory (default to empty string)
    #     dtmf_sequence = cm.get_context("dtmf_sequence") or ""
    #     # Map string tones to integer format if necessary
    #     tone_map = {
    #         "zero": 0,
    #         "one": 1,
    #         "two": 2,
    #         "three": 3,
    #         "four": 4,
    #         "five": 5,
    #         "six": 6,
    #         "seven": 7,
    #         "eight": 8,
    #         "nine": 9,
    #     }
    #     if tone in tone_map:
    #         tone_numeric = tone_map[tone]
    #     # Append the new tone if it's a digit
    #     if tone_numeric:
    #         # Ensure the sequence is respected: sequence_id - 1 is the index
    #         seq_index = sequence_id - 1 if sequence_id is not None else len(dtmf_sequence)
    #         dtmf_list = list(dtmf_sequence)
    #         # Expand list if needed
    #         while len(dtmf_list) <= seq_index:
    #             dtmf_list.append("")
    #         dtmf_list[seq_index] = str(tone_numeric)
    #         dtmf_sequence = "".join(dtmf_list)
    #         cm.update_context("dtmf_sequence", dtmf_sequence)
    #         cm.persist_to_redis(redis_mgr)

    #         # Sample value to match
    #         sample_value = "123"
    #         # If 3 digits entered, validate
    #         if len(dtmf_sequence) == 3:
    #             if dtmf_sequence == sample_value:
    #                 logger.info(f"DTMF validated for call {cid}: {dtmf_sequence} (match)")
    #                 cm.update_context("dtmf_validated", True)
    #             else:
    #                 logger.info(f"DTMF validation failed for call {cid}: {dtmf_sequence} (no match)")
    #                 cm.update_context("dtmf_validated", False)
    #             cm.persist_to_redis(redis_mgr)
    #     else:
    #         logger.info(f"DTMF tone ignored (not a digit): {tone}")

    @staticmethod
    async def _handle_transcription_failed(
        event: CloudEvent, cm: MemoManager, redis_mgr, cid: str, acs_caller
    ) -> None:
        """Handle transcription failure events."""
        reason = event.data.get("resultInformation", "Unknown reason")
        logger.error(f"⚠️ Transcription failed for call {cid}: {reason}")

        # Log detailed error information
        if "transcriptionUpdate" in event.data:
            transcription_update = event.data["transcriptionUpdate"]
            logger.info(f"TranscriptionUpdate attributes for call {cid}:")
            for key, value in transcription_update.items():
                logger.info(f"  {key}: {value}")

        # Handle specific error codes
        if isinstance(reason, dict):
            error_code = reason.get("code", "Unknown")
            sub_code = reason.get("subCode", "Unknown")
            message = reason.get("message", "No message")
            logger.error(
                f"   Error details - Code: {error_code}, SubCode: {sub_code}, Message: {message}"
            )

            # Check for WebSocket URL issues
            if sub_code == 8581:
                logger.error("🔴 WebSocket connection issue detected!")
                logger.error("   This usually means:")
                logger.error(
                    "   1. Your WebSocket endpoint is not accessible from Azure"
                )
                logger.error(
                    "   2. Your BASE_URL is incorrect or not publicly accessible"
                )
                logger.error("   3. Your WebSocket server is not running or crashed")

        # Attempt to restart transcription
        try:
            if acs_caller and hasattr(acs_caller, "call_automation_client"):
                call_connection_client = acs_caller.get_call_connection(cid)
                if call_connection_client:
                    call_connection_client.start_transcription()
                    logger.info(f"✅ Attempted to restart transcription for call {cid}")
                else:
                    logger.error(
                        f"❌ Could not get call connection for {cid} to restart transcription"
                    )
        except Exception as e:
            logger.error(
                f"❌ Failed to restart transcription for call {cid}: {e}", exc_info=True
            )

    @staticmethod
    async def _handle_call_disconnected(
        event: CloudEvent, cm: MemoManager, redis_mgr, cid: str
    ) -> None:
        """Handle call disconnection events."""
        logger.info(f"❌ Call disconnected for call {cid}")

        # Log additional details for debugging
        disconnect_reason = event.data.get(
            "resultInformation", "No resultInformation provided"
        )
        participants = event.data.get("participants", [])
        logger.info(f"Disconnect reason: {disconnect_reason}")
        logger.info(f"Participants at disconnect: {participants}")

        # Clean up conversation state
        try:
            cm.persist_to_redis(redis_mgr)
            logger.info(f"Persisted conversation state after disconnect for call {cid}")
        except Exception as e:
            logger.error(
                f"Failed to persist conversation state after disconnect for call {cid}: {e}"
            )

    @staticmethod
    async def process_media_callbacks(
        events: list, cm: MemoManager, redis_mgr
    ) -> Dict[str, str]:
        """
        Process media callback events.

        Args:
            events: List of media events to process
            cm: MemoManager instance
            redis_mgr: Redis manager instance

        Returns:
            Dict with processing status
        """
        try:
            for event in events:
                data = event.get("data", {})
                etype = event.get("type", "")
                logger.info("Media callback received: %s\n\tEventType: %s", data, etype)

                if etype == "Microsoft.Communication.PlayStarted":
                    cm.update_context("bot_speaking", True)
                    logger.info(
                        f"PlayStarted: Set bot_speaking=True for call {cm.session_id}"
                    )
                elif etype == "Microsoft.Communication.PlayCompleted":
                    cm.update_context("bot_speaking", False)
                    logger.info(
                        f"PlayCompleted: Set bot_speaking=False for call {cm.session_id}"
                    )
                elif etype == "Microsoft.Communication.PlayFailed":
                    reason = data.get("resultInformation", "Unknown reason")
                    logger.error(f"⚠️ PlayFailed for call {cm.session_id}: {reason}")
                    cm.update_context("bot_speaking", False)
                    logger.info(
                        f"PlayFailed: Set bot_speaking=False for call {cm.session_id}"
                    )
                elif etype == "Microsoft.Communication.PlayCanceled":
                    cm.update_context("bot_speaking", False)
                    logger.info(
                        f"PlayCanceled: Set bot_speaking=False for call {cm.session_id}"
                    )
                elif etype == "Microsoft.Communication.MediaStreamingFailed":
                    reason = data.get("resultInformation", "Unknown reason")
                    logger.error(
                        f"⚠️ MediaStreamingFailed for call {cm.session_id}: {reason}"
                    )
                    cm.update_context("bot_speaking", False)
                    logger.info(
                        f"MediaStreamingFailed: Set bot_speaking=False for call {cm.session_id}"
                    )
                else:
                    logger.info("Media callback event not handled: %s", etype)

            await cm.persist_to_redis_async(redis_mgr)
            return {"status": "media callback processed"}

        except Exception as exc:
            logger.error("Media callback error: %s", exc, exc_info=True)
            return {"error": str(exc)}
