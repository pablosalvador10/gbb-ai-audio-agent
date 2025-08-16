from __future__ import annotations

"""
Call Management Endpoints
========================

REST API endpoints for managing phone calls through Azure Communication Services.
"""
import re
import uuid
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Request, HTTPException, status, Query
from fastapi.responses import JSONResponse
from opentelemetry import trace
from azure.core.messaging import CloudEvent

from apps.rtagent.backend.src.utils.tracing import (
    trace_acs_operation,
    trace_acs_dependency,
)
from apps.rtagent.backend.api.v1.schemas.call import (
    CallInitiateRequest,
    CallInitiateResponse,
    CallStatusResponse,
    CallHangupResponse,
    CallListResponse,
    CallUpdateRequest,
)
from utils.ml_logging import get_logger

# V1 event system
from ..events import CallEventProcessor, ACSEventTypes  # noqa: F401  (used via processor)
from ..events import get_call_event_processor, register_default_handlers

logger = get_logger("api.v1.calls")
tracer = trace.get_tracer(__name__)

# IMPORTANT: v1 router will mount this APIRouter with /calls prefix
router = APIRouter(tags=["Call Management"])

_E164 = re.compile(r"^\+\d{6,15}$")


# ---------------------------------------------------------------------------
# Local helpers (avoid cyclic imports)
# ---------------------------------------------------------------------------
async def _broadcast_to_session(app, session_id: str, payload: Dict[str, Any]) -> int:
    """
    Send JSON payload to all websockets registered under a session_id.
    Returns the count of successful sends. Adds session_id to payload.
    """
    import json
    from starlette.websockets import WebSocketState

    data = json.dumps({**payload, "session_id": session_id})

    # Snapshot under lock to avoid iterator races; send outside lock
    async with app.state.session_lock:
        targets = tuple(app.state.session_sockets.get(session_id, ()))

    sent = 0
    for ws in targets:
        if ws.application_state == WebSocketState.CONNECTED:
            try:
                await ws.send_text(data)
                sent += 1
            except Exception:
                pass
    return sent


async def _broadcast_dashboards(app, payload: Dict[str, Any]) -> int:
    """Best-effort broadcast to operator dashboards."""
    import json
    from starlette.websockets import WebSocketState

    data = json.dumps(payload)
    sent = 0
    for ws in tuple(getattr(app.state, "clients", set())):
        if ws.application_state == WebSocketState.CONNECTED:
            try:
                await ws.send_text(data)
                sent += 1
            except Exception:
                pass
    return sent


def _create_call_event(event_type: str, call_id: str, data: dict) -> CloudEvent:
    """
    Create a CloudEvent for call-related operations using the V1 event system.
    """
    return CloudEvent(
        source="api/v1/calls",
        type=event_type,
        data={"callConnectionId": call_id, **data},
    )


def _require_dependency(obj, name: str):
    if not obj:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"{name} not initialised",
        )


# ---------------------------------------------------------------------------
# POST /api/v1/calls/initiate
# ---------------------------------------------------------------------------
@router.post(
    "/initiate",
    response_model=CallInitiateResponse,
    summary="Initiate Outbound Call",
    description="""
Initiate a new outbound call to the specified phone number.

- Validates phone number (E.164)
- Emits V1 call events
- Registers call_id → session_id mapping safely (if provided)
- Immediately broadcasts status to dashboards and (if provided) the browser session
""",
)
async def initiate_call(
    req: CallInitiateRequest,
    http_request: Request,
) -> CallInitiateResponse:
    app = http_request.app

    # Validate dependencies up front
    _require_dependency(getattr(app.state, "acs_caller", None), "ACS")
    _require_dependency(getattr(app.state, "redis", None), "Redis")

    # Validate phone
    number = req.target_number
    if not _E164.match(number):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid phone number format. Must be E.164 (e.g., +1234567890)",
        )

    # Try to capture session_id from the model; fallbacks for compatibility
    session_id: Optional[str] = req.session_id or http_request.query_params.get("session_id") or http_request.headers.get("x-session-id")

    with trace_acs_operation(
        tracer, logger, "initiate_call", session_id=session_id, call_connection_id=None
    ) as op:
        op.log_info(f"Initiating call to {number} (session_id={session_id})")

        try:
            # Start outbound call via lifecycle handler
            from ..handlers.acs_call_lifecycle import ACSLifecycleHandler

            acs_handler = ACSLifecycleHandler()

            with trace_acs_dependency(
                tracer, logger, "acs_lifecycle", "start_outbound_call"
            ) as dep_op:
                result = await acs_handler.start_outbound_call(
                    acs_caller=app.state.acs_caller,
                    target_number=number,
                    redis_mgr=app.state.redis,
                )

            if result.get("status") != "success":
                message = result.get("message", "Unknown error")
                op.set_error(message)
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Call initiation failed: {message}",
                )

            call_id = result.get("callId") or result.get("call_id") or str(uuid.uuid4())
            initiated_at = result.get("initiated_at")

            # --- Register call_id → session_id (race-safe) ---
            if session_id:
                async with app.state.session_lock:
                    app.state.call_session[call_id] = session_id

            # --- Emit V1 event through the processor (handlers can broadcast) ---
            register_default_handlers()  # idempotent
            processor = get_call_event_processor()
            initiated_event = _create_call_event(
                event_type="CallInitiated",
                call_id=call_id,
                data={
                    "target_number": number,
                    "initiated_at": initiated_at,
                    "api_version": "v1",
                    "status": "initiating",
                    "session_id": session_id,
                },
            )
            await processor.process_events([initiated_event], app.state)

            # --- Broadcast immediate status to dashboards and session (best-effort) ---
            payload = {
                "type": "call_status",
                "stage": "initiating",
                "call_id": call_id,
                "target_number": number,
                "message": f"Call initiation requested for {number}",
            }
            await _broadcast_dashboards(app, payload)
            if session_id:
                await _broadcast_to_session(app, session_id, payload)

            op.log_info(f"Call initiated successfully: {call_id}")

            return CallInitiateResponse(
                call_id=call_id,
                status="initiating",
                target_number=number,
                message=result.get("message", "call initiated successfully"),
                initiated_at=initiated_at,
                details={"api_version": "v1", "acs_result": result},
            )

        except HTTPException:
            raise
        except Exception as e:
            op.set_error(str(e))
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Internal server error: {str(e)}",
            )


# ---------------------------------------------------------------------------
# GET /api/v1/calls
# ---------------------------------------------------------------------------
@router.get(
    "/",
    response_model=CallListResponse,
    summary="List Calls",
    description="""
Retrieve a paginated list of calls with optional filtering.

- Pagination (page/limit)
- Filter by status
- Newest first (handled by your store/query)
""",
)
async def list_calls(
    request: Request,
    page: int = Query(1, ge=1, description="Page number (1-based)", example=1),
    limit: int = Query(10, ge=1, le=100, description="Items per page", example=10),
    status_filter: Optional[str] = Query(
        None,
        enum=["initiating", "ringing", "connected", "on_hold", "disconnected", "failed"],
        description="Optional call status filter",
    ),
) -> CallListResponse:
    app = request.app
    _require_dependency(getattr(app.state, "cosmos", None), "CosmosDB")

    with trace_acs_operation(tracer, logger, "list_calls") as op:
        try:
            # Build query
            query_filter: Dict[str, Any] = {}
            if status_filter:
                query_filter["status"] = status_filter

            # Query and page
            all_docs = app.state.cosmos.query_documents(query_filter)
            call_docs = [d for d in all_docs if "call_id" in d]

            start = (page - 1) * limit
            end = start + limit
            paginated = call_docs[start:end]

            calls = [
                CallStatusResponse(
                    call_id=doc.get("call_id", doc.get("_id", "unknown")),
                    status=doc.get("status", "unknown"),
                    duration=doc.get("duration", 0),
                    participants=doc.get("participants", []),
                    events=doc.get("events", []),
                )
                for doc in paginated
            ]

            # Optional: emit a lightweight analytic event (fire-and-forget)
            if call_docs:
                try:
                    processor = get_call_event_processor()
                    evt = _create_call_event(
                        "CallListRequested",
                        call_id="api-operation",
                        data={
                            "total_calls": len(call_docs),
                            "returned_calls": len(calls),
                            "page": page,
                            "limit": limit,
                            "status_filter": status_filter,
                            "api_version": "v1",
                        },
                    )
                    import asyncio

                    asyncio.create_task(processor.process_events([evt], app.state))
                except Exception as e:
                    op.log_info(f"List analytics emit failed: {e}")

            return CallListResponse(calls=calls, total=len(call_docs), page=page, limit=limit)

        except Exception as e:
            op.set_error(str(e))
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to list calls: {str(e)}",
            )


# ---------------------------------------------------------------------------
# POST /api/v1/calls/answer  (Event Grid validation + inbound answer)
# ---------------------------------------------------------------------------
@router.post(
    "/answer",
    summary="Answer Inbound Call (Event Grid)",
    description="""
Handle Event Grid validation and answer inbound calls via ACS.
""",
)
async def answer_inbound_call(http_request: Request) -> JSONResponse:
    app = http_request.app
    _require_dependency(getattr(app.state, "acs_caller", None), "ACS")

    with trace_acs_operation(tracer, logger, "answer_inbound_call") as op:
        try:
            body = await http_request.json()

            from ..handlers.acs_call_lifecycle import ACSLifecycleHandler

            acs_handler = ACSLifecycleHandler()
            with trace_acs_dependency(tracer, logger, "acs_lifecycle", "accept_inbound_call"):
                result = await acs_handler.accept_inbound_call(
                    request_body=body,
                    acs_caller=app.state.acs_caller,
                )

            op.log_info("Inbound call processed successfully")
            return result

        except Exception as exc:
            op.set_error(str(exc))
            return JSONResponse({"error": str(exc)}, status_code=500)


# ---------------------------------------------------------------------------
# POST /api/v1/calls/callbacks  (ACS webhooks)
# ---------------------------------------------------------------------------
@router.post(
    "/callbacks",
    summary="Handle ACS Callback Events",
    description="""
Receive and process Azure Communication Services callback events.
""",
)
async def handle_acs_callbacks(http_request: Request):
    app = http_request.app
    _require_dependency(getattr(app.state, "acs_caller", None), "ACS")

    try:
        events_data = await http_request.json()

        # Best-effort extract of callConnectionId for tracing
        call_connection_id: Optional[str] = None
        if isinstance(events_data, list) and events_data:
            first = events_data[0]
            if isinstance(first, dict):
                d = first.get("data", {})
                if isinstance(d, dict):
                    call_connection_id = d.get("callConnectionId")
        elif isinstance(events_data, dict):
            d = events_data.get("data", {})
            if isinstance(d, dict):
                call_connection_id = d.get("callConnectionId")

        with trace_acs_operation(
            tracer, logger, "process_callbacks", call_connection_id=call_connection_id
        ) as op:
            register_default_handlers()  # idempotent
            processor = get_call_event_processor()

            # Normalize to CloudEvent list
            cloud_events: List[CloudEvent] = []
            if isinstance(events_data, list):
                for item in events_data:
                    if isinstance(item, dict):
                        ev_type = item.get("eventType") or item.get("type", "Unknown")
                        cloud_events.append(
                            CloudEvent(
                                source="azure.communication.callautomation",
                                type=ev_type,
                                data=item.get("data", item),
                            )
                        )
            elif isinstance(events_data, dict):
                ev_type = events_data.get("eventType") or events_data.get("type", "Unknown")
                cloud_events.append(
                    CloudEvent(
                        source="azure.communication.callautomation",
                        type=ev_type,
                        data=events_data.get("data", events_data),
                    )
                )

            result = await processor.process_events(cloud_events, app.state)

            op.log_info(f"Processed {result.get('processed', 0)} ACS events")
            return JSONResponse(
                {
                    "status": "success",
                    "processed_events": result.get("processed", 0),
                    "failed_events": result.get("failed", 0),
                    "call_connection_id": call_connection_id,
                    "processing_system": "events_v1",
                },
                status_code=200,
            )

    except Exception as exc:
        logger.error(f"Unexpected error processing ACS callbacks: {exc}")
        return JSONResponse({"error": str(exc)}, status_code=500)
