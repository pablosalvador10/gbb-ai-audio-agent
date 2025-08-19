from __future__ import annotations

"""OpenAI streaming + tool-call orchestration layer.

Handles GPT chat-completion streaming, TTS relay, and function-calling for the
real-time voice agent.

Environment Variables
--------------------
OPENAI_MAX_RETRIES : int, default=3
    Maximum number of retry attempts for rate-limited or failed API calls.
OPENAI_BASE_DELAY : float, default=1.0
    Base delay in seconds for exponential backoff retry logic.
OPENAI_MAX_DELAY : float, default=60.0
    Maximum delay in seconds for exponential backoff retry logic.
GPT_FLOW_TRACING : bool, default=true
    Enable/disable detailed tracing for GPT flow operations.
STREAM_TRACING : bool, default=false
    Enable/disable high-frequency tracing for streaming operations.

Public API
----------
process_gpt_response() – Stream completions, emit TTS chunks, run tools.
"""

import asyncio
import json
import os
import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from fastapi import WebSocket
from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode
from urllib.parse import urlparse
from openai import RateLimitError, APITimeoutError, APIConnectionError, APIError

from apps.rtagent.backend.settings import AZURE_OPENAI_CHAT_DEPLOYMENT_ID, TTS_END
from apps.rtagent.backend.src.agents.tool_store.tool_registry import (
    available_tools as DEFAULT_TOOLS,
)
from apps.rtagent.backend.src.agents.tool_store.tools_helper import (
    function_mapping,
    push_tool_end,
    push_tool_start,
)
from apps.rtagent.backend.src.helpers import add_space
from apps.rtagent.backend.src.services.openai_services import client as az_openai_client
from apps.rtagent.backend.src.shared_ws import (
    broadcast_message,
    push_final,
    send_response_to_acs,
    send_tts_audio,
)
from apps.rtagent.backend.settings import AZURE_OPENAI_ENDPOINT
from utils.ml_logging import get_logger
from utils.trace_context import create_trace_context
from apps.rtagent.backend.src.utils.tracing_utils import (
    create_service_handler_attrs,
    create_service_dependency_attrs,
)

if TYPE_CHECKING:  # pragma: no cover – typing-only import
    from src.stateful.state_managment import MemoManager  # noqa: F401

logger = get_logger("gpt_flow")

# Get OpenTelemetry tracer for Application Map
tracer = trace.get_tracer(__name__)

# Performance optimization: Cache tracing configuration
_GPT_FLOW_TRACING = os.getenv("GPT_FLOW_TRACING", "true").lower() == "true"
_STREAM_TRACING = os.getenv("STREAM_TRACING", "false").lower() == "true"  # High freq

# Rate limiting configuration
_MAX_RETRIES = int(os.getenv("OPENAI_MAX_RETRIES", "3"))
_BASE_DELAY = float(os.getenv("OPENAI_BASE_DELAY", "1.0"))  # Base delay in seconds
_MAX_DELAY = float(os.getenv("OPENAI_MAX_DELAY", "60.0"))   # Max delay in seconds

JSONDict = Dict[str, Any]


# ---------------------------------------------------------------------------
# Rate limiting and retry helpers
# ---------------------------------------------------------------------------

async def _exponential_backoff_delay(attempt: int, base_delay: float = _BASE_DELAY) -> float:
    """Calculate exponential backoff delay with jitter.

    :param attempt: Current attempt number (0-based).
    :param base_delay: Base delay in seconds.
    :return: Delay in seconds with jitter.
    """
    delay = min(base_delay * (2 ** attempt), _MAX_DELAY)
    # Add jitter (±25% of the delay) using time-based entropy (fast, no RNG deps)
    jitter = delay * 0.25 * (2 * (time.time() % 1) - 1)
    return max(0.1, delay + jitter)


async def _handle_openai_errors(
    exc: Exception,
    attempt: int,
    span: trace.Span,
    call_connection_id: Optional[str] = None,
    session_id: Optional[str] = None,
) -> bool:
    """Handle OpenAI API errors with proper logging and telemetry.

    :param exc: The exception that occurred.
    :param attempt: Current attempt number (0-based).
    :param span: Current tracing span.
    :param call_connection_id: For correlation logging.
    :param session_id: For correlation logging.
    :return: True if the error is retryable, False otherwise.
    """
    correlation_info = {
        "call_connection_id": call_connection_id,
        "session_id": session_id,
        "attempt": attempt + 1,
        "max_retries": _MAX_RETRIES,
    }

    if isinstance(exc, RateLimitError):
        logger.warning(
            "OpenAI rate limit exceeded (429) - attempt %d/%d | %s",
            attempt + 1, _MAX_RETRIES, correlation_info,
            extra={
                "error_type": "rate_limit",
                "error_code": 429,
                "retry_attempt": attempt + 1,
                "max_retries": _MAX_RETRIES,
                "call_connection_id": call_connection_id,
                "session_id": session_id,
                "error_message": str(exc),
            }
        )
        span.add_event(
            "rate_limit_error",
            {
                "error.type": "RateLimitError",
                "error.message": str(exc),
                "retry_attempt": attempt + 1,
                "max_retries": _MAX_RETRIES,
            }
        )
        return attempt < _MAX_RETRIES - 1

    elif isinstance(exc, APITimeoutError):
        logger.warning(
            "OpenAI API timeout - attempt %d/%d | %s",
            attempt + 1, _MAX_RETRIES, correlation_info,
            extra={
                "error_type": "timeout",
                "retry_attempt": attempt + 1,
                "max_retries": _MAX_RETRIES,
                "call_connection_id": call_connection_id,
                "session_id": session_id,
                "error_message": str(exc),
            }
        )
        span.add_event(
            "timeout_error",
            {
                "error.type": "APITimeoutError",
                "error.message": str(exc),
                "retry_attempt": attempt + 1,
                "max_retries": _MAX_RETRIES,
            }
        )
        return attempt < _MAX_RETRIES - 1

    elif isinstance(exc, APIConnectionError):
        logger.warning(
            "OpenAI API connection error - attempt %d/%d | %s",
            attempt + 1, _MAX_RETRIES, correlation_info,
            extra={
                "error_type": "connection",
                "retry_attempt": attempt + 1,
                "max_retries": _MAX_RETRIES,
                "call_connection_id": call_connection_id,
                "session_id": session_id,
                "error_message": str(exc),
            }
        )
        span.add_event(
            "connection_error",
            {
                "error.type": "APIConnectionError",
                "error.message": str(exc),
                "retry_attempt": attempt + 1,
                "max_retries": _MAX_RETRIES,
            }
        )
        return attempt < _MAX_RETRIES - 1

    elif isinstance(exc, APIError):
        # Non-retryable general API error
        logger.error(
            "OpenAI API error (non-retryable) - %s | %s",
            str(exc), correlation_info,
            extra={
                "error_type": "api_error",
                "call_connection_id": call_connection_id,
                "session_id": session_id,
                "error_message": str(exc),
            }
        )
        span.add_event(
            "api_error",
            {
                "error.type": "APIError",
                "error.message": str(exc),
                "retryable": False,
            }
        )
        return False

    else:
        # Unexpected error
        logger.error(
            "Unexpected error during OpenAI call - %s | %s",
            str(exc), correlation_info,
            extra={
                "error_type": "unexpected",
                "call_connection_id": call_connection_id,
                "session_id": session_id,
                "error_message": str(exc),
                "error_class": exc.__class__.__name__,
            }
        )
        span.add_event(
            "unexpected_error",
            {
                "error.type": exc.__class__.__name__,
                "error.message": str(exc),
                "retryable": False,
            }
        )
        return False


# ---------------------------------------------------------------------------
# Voice + sender helpers
# ---------------------------------------------------------------------------

def _get_agent_voice_config(cm: "MemoManager") -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """Extract agent voice configuration from memory manager.

    :param cm: The active MemoManager instance.
    :return: Tuple of (voice_name, voice_style, voice_rate) or (None, None, None).
    """
    if cm is None:
        logger.warning("MemoManager is None, using default voice configuration")
        return None, None, None

    try:
        voice_name = cm.get_value_from_corememory("current_agent_voice")
        voice_style = cm.get_value_from_corememory("current_agent_voice_style", "chat")
        voice_rate = cm.get_value_from_corememory("current_agent_voice_rate", "+3%")
        return voice_name, voice_style, voice_rate
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to get agent voice config: %s", exc)
        return None, None, None


def _get_agent_sender_name(cm: "MemoManager", *, include_autoauth: bool = True) -> str:
    """Resolve the visible sender name for dashboard / UI."""
    try:
        active_agent = cm.get_value_from_corememory("active_agent") if cm else None
        authenticated = cm.get_value_from_corememory("authenticated") if cm else False

        if active_agent == "Claims":
            return "Claims Specialist"
        if active_agent == "General":
            return "General Info"
        if include_autoauth and active_agent == "AutoAuth":
            return "Auth Agent"
        if not authenticated:
            return "Auth Agent"
        return "Assistant"
    except Exception:  # noqa: BLE001
        return "Assistant"


# ---------------------------------------------------------------------------
# Emission helpers
# ---------------------------------------------------------------------------

async def _emit_streaming_text(
    text: str,
    ws: WebSocket,
    is_acs: bool,
    cm: "MemoManager",
    call_connection_id: Optional[str] = None,
    session_id: Optional[str] = None,
) -> None:
    """Emit one assistant text chunk via either ACS or WebSocket + TTS."""
    voice_name, voice_style, voice_rate = _get_agent_voice_config(cm)

    if _STREAM_TRACING:
        span_attrs = create_service_handler_attrs(
            service_name="gpt_flow",
            call_connection_id=call_connection_id,
            session_id=session_id,
            operation="emit_streaming_text",
            text_length=len(text),
            is_acs=is_acs,
            chunk_type="streaming_text",
        )
        with tracer.start_as_current_span(
            "gpt_flow.emit_streaming_text", attributes=span_attrs
        ) as span:
            try:
                if is_acs:
                    span.set_attribute("output_channel", "acs")
                    await send_response_to_acs(
                        ws,
                        text,
                        voice_name=voice_name,
                        voice_style=voice_style,
                        rate=voice_rate,
                    )
                else:
                    span.set_attribute("output_channel", "websocket_tts")
                    await send_tts_audio(
                        text,
                        ws,
                        voice_name=voice_name,
                        voice_style=voice_style,
                        rate=voice_rate,
                    )
                    speaker = _get_agent_sender_name(cm, include_autoauth=True)
                    await ws.send_text(
                        json.dumps(
                            {
                                "type": "assistant_streaming",
                                "content": text,
                                "speaker": speaker,
                            }
                        )
                    )
                span.add_event("text_emitted", {"text_length": len(text)})
            except Exception as exc:  # noqa: BLE001
                span.record_exception(exc)
                logger.error(
                    "Failed to emit streaming text | text_length: %d | is_acs: %s | call_id: %s | session: %s | error: %s",
                    len(text), is_acs, call_connection_id, session_id, str(exc),
                    extra={
                        "text_length": len(text),
                        "is_acs": is_acs,
                        "call_connection_id": call_connection_id,
                        "session_id": session_id,
                        "error_type": exc.__class__.__name__,
                        "error_message": str(exc),
                        "voice_name": voice_name,
                        "voice_style": voice_style,
                        "voice_rate": voice_rate,
                    }
                )
                raise
    else:
        # Fast path without high-frequency tracing
        try:
            if is_acs:
                await send_response_to_acs(
                    ws,
                    text,
                    voice_name=voice_name,
                    voice_style=voice_style,
                    rate=voice_rate,
                )
            else:
                await send_tts_audio(
                    text,
                    ws,
                    voice_name=voice_name,
                    voice_style=voice_style,
                    rate=voice_rate,
                )
                speaker = _get_agent_sender_name(cm, include_autoauth=True)
                await ws.send_text(
                    json.dumps(
                        {"type": "assistant_streaming", "content": text, "speaker": speaker}
                    )
                )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Failed to emit streaming text (fast path) | text_length: %d | is_acs: %s | call_id: %s | session: %s | error: %s",
                len(text), is_acs, call_connection_id, session_id, str(exc),
                extra={
                    "text_length": len(text),
                    "is_acs": is_acs,
                    "call_connection_id": call_connection_id,
                    "session_id": session_id,
                    "error_type": exc.__class__.__name__,
                    "error_message": str(exc),
                    "voice_name": voice_name,
                    "voice_style": voice_style,
                    "voice_rate": voice_rate,
                }
            )
            raise


async def _broadcast_dashboard(
    ws: WebSocket,
    cm: "MemoManager",
    message: str,
    *,
    include_autoauth: bool,
) -> None:
    """Broadcast a message to the relay dashboard with correct speaker label."""
    try:
        sender = _get_agent_sender_name(cm, include_autoauth=include_autoauth)
        logger.info(
            "🎯 _broadcast_dashboard called: sender='%s', include_autoauth=%s, message='%s...'",
            sender, include_autoauth, message[:50]
        )
        clients = await ws.app.state.websocket_manager.get_clients_snapshot()
        await broadcast_message(clients, message, sender)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to broadcast dashboard message: %s", exc)


# ---------------------------------------------------------------------------
# Chat + streaming helpers
# ---------------------------------------------------------------------------

def _build_chat_kwargs(
    *,
    history: List[JSONDict],
    model_id: str,
    temperature: float,
    top_p: float,
    max_tokens: int,
    tools: Optional[List[JSONDict]],
) -> JSONDict:
    """Build Azure OpenAI chat-completions kwargs."""
    return {
        "stream": True,
        "messages": history,
        "model": model_id,
        "max_tokens": max_tokens,
        "temperature": temperature,
        "top_p": top_p,
        "tools": tools or [],
        "tool_choice": "auto" if (tools or []) else "none",
    }


class _ToolCallState:
    """Minimal state carrier for a single tool call parsed from stream deltas."""

    def __init__(self) -> None:
        self.started: bool = False
        self.name: str = ""
        self.call_id: str = ""
        self.args_json: str = ""


async def _consume_openai_stream(
    response_stream: Any,
    ws: WebSocket,
    is_acs: bool,
    cm: "MemoManager",
    call_connection_id: Optional[str],
    session_id: Optional[str],
) -> Tuple[str, _ToolCallState]:
    """Consume the AOAI stream, emitting TTS chunks as punctuation arrives."""
    collected: List[str] = []       # temporary sentence buffer
    final_chunks: List[str] = []    # full assistant text
    tool = _ToolCallState()
    chunk_count = 0

    try:
        for chunk in response_stream:
            chunk_count += 1

            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta

            # Tool-call aggregation (function name + arguments as they stream)
            if getattr(delta, "tool_calls", None):
                tc = delta.tool_calls[0]
                tool.call_id = tc.id or tool.call_id
                tool.name = getattr(tc.function, "name", None) or tool.name
                tool.args_json += getattr(tc.function, "arguments", None) or ""
                if not tool.started:
                    tool.started = True
                    logger.debug(
                        "Tool call detected during streaming | tool: %s | call_id: %s | session: %s",
                        tool.name, call_connection_id, session_id,
                        extra={
                            "tool_name": tool.name,
                            "call_connection_id": call_connection_id,
                            "session_id": session_id,
                            "chunk_count": chunk_count,
                        }
                    )
                continue

            # Text streaming (flush on boundaries in TTS_END)
            if getattr(delta, "content", None):
                collected.append(delta.content)
                if delta.content in TTS_END:
                    streaming = add_space("".join(collected).strip())
                    logger.debug(
                        "Streaming text chunk | length: %d | call_id: %s | session: %s",
                        len(streaming), call_connection_id, session_id,
                        extra={
                            "chunk_length": len(streaming),
                            "call_connection_id": call_connection_id,
                            "session_id": session_id,
                            "chunk_count": chunk_count,
                            "text_preview": streaming[:50] + "..." if len(streaming) > 50 else streaming,
                        }
                    )
                    await _emit_streaming_text(
                        streaming, ws, is_acs, cm, call_connection_id, session_id
                    )
                    final_chunks.append(streaming)
                    collected.clear()

        # Handle trailing content (no terminating punctuation)
        if collected:
            pending = "".join(collected).strip()
            if pending:
                logger.debug(
                    "Handling trailing content | length: %d | call_id: %s | session: %s",
                    len(pending), call_connection_id, session_id,
                    extra={
                        "trailing_length": len(pending),
                        "call_connection_id": call_connection_id,
                        "session_id": session_id,
                        "total_chunks": chunk_count,
                    }
                )
                await _emit_streaming_text(pending, ws, is_acs, cm, call_connection_id, session_id)
                final_chunks.append(pending)

        logger.info(
            "Stream consumption completed | chunks: %d | text_length: %d | tool_detected: %s | call_id: %s",
            chunk_count, len("".join(final_chunks)), tool.started, call_connection_id,
            extra={
                "total_chunks": chunk_count,
                "total_text_length": len("".join(final_chunks)),
                "tool_detected": tool.started,
                "tool_name": tool.name if tool.started else None,
                "call_connection_id": call_connection_id,
                "session_id": session_id,
            }
        )

    except Exception as exc:
        logger.error(
            "Error during stream consumption | chunks_processed: %d | call_id: %s | session: %s | error: %s",
            chunk_count, call_connection_id, session_id, str(exc),
            extra={
                "chunks_processed": chunk_count,
                "call_connection_id": call_connection_id,
                "session_id": session_id,
                "error_type": exc.__class__.__name__,
                "error_message": str(exc),
                "collected_length": len("".join(collected)),
                "final_chunks_length": len("".join(final_chunks)),
            }
        )
        # Re-raise to let the caller handle the retry logic
        raise

    return "".join(final_chunks).strip(), tool


# ---------------------------------------------------------------------------
# Turn correlation helper (no behavior change; for metrics only)
# ---------------------------------------------------------------------------

def _ensure_turn(ws: WebSocket, cm: "MemoManager", *, agent: str) -> None:
    """Bind cm's tracker to an existing ws.state.turn_id or create one."""
    if not cm:
        return
    try:
        existing = getattr(ws.state, "turn_id", None)
        if existing:
            cm.start_turn(existing, agent=agent)
        else:
            new_id = cm.start_turn(agent=agent)
            if not new_id:
                # Very conservative fallback id if tracker returns empty
                new_id = f"{cm.session_id}:t{int(time.time() * 1000)}"
                cm.start_turn(new_id, agent=agent)
            ws.state.turn_id = new_id  # type: ignore[attr-defined]
    except Exception:
        logger.exception("Failed to bind/start turn for agent=%s", agent)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def process_gpt_response(  # noqa: D401
    cm: "MemoManager",
    user_prompt: str,
    ws: WebSocket,
    *,
    agent_name: str,
    is_acs: bool = False,
    model_id: str = AZURE_OPENAI_CHAT_DEPLOYMENT_ID,
    temperature: float = 0.5,
    top_p: float = 1.0,
    max_tokens: int = 4096,
    available_tools: Optional[List[Dict[str, Any]]] = None,
    call_connection_id: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Stream a chat completion, emitting TTS and handling tool calls."""
    # Ensure we have a turn id for correlation (no functional impact)
    _ensure_turn(ws, cm, agent="gpt_stream")

    # Create handler span for GPT flow service
    span_attrs = create_service_handler_attrs(
        service_name="gpt_flow",
        call_connection_id=call_connection_id,
        session_id=session_id,
        operation="process_response",
        agent_name=agent_name,
        model_id=model_id,
        is_acs=is_acs,
        temperature=temperature,
        top_p=top_p,
        max_tokens=max_tokens,
        tools_available=len(available_tools or DEFAULT_TOOLS),
        prompt_length=len(user_prompt) if user_prompt else 0,
    )

    with tracer.start_as_current_span(
        "gpt_flow.process_response", attributes=span_attrs
    ) as span:
        # Build history and tools
        agent_history: List[JSONDict] = cm.get_history(agent_name)
        agent_history.append({"role": "user", "content": user_prompt})
        tool_set = available_tools or DEFAULT_TOOLS
        span.set_attribute("tools.count", len(tool_set))

        chat_kwargs = _build_chat_kwargs(
            history=agent_history,
            model_id=model_id,
            temperature=temperature,
            top_p=top_p,
            max_tokens=max_tokens,
            tools=tool_set,
        )
        span.set_attribute("chat.history_length", len(agent_history))
        logger.debug("process_gpt_response – chat kwargs prepared: %s", chat_kwargs)

        # Create dependency span for calling Azure OpenAI
        azure_openai_attrs = create_service_dependency_attrs(
            source_service="gpt_flow",
            target_service="azure_openai",
            call_connection_id=call_connection_id,
            session_id=session_id,
            operation="stream_completion",
            model=model_id,
            stream=True,
        )
        aoai_endpoint = AZURE_OPENAI_ENDPOINT
        host = urlparse(aoai_endpoint).netloc or "api.openai.azure.com"

        tool_state = _ToolCallState()
        full_text: str = ""
        attempt = 0

        while attempt < _MAX_RETRIES:
            try:
                with tracer.start_as_current_span(
                    "gpt_flow.stream_completion",
                    kind=SpanKind.CLIENT,
                    attributes={
                        **azure_openai_attrs,
                        "peer.service": "azure-openai",
                        "server.address": host,
                        "server.port": 443,
                        "http.method": "POST",
                        "http.url": f"https://{host}/openai/deployments/{model_id}/chat/completions",
                        "pipeline.stage": "orchestrator -> aoai",
                        "retry_attempt": attempt + 1,
                        "max_retries": _MAX_RETRIES,
                    },
                ) as stream_span:
                    logger.info(
                        "Calling Azure OpenAI - attempt %d/%d | call_id: %s | session: %s | model: %s",
                        attempt + 1, _MAX_RETRIES, call_connection_id, session_id, model_id,
                        extra={
                            "call_connection_id": call_connection_id,
                            "session_id": session_id,
                            "model_id": model_id,
                            "attempt": attempt + 1,
                            "max_retries": _MAX_RETRIES,
                        }
                    )

                    response = az_openai_client.chat.completions.create(**chat_kwargs)
                    stream_span.add_event("openai_stream_started")

                    # Consume the stream and emit chunks as before
                    full_text, tool_state = await _consume_openai_stream(
                        response, ws, is_acs, cm, call_connection_id, session_id
                    )

                    stream_span.set_attribute("tool_call_detected", tool_state.started)
                    if tool_state.started:
                        stream_span.set_attribute("tool_name", tool_state.name)

                    # Success - break out of retry loop
                    logger.info(
                        "Azure OpenAI call successful on attempt %d | call_id: %s | session: %s",
                        attempt + 1, call_connection_id, session_id,
                        extra={
                            "call_connection_id": call_connection_id,
                            "session_id": session_id,
                            "attempt": attempt + 1,
                            "response_length": len(full_text) if full_text else 0,
                            "tool_call_detected": tool_state.started,
                        }
                    )
                    break

            except Exception as exc:
                # Decide whether to retry
                should_retry = await _handle_openai_errors(
                    exc, attempt, stream_span, call_connection_id, session_id
                )
                if not should_retry or attempt >= _MAX_RETRIES - 1:
                    logger.error(
                        "Azure OpenAI call failed after %d attempts | call_id: %s | session: %s | error: %s",
                        attempt + 1, call_connection_id, session_id, str(exc),
                        extra={
                            "call_connection_id": call_connection_id,
                            "session_id": session_id,
                            "final_attempt": attempt + 1,
                            "max_retries": _MAX_RETRIES,
                            "error_type": exc.__class__.__name__,
                            "error_message": str(exc),
                        }
                    )
                    span.record_exception(exc)
                    span.set_status(Status(StatusCode.ERROR, str(exc)))
                    raise

                # Backoff & retry
                delay = await _exponential_backoff_delay(attempt)
                logger.info(
                    "Retrying Azure OpenAI call in %.2fs | attempt %d/%d | call_id: %s",
                    delay, attempt + 2, _MAX_RETRIES, call_connection_id,
                    extra={
                        "call_connection_id": call_connection_id,
                        "session_id": session_id,
                        "retry_delay": delay,
                        "next_attempt": attempt + 2,
                        "max_retries": _MAX_RETRIES,
                    }
                )
                await asyncio.sleep(delay)
                attempt += 1

        # Finalize assistant text
        if full_text:
            agent_history.append({"role": "assistant", "content": full_text})
            await push_final(ws, "assistant", full_text, is_acs=is_acs)
            # Broadcast the final assistant response to relay dashboard
            await _broadcast_dashboard(
                ws, cm, full_text, include_autoauth=False  # preserve legacy behavior
            )
            span.set_attribute("response.length", len(full_text))

        # Handle follow-up tool call (if any)
        if tool_state.started:
            span.add_event(
                "tool_execution_starting",
                {"tool_name": tool_state.name, "tool_id": tool_state.call_id},
            )

            agent_history.append(
                {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": tool_state.call_id,
                            "type": "function",
                            "function": {
                                "name": tool_state.name,
                                "arguments": tool_state.args_json,
                            },
                        }
                    ],
                }
            )
            result = await _handle_tool_call(
                tool_state.name,
                tool_state.call_id,
                tool_state.args_json,
                cm,
                ws,
                agent_name,
                is_acs,
                model_id,
                temperature,
                top_p,
                max_tokens,
                tool_set,
                call_connection_id,
                session_id,
            )
            if result is not None:
                # Persist tool output and update slots in the background
                async def persist_tool_results() -> None:
                    cm.persist_tool_output(tool_state.name, result)
                    if isinstance(result, dict) and "slots" in result:
                        cm.update_slots(result["slots"])

                asyncio.create_task(persist_tool_results())
                span.set_attribute("tool.execution_success", True)
                span.add_event("tool_execution_completed", {"tool_name": tool_state.name})
            return result

        span.set_attribute("completion_type", "text_only")
        return None


# ---------------------------------------------------------------------------
# Tool handling
# ---------------------------------------------------------------------------

async def _handle_tool_call(  # noqa: PLR0913
    tool_name: str,
    tool_id: str,
    args: str,
    cm: "MemoManager",
    ws: WebSocket,
    agent_name: str,
    is_acs: bool,
    model_id: str,
    temperature: float,
    top_p: float,
    max_tokens: int,
    available_tools: List[Dict[str, Any]],
    call_connection_id: Optional[str] = None,
    session_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute a tool, emit telemetry events, and trigger GPT follow-up.

    :raises ValueError: If tool_name does not exist in function_mapping.
    :return: Parsed result from the tool execution.
    """
    with create_trace_context(
        name="gpt_flow.handle_tool_call",
        call_connection_id=call_connection_id,
        session_id=session_id,
        metadata={
            "tool_name": tool_name,
            "tool_id": tool_id,
            "agent_name": agent_name,
            "is_acs": is_acs,
            "args_length": len(args) if args else 0,
        },
    ) as trace_ctx:
        params: JSONDict = json.loads(args or "{}")
        fn = function_mapping.get(tool_name)
        if fn is None:
            trace_ctx.set_attribute("error", f"Unknown tool '{tool_name}'")
            raise ValueError(f"Unknown tool '{tool_name}'")

        trace_ctx.set_attribute("tool.parameters_count", len(params))
        call_short_id = uuid.uuid4().hex[:8]
        trace_ctx.set_attribute("tool.call_id", call_short_id)

        await push_tool_start(ws, call_short_id, tool_name, params, is_acs=is_acs)
        trace_ctx.add_event("tool_start_pushed", {"call_id": call_short_id})

        # Execute tool with nested tracing
        with create_trace_context(
            name=f"gpt_flow.execute_tool.{tool_name}",
            call_connection_id=call_connection_id,
            session_id=session_id,
            metadata={"tool_name": tool_name, "call_id": call_short_id, "parameters": params},
        ) as exec_ctx:
            t0 = time.perf_counter()
            result_raw = await fn(params)  # Tool functions are expected to be async.
            elapsed_ms = (time.perf_counter() - t0) * 1000

            exec_ctx.set_attribute("execution.duration_ms", elapsed_ms)
            exec_ctx.set_attribute("execution.success", True)

            result: JSONDict = json.loads(result_raw) if isinstance(result_raw, str) else result_raw
            exec_ctx.set_attribute("result.type", type(result).__name__)

        agent_history = cm.get_history(agent_name)
        agent_history.append(
            {
                "tool_call_id": tool_id,
                "role": "tool",
                "name": tool_name,
                "content": json.dumps(result),
            }
        )

        await push_tool_end(
            ws, call_short_id, tool_name, "success", elapsed_ms, result=result, is_acs=is_acs
        )
        trace_ctx.add_event("tool_end_pushed", {"elapsed_ms": elapsed_ms})

        # Broadcast tool completion to relay dashboard (only for ACS calls)
        if is_acs:
            await _broadcast_dashboard(ws, cm, f"🛠️ {tool_name} ✔️", include_autoauth=False)

        # Handle tool follow-up with tracing
        trace_ctx.add_event("starting_tool_followup")
        await _process_tool_followup(
            cm,
            ws,
            agent_name,
            is_acs,
            model_id,
            temperature,
            top_p,
            max_tokens,
            available_tools,
            call_connection_id,
            session_id,
        )

        trace_ctx.set_attribute("tool.execution_complete", True)
        return result


async def _process_tool_followup(  # noqa: PLR0913
    cm: "MemoManager",
    ws: WebSocket,
    agent_name: str,
    is_acs: bool,
    model_id: str,
    temperature: float,
    top_p: float,
    max_tokens: int,
    available_tools: List[Dict[str, Any]],
    call_connection_id: Optional[str] = None,
    session_id: Optional[str] = None,
) -> None:
    """Invoke GPT once more after tool execution (no new user input)."""
    with create_trace_context(
        name="gpt_flow.tool_followup",
        call_connection_id=call_connection_id,
        session_id=session_id,
        metadata={
            "agent_name": agent_name,
            "model_id": model_id,
            "is_acs": is_acs,
            "followup_type": "post_tool_execution",
        },
    ) as trace_ctx:
        trace_ctx.add_event("starting_followup_completion")

        await process_gpt_response(
            cm,
            "",  # No new user prompt.
            ws,
            agent_name=agent_name,
            is_acs=is_acs,
            model_id=model_id,
            temperature=temperature,
            top_p=top_p,
            max_tokens=max_tokens,
            available_tools=available_tools,
            call_connection_id=call_connection_id,
            session_id=session_id,
        )

        trace_ctx.add_event("followup_completion_finished")
