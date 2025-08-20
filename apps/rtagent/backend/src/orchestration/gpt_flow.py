from __future__ import annotations

"""
OpenAI streaming + tool-call orchestration layer.

Public API
----------
process_gpt_response() – Stream completions, emit TTS chunks, run tools,
                         and (optionally) run a single follow-up completion.
"""

import asyncio
import json
import os
import random
import time
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

from fastapi import WebSocket
from opentelemetry import trace
from opentelemetry.trace import SpanKind
from urllib.parse import urlparse

from apps.rtagent.backend.settings import (
    AZURE_OPENAI_CHAT_DEPLOYMENT_ID,
    AZURE_OPENAI_ENDPOINT,
    TTS_END,
)
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
from utils.ml_logging import get_logger
from utils.trace_context import create_trace_context
from apps.rtagent.backend.src.utils.tracing import (
    create_service_handler_attrs,
    create_service_dependency_attrs,
)

if TYPE_CHECKING:  # pragma: no cover
    from src.stateful.state_managment import MemoManager  # noqa: F401

# ---------------------------------------------------------------------------
# Logging / Tracing
# ---------------------------------------------------------------------------
logger = get_logger("orchestration.gpt_flow")
tracer = trace.get_tracer(__name__)

_STREAM_TRACING = os.getenv("STREAM_TRACING", "false").lower() == "true"  # High freq

JSONDict = Dict[str, Any]

# ---------------------------------------------------------------------------
# Retry / Rate-limit configuration
# ---------------------------------------------------------------------------
def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except Exception:
        return default

AOAI_RETRY_MAX_ATTEMPTS: int = int(os.getenv("AOAI_RETRY_MAX_ATTEMPTS", "4"))
AOAI_RETRY_BASE_DELAY_SEC: float = _env_float("AOAI_RETRY_BASE_DELAY_SEC", 0.5)
AOAI_RETRY_MAX_DELAY_SEC: float = _env_float("AOAI_RETRY_MAX_DELAY_SEC", 8.0)
AOAI_RETRY_BACKOFF_FACTOR: float = _env_float("AOAI_RETRY_BACKOFF_FACTOR", 2.0)
AOAI_RETRY_JITTER_SEC: float = _env_float("AOAI_RETRY_JITTER_SEC", 0.2)

# Limit model-driven “follow-up” loops to avoid recursion spirals.
MAX_TOOL_FOLLOWUPS = int(os.getenv("MAX_TOOL_FOLLOWUPS", "2"))

@dataclass
class RateLimitInfo:
    request_id: Optional[str] = None
    retry_after: Optional[float] = None
    region: Optional[str] = None
    remaining_requests: Optional[int] = None
    remaining_tokens: Optional[int] = None
    reset_requests: Optional[str] = None
    reset_tokens: Optional[str] = None
    limit_requests: Optional[int] = None
    limit_tokens: Optional[int] = None

# ---------------------------------------------------------------------------
# Header / RL helpers
# ---------------------------------------------------------------------------
def _parse_int(val: Optional[str]) -> Optional[int]:
    try:
        return int(val) if val is not None and val != "" else None
    except Exception:
        return None

def _parse_float(val: Optional[str]) -> Optional[float]:
    try:
        return float(val) if val is not None and val != "" else None
    except Exception:
        return None

def _extract_headers(container: Any) -> Dict[str, str]:
    """Best-effort header extraction from various SDK shapes."""
    cand_attrs = ("headers", "response", "http_response", "_response")
    headers: Optional[Dict[str, str]] = None

    if hasattr(container, "headers") and isinstance(container.headers, dict):
        headers = container.headers
    if headers is None:
        for attr in cand_attrs:
            obj = getattr(container, attr, None)
            if obj is None:
                continue
            maybe = getattr(obj, "headers", None)
            if isinstance(maybe, dict):
                headers = maybe
                break
            if callable(getattr(obj, "headers", None)):
                try:
                    h = obj.headers()
                    if isinstance(h, dict):
                        headers = h
                        break
                except Exception:
                    pass
    if headers is None:
        logger.warning(
            "No headers could be extracted from container",
            extra={"container_type": type(container).__name__},
        )
    return headers or {}

def _rate_limit_from_headers(headers: Dict[str, str]) -> RateLimitInfo:
    h = {k.lower(): v for k, v in headers.items()}
    return RateLimitInfo(
        request_id=h.get("x-request-id") or h.get("x-ms-request-id"),
        retry_after=_parse_float(h.get("retry-after")),
        region=h.get("x-ms-region") or h.get("azureml-model-deployment"),
        remaining_requests=_parse_int(h.get("x-ratelimit-remaining-requests") or h.get("ratelimit-remaining-requests")),
        remaining_tokens=_parse_int(h.get("x-ratelimit-remaining-tokens") or h.get("ratelimit-remaining-tokens")),
        reset_requests=h.get("x-ratelimit-reset-requests") or h.get("ratelimit-reset-requests"),
        reset_tokens=h.get("x-ratelimit-reset-tokens") or h.get("ratelimit-reset-tokens"),
        limit_requests=_parse_int(h.get("x-ratelimit-limit-requests") or h.get("ratelimit-limit-requests")),
        limit_tokens=_parse_int(h.get("x-ratelimit-limit-tokens") or h.get("ratelimit-limit-tokens")),
    )

def _log_rate_limit(prefix: str, info: RateLimitInfo) -> None:
    logger.info(
        "%s | req_id=%s region=%s rem_req=%s rem_tok=%s lim_req=%s lim_tok=%s reset_req=%s reset_tok=%s retry_after=%s",
        prefix,
        info.request_id,
        info.region,
        info.remaining_requests,
        info.remaining_tokens,
        info.limit_requests,
        info.limit_tokens,
        info.reset_requests,
        info.reset_tokens,
        info.retry_after,
        extra={
            "aoai_request_id": info.request_id,
            "aoai_region": info.region,
            "aoai_remaining_requests": info.remaining_requests,
            "aoai_remaining_tokens": info.remaining_tokens,
            "aoai_limit_requests": info.limit_requests,
            "aoai_limit_tokens": info.limit_tokens,
            "aoai_reset_requests": info.reset_requests,
            "aoai_reset_tokens": info.reset_tokens,
            "aoai_retry_after": info.retry_after,
        },
    )

def _set_span_rate_limit(span, info: RateLimitInfo) -> None:
    if not span:
        return
    span.set_attribute("aoai.request_id", info.request_id or "")
    span.set_attribute("aoai.region", info.region or "")
    if info.remaining_requests is not None:
        span.set_attribute("aoai.ratelimit.remaining_requests", info.remaining_requests)
    if info.remaining_tokens is not None:
        span.set_attribute("aoai.ratelimit.remaining_tokens", info.remaining_tokens)
    if info.limit_requests is not None:
        span.set_attribute("aoai.ratelimit.limit_requests", info.limit_requests)
    if info.limit_tokens is not None:
        span.set_attribute("aoai.ratelimit.limit_tokens", info.limit_tokens)
    if info.retry_after is not None:
        span.set_attribute("aoai.retry_after", info.retry_after)
    if info.reset_requests:
        span.set_attribute("aoai.reset_requests", info.reset_requests)
    if info.reset_tokens:
        span.set_attribute("aoai.reset_tokens", info.reset_tokens)

def _inspect_client_retry_settings() -> None:
    try:
        max_retries = getattr(az_openai_client, "max_retries", None)
        transport = getattr(az_openai_client, "transport", None)
        logger.info(
            "AOAI SDK retry: max_retries=%s transport=%s",
            max_retries,
            type(transport).__name__ if transport else None,
        )
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Latency helpers
# ---------------------------------------------------------------------------
class _NoOpLatency:
    def start(self, *_args, **_kwargs):
        return None
    def stop(self, *_args, **_kwargs):
        return None
    def mark(self, *_args, **_kwargs):
        return None

def _lt(ws: WebSocket):
    try:
        return getattr(ws.state, "lt", _NoOpLatency())
    except Exception:
        return _NoOpLatency()

def _log_latency_stop(name: str, dur: Any) -> None:
    try:
        if isinstance(dur, (int, float)):
            logger.info("[Latency] %s: %.3f ms", name, float(dur))
        else:
            logger.info("[Latency] %s stopped", name)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Error helpers
# ---------------------------------------------------------------------------
def _extract_status_from_exc(exc: Exception) -> Optional[int]:
    for attr in ("status", "status_code", "http_status", "statusCode"):
        try:
            v = getattr(exc, attr, None)
            if isinstance(v, int):
                return v
        except Exception:
            pass
    for attr in ("response", "http_response", "_response"):
        try:
            obj = getattr(exc, attr, None)
            if obj is None:
                continue
            v = getattr(obj, "status_code", None)
            if isinstance(v, int):
                return v
            v = getattr(obj, "status", None)
            if isinstance(v, int):
                return v
        except Exception:
            pass
    try:
        s = str(exc)
        for token in ("429", "500", "502", "503", "504", "400", "401", "403", "404"):
            if token in s:
                return int(token)
    except Exception:
        pass
    return None

def _summarize_headers(headers: Dict[str, str]) -> str:
    keys = [
        "x-request-id",
        "x-ms-request-id",
        "x-ms-region",
        "x-ratelimit-remaining-requests",
        "x-ratelimit-remaining-tokens",
        "x-ratelimit-limit-requests",
        "x-ratelimit-limit-tokens",
        "x-ratelimit-reset-requests",
        "x-ratelimit-reset-tokens",
        "retry-after",
    ]
    low = {k.lower(): v for k, v in headers.items()}
    pick = {k: low.get(k) for k in keys if low.get(k) is not None}
    return json.dumps(pick)

# ---------------------------------------------------------------------------
# Voice + sender helpers
# ---------------------------------------------------------------------------
def _get_agent_voice_config(cm: "MemoManager") -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if cm is None:
        logger.warning("MemoManager is None, using default voice configuration")
        return None, None, None
    try:
        voice_name = cm.get_value_from_corememory("current_agent_voice")
        voice_style = cm.get_value_from_corememory("current_agent_voice_style", "chat")
        voice_rate = cm.get_value_from_corememory("current_agent_voice_rate", "+3%")
        return voice_name, voice_style, voice_rate
    except Exception as exc:
        logger.warning("Failed to get agent voice config: %s", exc)
        return None, None, None

def _get_agent_sender_name(cm: "MemoManager", *, include_autoauth: bool = True) -> str:
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
    except Exception:
        return "Assistant"

# ---------------------------------------------------------------------------
# History sanitization & guards
# ---------------------------------------------------------------------------
def _strip_trailing_dangling_tool_calls(history: List[JSONDict]) -> bool:
    """Remove a final assistant(tool_calls) that lacks matching tool replies."""
    if not history:
        return False
    for i in range(len(history) - 1, -1, -1):
        msg = history[i]
        if not (isinstance(msg, dict) and msg.get("role") == "assistant" and msg.get("tool_calls")):
            continue
        tc_ids = [tc.get("id") for tc in msg.get("tool_calls", []) if isinstance(tc, dict)]
        if not tc_ids:
            del history[i]
            logger.warning("Sanitizer: removed malformed assistant tool_calls at index=%s", i)
            return True
        j = i + 1
        matched: List[str] = []
        while j < len(history) and history[j].get("role") == "tool":
            tid = history[j].get("tool_call_id")
            if tid in tc_ids:
                matched.append(tid)
            j += 1
        if set(tc_ids) - set(matched):
            del history[i]
            k = i
            while k < len(history) and history[k].get("role") == "tool":
                if history[k].get("tool_call_id") in tc_ids:
                    del history[k]
                else:
                    break
            logger.warning(
                "Sanitizer: removed dangling assistant tool_calls and associated tool replies (ids=%s)",
                tc_ids,
            )
            return True
        break
    return False

def _is_tool_call_order_error(exc: Exception) -> bool:
    s = str(exc)
    return _extract_status_from_exc(exc) == 400 and "assistant message with 'tool_calls' must be followed" in s

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
        with tracer.start_as_current_span("gpt_flow.emit_streaming_text", attributes=span_attrs):
            if is_acs:
                await send_response_to_acs(ws, text, latency_tool=_lt(ws), voice_name=voice_name, voice_style=voice_style, rate=voice_rate)
            else:
                await send_tts_audio(text, ws, latency_tool=_lt(ws), voice_name=voice_name, voice_style=voice_style, rate=voice_rate)
                speaker = _get_agent_sender_name(cm, include_autoauth=True)
                await ws.send_text(json.dumps({"type": "assistant_streaming", "content": text, "speaker": speaker}))
    else:
        if is_acs:
            await send_response_to_acs(ws, text, latency_tool=_lt(ws), voice_name=voice_name, voice_style=voice_style, rate=voice_rate)
        else:
            await send_tts_audio(text, ws, latency_tool=_lt(ws), voice_name=voice_name, voice_style=voice_style, rate=voice_rate)
            speaker = _get_agent_sender_name(cm, include_autoauth=True)
            await ws.send_text(json.dumps({"type": "assistant_streaming", "content": text, "speaker": speaker}))

async def _broadcast_dashboard(
    ws: WebSocket,
    cm: "MemoManager",
    message: str,
    *,
    include_autoauth: bool,
) -> None:
    """Compat: prefer websocket_manager snapshot, fall back to legacy clients list."""
    try:
        sender = _get_agent_sender_name(cm, include_autoauth=include_autoauth)
        websocket_manager = getattr(ws.app.state, "websocket_manager", None)
        if websocket_manager and hasattr(websocket_manager, "get_clients_snapshot"):
            clients = await websocket_manager.get_clients_snapshot()
            await broadcast_message(clients, message, sender)
        else:
                # legacy path
                await broadcast_message(ws.app.state.clients, message, sender)
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to broadcast dashboard message: %s", exc)

# ---------------------------------------------------------------------------
# Chat + streaming helpers – with explicit retry & header capture
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

@dataclass
class _ToolCall:
    id: str
    name: str
    arguments: str

@dataclass
class _ToolCallsState:
    """Aggregates all tool_calls emitted in a single streamed completion."""
    calls: List[_ToolCall]
    @property
    def started(self) -> bool:
        return bool(self.calls)

async def _openai_stream_with_retry(
    chat_kwargs: Dict[str, Any],
    *,
    model_id: str,
    dep_span,  # active OTEL span
) -> Tuple[Iterable[Any], RateLimitInfo]:
    _inspect_client_retry_settings()
    attempts = 0
    last_info = RateLimitInfo()
    aoai_host = urlparse(AZURE_OPENAI_ENDPOINT).netloc or "api.openai.azure.com"

    logger.info(
        "Starting AOAI stream request: model=%s host=%s max_attempts=%d",
        model_id,
        aoai_host,
        AOAI_RETRY_MAX_ATTEMPTS,
    )

    while True:
        attempts += 1
        try:
            with_stream_ctx = getattr(az_openai_client.chat.completions, "with_streaming_response", None)
            if callable(with_stream_ctx):
                ctx = with_stream_ctx.create(**chat_kwargs)
                with ctx as resp_ctx:
                    headers = _extract_headers(resp_ctx)
                    last_info = _rate_limit_from_headers(headers)
                    _log_rate_limit("AOAI stream started", last_info)
                    _set_span_rate_limit(dep_span, last_info)
                    dep_span.add_event("openai_stream_started", {"attempt": attempts})
                    return resp_ctx, last_info
            else:
                response_stream = az_openai_client.chat.completions.create(**chat_kwargs)
                dep_span.add_event("openai_stream_started", {"attempt": attempts})
                return response_stream, last_info

        except Exception as exc:  # noqa: BLE001
            headers = _extract_headers(exc)
            last_info = _rate_limit_from_headers(headers)
            status = _extract_status_from_exc(exc)

            logger.error(
                "AOAI stream error attempt=%s/%s status=%s req_id=%s retry_after=%s headers=%s exc=%s",
                attempts,
                AOAI_RETRY_MAX_ATTEMPTS,
                status,
                last_info.request_id,
                last_info.retry_after,
                _summarize_headers({k.lower(): v for k, v in headers.items()}),
                repr(exc),
            )

            _set_span_rate_limit(dep_span, last_info)

            # Classify retryable
            name = type(exc).__name__.lower()
            msg = str(exc).lower()
            retryable_names = (
                "ratelimit", "timeout", "apitimeout", "serviceunavailable",
                "apierror", "apistatuserror", "httpresponseerror", "httpserror",
                "badgateway", "gatewaytimeout", "too many requests", "connectionerror",
            )
            retryable = any(k in name for k in retryable_names) or any(k in msg for k in retryable_names) or any(c in msg for c in ("429", "502", "503", "504"))
            if not retryable or attempts >= AOAI_RETRY_MAX_ATTEMPTS:
                dep_span.record_exception(exc)
                dep_span.set_attribute("retry.exhausted", True)
                raise

            # Respect Retry-After, else expo backoff + jitter
            if last_info.retry_after is not None and last_info.retry_after >= 0:
                delay = float(last_info.retry_after)
            else:
                delay = min(AOAI_RETRY_BASE_DELAY_SEC * (AOAI_RETRY_BACKOFF_FACTOR ** (attempts - 1)), AOAI_RETRY_MAX_DELAY_SEC) + random.uniform(0, AOAI_RETRY_JITTER_SEC)
            dep_span.set_attribute("retry.delay_sec", delay)
            await asyncio.sleep(delay)

async def _consume_openai_stream(
    response_stream: Any,
    ws: WebSocket,
    is_acs: bool,
    cm: "MemoManager",
    call_connection_id: Optional[str],
    session_id: Optional[str],
) -> Tuple[str, _ToolCallsState]:
    """Consume stream, emit TTS chunks, and aggregate ALL tool calls."""
    collected: List[str] = []
    final_chunks: List[str] = []

    calls_map: Dict[str, _ToolCall] = {}
    order: List[str] = []

    lt = _lt(ws)
    first_seen = False
    consume_started = False

    for chunk in response_stream:
        if not first_seen:
            first_seen = True
            try:
                dur = lt.stop("aoai:ttfb")
                _log_latency_stop("aoai:ttfb", dur)
            except Exception:
                pass
            try:
                lt.start("aoai:consume")
                consume_started = True
            except Exception:
                consume_started = False

        if not getattr(chunk, "choices", None):
            continue
        delta = chunk.choices[0].delta

        # Tool-call aggregation (support multi-call)
        if getattr(delta, "tool_calls", None):
            for tc in delta.tool_calls:
                call_id = getattr(tc, "id", None) or uuid.uuid4().hex
                fn = getattr(tc, "function", None)
                name_delta = getattr(fn, "name", None) if fn else None
                args_delta = getattr(fn, "arguments", None) if fn else None
                if call_id not in calls_map:
                    calls_map[call_id] = _ToolCall(id=call_id, name=name_delta or "", arguments="")
                    order.append(call_id)
                if name_delta:
                    calls_map[call_id].name = name_delta
                if args_delta:
                    calls_map[call_id].arguments += args_delta
            continue

        # Text streaming (flush on boundaries in TTS_END)
        if getattr(delta, "content", None):
            collected.append(delta.content)
            if delta.content in TTS_END:
                streaming = add_space("".join(collected).strip())
                if streaming:
                    await _emit_streaming_text(streaming, ws, is_acs, cm, call_connection_id, session_id)
                    final_chunks.append(streaming)
                    collected.clear()

    if collected:
        pending = "".join(collected).strip()
        if pending:
            await _emit_streaming_text(pending, ws, is_acs, cm, call_connection_id, session_id)
            final_chunks.append(pending)

    if consume_started:
        try:
            dur = lt.stop("aoai:consume")
            _log_latency_stop("aoai:consume", dur)
        except Exception:
            pass

    state = _ToolCallsState(calls=[calls_map[k] for k in order])
    return "".join(final_chunks).strip(), state

# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------
async def process_gpt_response(  # noqa: PLR0913
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
    followup_depth: int = 0,
) -> Optional[Dict[str, Any]]:
    """
    Stream a chat completion, emit TTS, handle tool calls, and optionally run a
    single follow-up completion (bounded by MAX_TOOL_FOLLOWUPS).
    """
    tool_set = available_tools or DEFAULT_TOOLS

    # Build history safely (self-heal any trailing dangling tool calls)
    agent_history: List[JSONDict] = cm.get_history(agent_name)
    mutated = _strip_trailing_dangling_tool_calls(agent_history)
    if mutated:
        logger.warning("History sanitized before request (dangling tool_calls removed)")

    if user_prompt and user_prompt.strip():
        agent_history.append({"role": "user", "content": user_prompt})

    logger.info(
        "Starting GPT response: agent=%s model=%s prompt_len=%d tools=%d",
        agent_name,
        model_id,
        len(user_prompt) if user_prompt else 0,
        len(tool_set),
    )

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
        tools_available=len(tool_set),
        prompt_length=len(user_prompt) if user_prompt else 0,
    )

    with tracer.start_as_current_span("gpt_flow.process_response", attributes=span_attrs) as span:
        chat_kwargs = _build_chat_kwargs(
            history=agent_history,
            model_id=model_id,
            temperature=temperature,
            top_p=top_p,
            max_tokens=max_tokens,
            tools=tool_set,
        )
        span.set_attribute("chat.history_length", len(agent_history))

        azure_openai_attrs = create_service_dependency_attrs(
            source_service="gpt_flow",
            target_service="azure_openai",
            call_connection_id=call_connection_id,
            session_id=session_id,
            operation="stream_completion",
            model=model_id,
            stream=True,
        )
        host = urlparse(AZURE_OPENAI_ENDPOINT).netloc or "api.openai.azure.com"

        last_rate_info = RateLimitInfo()
        lt = _lt(ws)
        try:
            lt.start("aoai:total")
        except Exception:
            pass

        async def _stream_once() -> Tuple[str, _ToolCallsState]:
            nonlocal last_rate_info  # <-- keep scope explicit, declared at top of the function
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
                },
            ) as dep_span:
                try:
                    try:
                        lt.start("aoai:ttfb")
                    except Exception:
                        pass
                    response_stream, rl = await _openai_stream_with_retry(
                        chat_kwargs, model_id=model_id, dep_span=dep_span
                    )
                except Exception as e:
                    # One-time self-heal for AOAI 400 tool-call order error
                    if _is_tool_call_order_error(e):
                        logger.warning("AOAI 400 tool_call ordering error – attempting history self-heal & retry once")
                        if _strip_trailing_dangling_tool_calls(agent_history):
                            chat_kwargs["messages"] = agent_history
                            response_stream, rl = await _openai_stream_with_retry(
                                chat_kwargs, model_id=model_id, dep_span=dep_span
                            )
                        else:
                            raise
                    else:
                        raise

                last_rate_info = rl
                full_text, tools_state = await _consume_openai_stream(
                    response_stream, ws, is_acs, cm, call_connection_id, session_id
                )
                dep_span.set_attribute("tool_call_detected", tools_state.started)
                return full_text, tools_state

        try:
            full_text, tools_state = await _stream_once()
        except Exception as exc:  # noqa: BLE001
            try:
                dur = lt.stop("aoai:ttfb"); _log_latency_stop("aoai:ttfb", dur)
            except Exception:
                pass
            try:
                dur = lt.stop("aoai:consume"); _log_latency_stop("aoai:consume", dur)
            except Exception:
                pass
            try:
                dur = lt.stop("aoai:total"); _log_latency_stop("aoai:total", dur)
            except Exception:
                pass

            _log_rate_limit("AOAI final failure", last_rate_info)
            span.record_exception(exc)
            headers = _extract_headers(exc)
            info = _rate_limit_from_headers(headers)
            status = _extract_status_from_exc(exc)
            logger.error(
                "AOAI streaming failed status=%s req_id=%s headers=%s exc=%s",
                status,
                info.request_id,
                _summarize_headers({k.lower(): v for k, v in headers.items()}),
                repr(exc),
            )
            raise
        finally:
            try:
                dur = lt.stop("aoai:total"); _log_latency_stop("aoai:total", dur)
            except Exception:
                pass

        # Append assistant text (if any) & broadcast
        if full_text:
            agent_history.append({"role": "assistant", "content": full_text})
            await push_final(ws, "assistant", full_text, is_acs=is_acs)
            await _broadcast_dashboard(ws, cm, full_text, include_autoauth=False)
            span.set_attribute("response.length", len(full_text))

        # If model requested tools, bundle all tool_calls into a single assistant msg
        if tools_state.started:
            assistant_tc_msg = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {"id": c.id, "type": "function", "function": {"name": c.name, "arguments": c.arguments}}
                    for c in tools_state.calls
                ],
            }
            agent_history.append(assistant_tc_msg)
            appended_tool_ids: List[str] = []
            try:
                for c in tools_state.calls:
                    await _execute_single_tool(
                        cm=cm,
                        ws=ws,
                        agent_name=agent_name,
                        is_acs=is_acs,
                        model_id=model_id,
                        temperature=temperature,
                        top_p=top_p,
                        max_tokens=max_tokens,
                        tool_name=c.name,
                        tool_id=c.id,
                        args_json=c.arguments,
                        call_connection_id=call_connection_id,
                        session_id=session_id,
                    )
                    appended_tool_ids.append(c.id)
            except Exception as tool_exc:
                _rollback_tool_block(agent_history, assistant_tc_msg, appended_tool_ids)
                logger.error("Tool execution failed – rolled back tool_calls block; error=%s", tool_exc)
                raise

            # Optional follow-up: bounded by MAX_TOOL_FOLLOWUPS
            if followup_depth < MAX_TOOL_FOLLOWUPS:
                return await process_gpt_response(
                    cm,
                    "",  # no new user text
                    ws,
                    agent_name=agent_name,
                    is_acs=is_acs,
                    model_id=model_id,
                    temperature=temperature,
                    top_p=top_p,
                    max_tokens=max_tokens,
                    available_tools=tool_set,
                    call_connection_id=call_connection_id,
                    session_id=session_id,
                    followup_depth=followup_depth + 1,
                )
            else:
                logger.warning("Max tool followups reached: %s", MAX_TOOL_FOLLOWUPS)
                return None

        span.set_attribute("completion_type", "text_only")
        return None

# ---------------------------------------------------------------------------
# Tool execution (single) – caller manages flow
# ---------------------------------------------------------------------------
async def _execute_single_tool(  # noqa: PLR0913
    *,
    cm: "MemoManager",
    ws: WebSocket,
    agent_name: str,
    is_acs: bool,
    model_id: str,
    temperature: float,
    top_p: float,
    max_tokens: int,
    tool_name: str,
    tool_id: str,
    args_json: str,
    call_connection_id: Optional[str],
    session_id: Optional[str],
) -> Dict[str, Any]:
    logger.info("Executing tool: %s id=%s args_len=%d", tool_name, tool_id, len(args_json) if args_json else 0)

    with create_trace_context(
        name="gpt_flow.handle_tool_call",
        call_connection_id=call_connection_id,
        session_id=session_id,
        metadata={
            "tool_name": tool_name,
            "tool_id": tool_id,
            "agent_name": agent_name,
            "is_acs": is_acs,
            "args_length": len(args_json) if args_json else 0,
        },
    ) as trace_ctx:
        try:
            params: JSONDict = json.loads(args_json or "{}")
        except Exception as parse_exc:
            trace_ctx.set_attribute("error", f"Invalid tool arguments JSON: {parse_exc}")
            logger.error("Invalid tool arguments JSON for %s: %s", tool_name, parse_exc)
            raise

        fn = function_mapping.get(tool_name)
        if fn is None:
            trace_ctx.set_attribute("error", f"Unknown tool '{tool_name}'")
            logger.error("Unknown tool requested: %s (available=%s)", tool_name, list(function_mapping.keys()))
            raise ValueError(f"Unknown tool '{tool_name}'")

        trace_ctx.set_attribute("tool.parameters_count", len(params))
        call_short_id = uuid.uuid4().hex[:8]
        trace_ctx.set_attribute("tool.call_id", call_short_id)

        await push_tool_start(ws, call_short_id, tool_name, params, is_acs=is_acs)

        with create_trace_context(
            name=f"gpt_flow.execute_tool.{tool_name}",
            call_connection_id=call_connection_id,
            session_id=session_id,
            metadata={"tool_name": tool_name, "call_id": call_short_id, "parameters": params},
        ) as exec_ctx:
            t0 = time.perf_counter()
            try:
                result_raw = await fn(params)
                elapsed_ms = (time.perf_counter() - t0) * 1000
                exec_ctx.set_attribute("execution.duration_ms", elapsed_ms)
                exec_ctx.set_attribute("execution.success", True)
                result: JSONDict = json.loads(result_raw) if isinstance(result_raw, str) else result_raw
                exec_ctx.set_attribute("result.type", type(result).__name__)
            except Exception as tool_exc:
                elapsed_ms = (time.perf_counter() - t0) * 1000
                exec_ctx.set_attribute("execution.duration_ms", elapsed_ms)
                exec_ctx.set_attribute("execution.success", False)
                exec_ctx.record_exception(tool_exc)
                await push_tool_end(ws, call_short_id, tool_name, "error", elapsed_ms, result={"error": str(tool_exc)}, is_acs=is_acs)
                raise

        # Append tool role message to history
        agent_history = cm.get_history(agent_name)
        agent_history.append(
            {"tool_call_id": tool_id, "role": "tool", "name": tool_name, "content": json.dumps(result)}
        )

        await push_tool_end(ws, call_short_id, tool_name, "success", elapsed_ms, result=result, is_acs=is_acs)

        async def _persist():
            try:
                cm.persist_tool_output(tool_name, result)
                if isinstance(result, dict) and "slots" in result:
                    cm.update_slots(result["slots"])  # type: ignore[index]
            except Exception as e:
                logger.warning("Persist tool output failed: %s", e)
        asyncio.create_task(_persist())

        return result

def _rollback_tool_block(history: List[JSONDict], assistant_tc_msg: JSONDict, appended_tool_ids: List[str]) -> None:
    """Remove the just-appended assistant(tool_calls) and any tool replies for ids."""
    i = len(history) - 1
    while i >= 0 and appended_tool_ids:
        msg = history[i]
        if msg.get("role") == "tool" and msg.get("tool_call_id") in appended_tool_ids:
            del history[i]
            appended_tool_ids.remove(msg.get("tool_call_id"))
        else:
            break
        i -= 1
    if history and history[-1] is assistant_tc_msg:
        history.pop()
    else:
        for j in range(len(history) - 1, -1, -1):
            if history[j] is assistant_tc_msg:
                del history[j]
                break

# ---------------------------------------------------------------------------
# Tool follow-up helper
# ---------------------------------------------------------------------------
async def _process_tool_followup(  # noqa: PLR0913 (compat shim)
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
    await process_gpt_response(
        cm,
        "",  # No new user prompt
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
        followup_depth=1,
    )

