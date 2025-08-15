"""
Shared tracing utilities for OpenTelemetry instrumentation.

Provides common helper functions for span attributes and structured logging
without overriding OpenTelemetry Resource settings. These helpers avoid
encoding `service.name` or `span.kind` as span attributes—set `service.name`
via the TracerProvider Resource and pass `kind=SpanKind.*` when starting spans.

The helpers also prefer semantic attributes for edges (e.g., `peer.service`,
`net.peer.name`, `network.protocol.name`).
"""

import os
from typing import Any, Dict, Optional

from utils.ml_logging import get_logger

# Default logger for fallback usage
_default_logger = get_logger("tracing_utils")

# Performance optimization: Cache tracing configuration
TRACING_ENABLED = os.getenv("ENABLE_TRACING", "false").lower() == "true"

# Logical service names used in logs/attributes (Resource `service.name` should
# be configured at process startup; these are for human-friendly values only.)
SERVICE_NAMES = {
    "acs_router": "acs-router",
    "acs_media_ws": "acs-websocket",
    "acs_media_handler": "acs-media-handler",
    "orchestrator": "orchestration-engine",
    "general_info_agent": "general-info-service",
    "claim_intake_agent": "claims-service",
    "gpt_flow": "gpt-completion-service",
    "azure_openai": "azure-openai-service",
    # Legacy aliases
    "websocket": "acs-websocket",
    "media_handler": "acs-media-handler",
    "orchestration": "orchestration-engine",
    "auth_agent": "auth-service",
    "general_agent": "general-info-service",
    "claims_agent": "claims-service",
}


def create_span_attrs(
    component: str = "unknown",
    service: str = "unknown",
    call_connection_id: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:
    """Create generic span attributes with common fields.

    NOTE: We intentionally DO NOT set `service.name` or `span.kind` here.
    Set `service.name` on the Resource, and pass `kind=SpanKind.*` when
    creating spans.
    """
    attrs = {
        "component": component,
        "service": service,
        "service.version": "1.0.0",
    }
    if call_connection_id:
        attrs["rt.call.connection_id"] = call_connection_id
    attrs.update({k: v for k, v in kwargs.items() if v is not None})
    return attrs


def create_service_dependency_attrs(
    source_service: str,
    target_service: str,
    call_connection_id: Optional[str] = None,
    session_id: Optional[str] = None,
    *,
    ws: bool | None = None,
    **kwargs,
) -> Dict[str, Any]:
    """Create attributes for CLIENT spans that represent dependencies.

    Uses semantic keys to help App Map draw edges correctly.
    - peer.service: logical target
    - net.peer.name: target name/host (same logical value if not DNS based)
    - network.protocol.name: "websocket" for WS edges when ws=True
    """
    target_name = SERVICE_NAMES.get(target_service, target_service)

    attrs: Dict[str, Any] = {
        "component": source_service,
        "peer.service": target_name,
        "net.peer.name": target_name,
    }
    if ws:
        attrs["network.protocol.name"] = "websocket"
    if call_connection_id is not None:
        attrs["rt.call.connection_id"] = call_connection_id
    if session_id is not None:
        attrs["rt.session.id"] = session_id

    # Merge any extra attributes
    attrs.update({k: v for k, v in kwargs.items() if v is not None})
    return attrs


def create_service_handler_attrs(
    service_name: str,
    call_connection_id: Optional[str] = None,
    session_id: Optional[str] = None,
    **kwargs,
) -> Dict[str, Any]:
    """Create attributes for SERVER spans that represent handlers.

    These identify the component and include stable correlation keys.
    """
    attrs: Dict[str, Any] = {
        "component": service_name,
    }
    if call_connection_id:
        attrs["rt.call.connection_id"] = call_connection_id
    if session_id:
        attrs["rt.session.id"] = session_id

    attrs.update({k: v for k, v in kwargs.items() if v is not None})
    return attrs


def log_with_context(
    logger,
    level: str,
    message: str,
    operation: Optional[str] = None,
    **kwargs,
) -> None:
    """Structured logging with consistent context.

    Filters None values to keep logs clean.
    """
    extra = {"operation_name": operation}
    extra.update({k: v for k, v in kwargs.items() if v is not None})

    try:
        getattr(logger, level)(message, extra=extra)
    except AttributeError:
        _default_logger.warning(
            f"Invalid log level '{level}' for message: {message}", extra=extra
        )
