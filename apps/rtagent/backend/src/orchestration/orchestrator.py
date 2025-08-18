from __future__ import annotations

"""rtagent_orchestrator_refactor
=================================
Main orchestration loop for the XYMZ Insurance **ARTAgent** real‑time voice bot.

Behavior-preserving refactor with two key goals:
- **No HumanEscalation agent**. Escalation sets `escalated=True` and the
  orchestrator terminates the session for **any** agent path.
- **Configurable orchestration pattern**. A tiny API lets you declare the
  *entry agent* (always coerced to `AutoAuth`) and a list of *specialists* you
  can extend without touching routing logic.

Public entry-point remains: :func:`route_turn`.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, Iterable, Optional, Tuple, Protocol
import json
import os

from fastapi import WebSocket
from opentelemetry import trace

from apps.rtagent.backend.src.services.acs.session_terminator import (
    terminate_session,
    TerminationReason,
)
from apps.rtagent.backend.src.shared_ws import (
    broadcast_message,
    send_tts_audio,
    send_response_to_acs,
)
from src.enums.monitoring import SpanAttr  # noqa: F401 – imported for side‑effects
from utils.ml_logging import get_logger
from apps.rtagent.backend.src.utils.tracing_utils import (
    create_service_handler_attrs,
    create_service_dependency_attrs,
)

if TYPE_CHECKING:  # pragma: no cover
    from src.stateful.state_managment import MemoManager  # noqa: N812 – external naming

logger = get_logger(__name__)

# Get OpenTelemetry tracer for Application Map
tracer = trace.get_tracer(__name__)

# -------------------------------------------------------------
# Configuration + Orchestration pattern
# -------------------------------------------------------------
_ORCHESTRATOR_TRACING: bool = (
    os.getenv("ORCHESTRATOR_TRACING", "true").lower() == "true"
)
_LAST_ANNOUNCED_KEY = "last_announced_agent"
_APP_GREETS_ATTR = "greet_counts"

# Orchestration pattern (entry + specialists). Defaults preserve current flow.
_ENTRY_AGENT: str = "AutoAuth"
_SPECIALISTS: list[str] = ["General", "Claims"]


def configure_entry_and_specialists(*, entry_agent: str = "AutoAuth", specialists: Optional[Iterable[str]] = None) -> None:
    """Configure the entry agent and ordered list of specialists.

    The entry agent is **forced to 'AutoAuth'** to keep a consistent flow as requested.
    Specialists are stored (case-sensitive names) and used for generic handoffs.

    Example:
        configure_entry_and_specialists(entry_agent="AutoAuth", specialists=["General", "Claims", "Billing"]) 
    """
    global _ENTRY_AGENT, _SPECIALISTS  # noqa: PLW0603
    if entry_agent != "AutoAuth":
        logger.warning("Entry agent overridden to 'AutoAuth' (requested '%s')", entry_agent)
    _ENTRY_AGENT = "AutoAuth"
    _SPECIALISTS = list(specialists or ["General", "Claims"])


# -------------------------------------------------------------
# Registry for agent handlers (extensible)
# -------------------------------------------------------------
class AgentHandler(Protocol):
    """Protocol for agent handler functions."""
    async def __call__(
        self,
        cm: "MemoManager", 
        utterance: str, 
        ws: WebSocket,
        *,
        is_acs: bool
    ) -> None: ...

@dataclass(frozen=True)
class _AgentBinding:
    """Binding information for known agents.

    The orchestrator historically accessed concrete agent instances via
    ``ws.app.state`` attributes. We keep that behavior but centralize the
    mapping here and support a fallback dictionary ``agent_instances`` for
    future agent names.
    """

    name: str
    ws_attr: Optional[str]  # attribute name on ws.app.state, e.g. "claim_intake_agent"


# Static binding map to locate agent instances
_AGENT_BINDINGS: Dict[str, _AgentBinding] = {
    "AutoAuth": _AgentBinding(name="AutoAuth", ws_attr="auth_agent"),
    "Claims": _AgentBinding(name="Claims", ws_attr="claim_intake_agent"),
    "General": _AgentBinding(name="General", ws_attr="general_info_agent"),
}

# Registered handlers (kept public via SPECIALIST_MAP for backward compat)
_REGISTRY: Dict[str, AgentHandler] = {}


def register_specialist(name: str, handler: AgentHandler) -> None:
    """Register a specialist/entry agent handler.

    The name should match ``active_agent`` values stored in CoreMemory.
    """
    _REGISTRY[name] = handler


def register_specialists(handlers: Dict[str, AgentHandler]) -> None:
    """Bulk-register multiple agents in one call."""
    for k, v in (handlers or {}).items():
        register_specialist(k, v)


def get_specialist(name: str) -> Optional[AgentHandler]:
    """Return a handler for the given agent name, if registered."""
    return _REGISTRY.get(name)


def list_specialists() -> Iterable[str]:
    """List the registered agent names."""
    return _REGISTRY.keys()


# -------------------------------------------------------------
# Utility helpers
# -------------------------------------------------------------


def _get_correlation_context(ws: WebSocket, cm: "MemoManager") -> Tuple[str, str]:
    """Extract correlation context from *WebSocket* and *MemoManager*.

    :returns: ``(call_connection_id, session_id)``
    """
    if cm is None:
        logger.warning(
            "⚠️ MemoManager is None in _get_correlation_context, using WebSocket fallbacks"
        )
        call_connection_id = (
            getattr(ws.state, "call_connection_id", None)
            or ws.headers.get("x-call-connection-id")
            or "unknown"
        )
        session_id = (
            getattr(ws.state, "session_id", None)
            or ws.headers.get("x-session-id")
            or "unknown"
        )
        return call_connection_id, session_id

    call_connection_id = (
        getattr(ws.state, "call_connection_id", None)
        or ws.headers.get("x-call-connection-id")
        or cm.session_id
    )
    session_id = (
        getattr(ws.state, "session_id", None)
        or ws.headers.get("x-session-id")
        or cm.session_id
    )
    return call_connection_id, session_id


def _cm_get(cm: "MemoManager", key: str, default: Any = None) -> Any:
    """Shorthand for ``cm.get_value_from_corememory`` with a default."""
    if cm is None:
        logger.warning(
            "⚠️ MemoManager is None when trying to get key '%s', returning default: %s",
            key,
            default,
        )
        return default
    return cm.get_value_from_corememory(key, default)


def _cm_set(cm: "MemoManager", **kwargs: Dict[str, Any]) -> None:
    """Bulk update core‑memory with ``key=value`` pairs."""
    if cm is None:
        logger.warning("⚠️ MemoManager is None when trying to set values: %s", kwargs)
        return
    for k, v in kwargs.items():
        cm.update_corememory(k, v)


def _get_agent_instance(ws: WebSocket, agent_name: str) -> Any:
    """Return the agent instance for ``agent_name``.

    First uses known bindings (auth/claims/general). If not found, tries
    ``ws.app.state.agent_instances[agent_name]`` when present.
    """
    binding = _AGENT_BINDINGS.get(agent_name)
    if binding and binding.ws_attr:
        return getattr(ws.app.state, binding.ws_attr, None)
    # Fallback dictionary for custom agents
    instances = getattr(ws.app.state, "agent_instances", None)
    if isinstance(instances, dict):
        return instances.get(agent_name)
    return None


def _sync_voice_from_agent(cm: "MemoManager", ws: WebSocket, agent_name: str) -> None:
    """Update CoreMemory voice based on the agent instance (if available)."""
    agent = _get_agent_instance(ws, agent_name)
    voice_name = getattr(agent, "voice_name", None) if agent else None
    voice_style = getattr(agent, "voice_style", "chat") if agent else "chat"
    voice_rate = getattr(agent, "voice_rate", "+3%") if agent else "+3%"
    _cm_set(
        cm,
        current_agent_voice=voice_name,
        current_agent_voice_style=voice_style,
        current_agent_voice_rate=voice_rate,
    )


async def _maybe_terminate_if_escalated(
    cm: "MemoManager", ws: WebSocket, *, is_acs: bool
) -> bool:
    """If memory shows escalation, notify UI and terminate the session.

    Returns True if termination was triggered.
    """
    if _cm_get(cm, "escalated", False):
        # Preserve previous UI signal
        try:
            await ws.send_text(json.dumps({"type": "live_agent_transfer"}))
        except Exception:  # pragma: no cover - UI signal best-effort
            pass
        call_connection_id, _ = _get_correlation_context(ws, cm)
        await terminate_session(
            ws,
            is_acs=is_acs,
            call_connection_id=call_connection_id,
            reason=TerminationReason.HUMAN_HANDOFF,
        )
        return True
    return False


async def _send_agent_greeting(
    cm: "MemoManager", ws: WebSocket, agent_name: str, is_acs: bool
) -> None:
    """Emit a greeting when switching to *agent_name*.

    A per‑connection, per‑agent counter now lives in ``ws.state.greet_counts``
    (migrated from the previous ``app.state`` global) so that subsequent agent
    returns on the same WebSocket use the "Hi again…" variant while fully
    isolating concurrent sessions. Structure: ``{agent_name: count}``.
    """
    if cm is None:
        logger.error(
            "❌ MemoManager is None in _send_agent_greeting for agent: %s", agent_name
        )
        return

    # Prevent duplicate greeting on consecutive turns.
    if agent_name == _cm_get(cm, _LAST_ANNOUNCED_KEY):
        return

    # Resolve agent, gather voice
    agent = _get_agent_instance(ws, agent_name)
    voice_name = getattr(agent, "voice_name", None) if agent else None
    voice_style = getattr(agent, "voice_style", "chat") if agent else "chat"
    voice_rate = getattr(agent, "voice_rate", "+3%") if agent else "+3%"

    # Per‑connection counters stored on ws.state (was app.state). Backwards‑safe
    # isolation: no cross‑session race potential.
    state_counts: Dict[str, int] = getattr(ws.state, _APP_GREETS_ATTR, {})
    if not hasattr(ws.state, _APP_GREETS_ATTR):
        ws.state.__setattr__(_APP_GREETS_ATTR, state_counts)  # initialize

    actual_agent_name = getattr(agent, "name", None) or agent_name
    counter = state_counts.get(actual_agent_name, 0)
    state_counts[actual_agent_name] = counter + 1

    caller_name = _cm_get(cm, "caller_name")
    topic = _cm_get(cm, "topic") or _cm_get(cm, "claim_intent") or "your policy"

    if counter == 0:
        greeting = (
            f"Hi {caller_name}, this is the {agent_name} specialist agent. "
            f"I understand you're calling about {topic}. How can I help you further?"
        )
    else:
        greeting = (
            f"Hi again {caller_name}, {agent_name} specialist back on the line. "
            f"Let's continue with {topic}."
        )

    cm.append_to_history(actual_agent_name, "assistant", greeting)
    _cm_set(cm, **{_LAST_ANNOUNCED_KEY: agent_name})

    if is_acs:
        logger.info(
            "🎤 ACS greeting #%s for %s (voice: %s): %s",
            counter + 1,
            agent_name,
            voice_name or "default",
            greeting,
        )
        if agent_name == "Claims":
            agent_sender = "Claims Specialist"
        elif agent_name == "General":
            agent_sender = "General Info"
        else:
            agent_sender = "Assistant"
        clients = await ws.app.state.websocket_manager.get_clients_snapshot()
        await broadcast_message(clients, greeting, agent_sender)
        try:
            # Use send_response_to_acs for proper ACS audio playback
            await send_response_to_acs(
                ws=ws,
                text=greeting,
                blocking=False,
                latency_tool=ws.state.lt,
                voice_name=voice_name,
                voice_style=voice_style,
                rate=voice_rate,
            )
        except Exception as e:
            logger.error(f"Failed to send ACS greeting audio: {e}")
            logger.warning("ACS greeting sent as text only.")
    else:
        logger.info(
            "💬 WS greeting #%s for %s (voice: %s)",
            counter + 1,
            agent_name,
            voice_name or "default",
        )
        await ws.send_text(json.dumps({"type": "status", "message": greeting}))
        await send_tts_audio(
            greeting,
            ws,
            latency_tool=ws.state.lt,
            voice_name=voice_name,
            voice_style=voice_style,
            rate=voice_rate,
        )


# -------------------------------------------------------------
# 1. Authentication agent
# -------------------------------------------------------------
async def run_auth_agent(
    cm: "MemoManager",
    utterance: str,
    ws: WebSocket,
    *,
    is_acs: bool,
) -> None:
    """
    Run *AuthAgent* once per session.

    • On **emergency escalation**, set `escalated=True`, store reason, and return.
    • On **successful auth**, cache caller info & chosen specialist.
    """
    if cm is None:
        logger.error("❌ MemoManager is None in run_auth_agent")
        raise ValueError("MemoManager (cm) parameter cannot be None in run_auth_agent")

    auth_agent = _get_agent_instance(ws, "AutoAuth")

    # Minimal Latency Suite V2: Agent processing is already tracked by the orchestrator
    # No need for additional legacy tracking here
    result: Dict[str, Any] | Any = await auth_agent.respond(  # type: ignore[union-attr]
        cm, utterance, ws, is_acs=is_acs
    )
    logger.info("🚨 Auth result type: %s, value: %s", type(result).__name__, result)

    if isinstance(result, dict) and result.get("handoff") == "human_agent":
        logger.info("🔀 Processing human_agent handoff…")
        reason = result.get("reason") or result.get("escalation_reason")
        _cm_set(cm, escalated=True, escalation_reason=reason)
        logger.warning("🚨 Escalation during auth – session=%s reason=%s", cm.session_id, reason)
        return  # termination handled by orchestrator

    logger.info(
        "🔍 Processing auth result - type: %s, is_dict: %s",
        type(result).__name__,
        isinstance(result, dict),
    )

    if isinstance(result, dict) and result.get("authenticated"):
        caller_name: str | None = result.get("caller_name")
        policy_id: str | None = result.get("policy_id")
        claim_intent: str | None = result.get("claim_intent")
        topic: str | None = result.get("topic")
        intent: str = result.get("intent", "general")
        active_agent: str = "Claims" if intent == "claims" else "General"

        _cm_set(
            cm,
            authenticated=True,
            caller_name=caller_name,
            policy_id=policy_id,
            claim_intent=claim_intent,
            topic=topic,
            active_agent=active_agent,
        )

        logger.info(
            "✅ Auth OK – session=%s caller=%s policy=%s → %s agent",
            cm.session_id,
            caller_name,
            policy_id,
            active_agent,
        )

        # Store voice configuration for the active agent in memory
        _sync_voice_from_agent(cm, ws, active_agent)

        # Send greeting with the correct agent voice
        await _send_agent_greeting(cm, ws, active_agent, is_acs)


# -------------------------------------------------------------
# 2. Specialist agents (shared base + thin wrappers)
# -------------------------------------------------------------


async def _run_specialist_base(
    *,
    agent_key: str,
    cm: "MemoManager",
    utterance: str,
    ws: WebSocket,
    is_acs: bool,
    context_message: str,
    respond_kwargs: Dict[str, Any],
    latency_label: str,
) -> None:
    """Shared runner for specialist agents (behavior-preserving).

    Thin wrapper used by concrete functions to avoid duplication.
    """
    agent = _get_agent_instance(ws, agent_key)

    # Context injection for agent awareness (preserve current content)
    cm.append_to_history(getattr(agent, "name", agent_key), "assistant", context_message)

    # Use minimal tracking through cm.track() interface
    async with cm.track(latency_label):
        resp = await agent.respond(  # type: ignore[union-attr]
            cm,
            utterance,
            ws,
            is_acs=is_acs,
            **respond_kwargs,
        )

    await _process_tool_response(cm, resp, ws, is_acs)


async def run_general_agent(
    cm: "MemoManager",
    utterance: str,
    ws: WebSocket,
    *,
    is_acs: bool,
) -> None:
    """Handle a turn with the *GeneralInfoAgent*."""
    if cm is None:
        logger.error("❌ MemoManager is None in run_general_agent")
        raise ValueError("MemoManager (cm) parameter cannot be None in run_general_agent")

    caller_name = _cm_get(cm, "caller_name")
    topic = _cm_get(cm, "topic")
    policy_id = _cm_get(cm, "policy_id")

    context_msg = f"Authenticated caller: {caller_name} (Policy: {policy_id}) | Topic: {topic}"
    await _run_specialist_base(
        agent_key="General",
        cm=cm,
        utterance=utterance,
        ws=ws,
        is_acs=is_acs,
        context_message=context_msg,
        respond_kwargs={"caller_name": caller_name, "topic": topic, "policy_id": policy_id},
        latency_label="general_agent",
    )


async def run_claims_agent(
    cm: "MemoManager",
    utterance: str,
    ws: WebSocket,
    *,
    is_acs: bool,
) -> None:
    """Handle a turn with the *ClaimIntakeAgent*."""
    if cm is None:
        logger.error("❌ MemoManager is None in run_claims_agent")
        raise ValueError("MemoManager (cm) parameter cannot be None in run_claims_agent")

    caller_name = _cm_get(cm, "caller_name")
    claim_intent = _cm_get(cm, "claim_intent")
    policy_id = _cm_get(cm, "policy_id")

    context_msg = f"Authenticated caller: {caller_name} (Policy: {policy_id}) | Claim Intent: {claim_intent}"
    await _run_specialist_base(
        agent_key="Claims",
        cm=cm,
        utterance=utterance,
        ws=ws,
        is_acs=is_acs,
        context_message=context_msg,
        respond_kwargs={"caller_name": caller_name, "claim_intent": claim_intent, "policy_id": policy_id},
        latency_label="claim_agent",
    )


# -------------------------------------------------------------
# 3. Structured tool‑response post‑processing
# -------------------------------------------------------------


def _get_field(resp: Dict[str, Any], key: str) -> Any:  # noqa: D401 – simple util
    """Return ``resp[key]`` or ``resp['data'][key]`` if nested."""
    if key in resp:
        return resp[key]
    return resp.get("data", {}).get(key) if isinstance(resp.get("data"), dict) else None


async def _process_tool_response(  # pylint: disable=too-complex
    cm: "MemoManager", resp: Any, ws: WebSocket, is_acs: bool
) -> None:
    """Inspect structured tool outputs and update core‑memory accordingly."""
    if cm is None:
        logger.error("❌ MemoManager is None in _process_tool_response")
        return

    if not isinstance(resp, dict):
        return

    prev_agent: str | None = _cm_get(cm, "active_agent")

    handoff_type = _get_field(resp, "handoff")
    target_agent = _get_field(resp, "target_agent")

    # FNOL‑specific outputs
    claim_success = resp.get("claim_success")

    # Primary call‑reason updates (may come from auth agent or later)
    topic = _get_field(resp, "topic")
    claim_intent = _get_field(resp, "claim_intent")
    intent = _get_field(resp, "intent")

    # ─── Unified intent routing (post‑auth) ─────────────
    if intent in {"claims", "general"} and _cm_get(cm, "authenticated", False):
        new_agent: str = "Claims" if intent == "claims" else "General"
        _cm_set(cm, active_agent=new_agent, claim_intent=claim_intent, topic=topic)
        _sync_voice_from_agent(cm, ws, new_agent)
        if new_agent != prev_agent:
            logger.info("🔀 Routed via intent → %s", new_agent)
            await _send_agent_greeting(cm, ws, new_agent, is_acs)
        return  # Skip legacy hand‑off logic if present

    # ─── hand‑off (non‑auth transfers) ──────
    if handoff_type == "ai_agent" and target_agent:
        # Prefer explicit target if it is registered or in configured specialists
        if target_agent in _REGISTRY or target_agent in _SPECIALISTS:
            new_agent = target_agent
        elif "Claim" in target_agent:
            new_agent = "Claims"
        else:
            new_agent = "General"

        if new_agent == "Claims":
            _cm_set(cm, active_agent=new_agent, claim_intent=claim_intent)
        else:
            _cm_set(cm, active_agent=new_agent, topic=topic)

        _sync_voice_from_agent(cm, ws, new_agent)
        logger.info("🔀 Hand‑off → %s", new_agent)
        if new_agent != prev_agent:
            await _send_agent_greeting(cm, ws, new_agent, is_acs)

    elif handoff_type == "human_agent":
        # No HumanEscalation agent; set escalation flag and let orchestrator terminate.
        reason = _get_field(resp, "reason") or _get_field(resp, "escalation_reason")
        _cm_set(cm, escalated=True, escalation_reason=reason)

    # ─── 3. Claim intake completed ─────────────────────────────
    elif claim_success:
        _cm_set(cm, intake_completed=True, latest_claim_id=resp["claim_id"])  # type: ignore[index]


# Mapping from *active_agent* value ➜ handler coroutine.
# Kept for backward compatibility; now just a view over _REGISTRY.
SPECIALIST_MAP: Dict[str, AgentHandler] = _REGISTRY

# -------------------------------------------------------------
# 4. Public entry‑point (per user turn)
# -------------------------------------------------------------
async def route_turn(
    cm: "MemoManager",
    transcript: str,
    ws: WebSocket,
    *,
    is_acs: bool,
) -> None:
    """Handle **one** user turn plus any immediate follow‑ups.

    Responsibilities:
    * Broadcast the user message to supervisor dashboards.
    * Run the authentication agent until success.
    * Delegate to the correct specialist agent.
    * Detect when a live human transfer is required.
    * Persist conversation state to Redis for resilience.
    """
    if cm is None:
        logger.error("❌ MemoManager (cm) is None - cannot process orchestration")
        raise ValueError("MemoManager (cm) parameter cannot be None")

    # Minimal Latency Suite V2: Start orchestrator turn tracking
    orchestrator_turn_id = None
    route_turn_context = None
    
    try:
        if hasattr(cm, 'track'):
            # Track the overall route_turn stage using minimal tracker
            route_turn_context = cm.track("route_turn")
            if route_turn_context is not None:
                await route_turn_context.__aenter__()
                logger.info("🚀 Started minimal latency tracking for route_turn")
            else:
                logger.warning("cm.track returned None - latency tracking disabled")
    except Exception as e:
        logger.warning(f"Minimal tracking initialization failed in orchestrator: {e}")
        route_turn_context = None

    # Extract correlation context - defensive programming  
    try:
        correlation_result = _get_correlation_context(ws, cm)
        if correlation_result is None or not isinstance(correlation_result, tuple) or len(correlation_result) != 2:
            logger.error("❌ _get_correlation_context returned invalid result: %s", correlation_result)
            call_connection_id, session_id = "unknown", "unknown"
        else:
            call_connection_id, session_id = correlation_result
    except Exception as e:
        logger.error("❌ Error extracting correlation context: %s", e)
        call_connection_id, session_id = "unknown", "unknown"

    # Initialize session with configured entry agent if no active_agent is set
    if not _cm_get(cm, "authenticated", False) and _cm_get(cm, "active_agent") != _ENTRY_AGENT:
        _cm_set(cm, active_agent=_ENTRY_AGENT)

    # Create handler span for orchestrator service
    span_attrs = create_service_handler_attrs(
        service_name="orchestrator",
        call_connection_id=call_connection_id,
        session_id=session_id,
        operation="route_turn",
        transcript_length=len(transcript),
        is_acs=is_acs,
        authenticated=_cm_get(cm, "authenticated", False),
        active_agent=_cm_get(cm, "active_agent", "none"),
    )

    with tracer.start_as_current_span(
        "orchestrator.route_turn", attributes=span_attrs
    ) as span:
        redis_mgr = ws.app.state.redis

        # 0) Broadcast raw user transcript to dashboards.
        try:
            clients = await ws.app.state.websocket_manager.get_clients_snapshot()
            await broadcast_message(clients, transcript, "User")
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.error("Broadcast failure: %s", exc)

        try:
            # 1) Unified escalation check (for *any* agent)
            if await _maybe_terminate_if_escalated(cm, ws, is_acs=is_acs):
                return

            # 2) Dispatch to agent (AutoAuth or specialists; registry-backed)
            active: str = _cm_get(cm, "active_agent") or _ENTRY_AGENT
            span.set_attribute("orchestrator.stage", "specialist_dispatch")
            span.set_attribute("orchestrator.target_agent", active)

            handler = get_specialist(active)
            if handler is None:
                logger.warning("Unknown active_agent=%s session=%s", active, cm.session_id)
                span.set_attribute("orchestrator.error", "unknown_agent")
                return

            # Minimal Latency Suite V2: Track agent execution
            agent_context = None
            try:
                if route_turn_context:
                    agent_context = cm.track(f"agent_{active.lower()}")
                    if agent_context is not None:
                        await agent_context.__aenter__()
                        logger.info(f"🤖 Minimal tracking started for agent: {active}")
                    else:
                        logger.warning(f"cm.track returned None for agent {active} - agent tracking disabled")
                        agent_context = None
            except Exception as e:
                logger.warning(f"Minimal agent tracking initialization failed: {e}")

            agent_attrs = create_service_dependency_attrs(
                source_service="orchestrator",
                target_service=active.lower() + "_agent",
                call_connection_id=call_connection_id,
                session_id=session_id,
                operation="process_turn",
                transcript_length=len(transcript),
            )

            with tracer.start_as_current_span(
                f"orchestrator.call_{active.lower()}_agent", attributes=agent_attrs
            ):
                await handler(cm, transcript, ws, is_acs=is_acs)

                # Minimal Latency Suite V2: End agent execution
                if agent_context:
                    try:
                        await agent_context.__aexit__(None, None, None)
                        logger.info(f"✅ Minimal agent tracking completed for: {active}")
                    except Exception as e:
                        logger.warning(f"Minimal agent tracking cleanup failed: {e}")

                # 3) After any agent runs, if escalation flag was set during the turn, terminate.
                if await _maybe_terminate_if_escalated(cm, ws, is_acs=is_acs):
                    return

        except Exception:  # pylint: disable=broad-exception-caught
            # Minimal Latency Suite V2: Cleanup on exception
            if 'agent_context' in locals() and agent_context:
                try:
                    await agent_context.__aexit__(Exception, None, None)
                except Exception:
                    pass
            
            logger.exception("💥 route_turn crash – session=%s", cm.session_id)
            span.set_attribute("orchestrator.error", "exception")
            raise
        finally:
            # Minimal Latency Suite V2: Complete route_turn stage
            if route_turn_context:
                try:
                    await route_turn_context.__aexit__(None, None, None)
                    logger.info("✅ Minimal route_turn tracking completed")
                except Exception as e:
                    logger.warning(f"Minimal route_turn tracking cleanup failed: {e}")
            
            # Ensure core‑memory is persisted even if a downstream component failed.
            await cm.persist_to_redis_async(redis_mgr)


# ---------------------------------------------------------------------------
# Default registrations (keeps current behavior)
# ---------------------------------------------------------------------------
def _bind_default_handlers() -> None:
    """Register default agent handlers."""
    register_specialist("AutoAuth", run_auth_agent)
    register_specialist("General", run_general_agent)
    register_specialist("Claims", run_claims_agent)


# Bind defaults immediately
_bind_default_handlers()