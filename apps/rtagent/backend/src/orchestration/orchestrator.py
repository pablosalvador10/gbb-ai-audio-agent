from __future__ import annotations

"""rtagent_orchestrator
=================================
Main orchestration loop for the XYMZ Insurance **RTAgent** real‑time voice bot.

The routing logic: the *authentication* tool determines the
``intent`` ("claims" | "general") and optional ``claim_intent``.  Once the
caller is authenticated the orchestrator dispatches to the corresponding
specialist agent.  Specialists can still trigger hand‑offs via
``handoff: ai_agent`` or ``handoff: human_agent``.
"""

from contextlib import asynccontextmanager
from apps.rtagent.backend.src.services.acs.session_terminator import terminate_session, TerminationReason
from typing import TYPE_CHECKING, Any, Callable, Dict, Tuple
import json
import os

from fastapi import WebSocket

from apps.rtagent.backend.src.shared_ws import (
    broadcast_message,
    send_tts_audio,
)
from src.enums.monitoring import SpanAttr  # noqa: F401 – imported for side‑effects
from utils.ml_logging import get_logger
from utils.trace_context import create_trace_context  # noqa: F401 – may be used elsewhere

if TYPE_CHECKING:  # pragma: no cover
    from src.stateful.state_managment import MemoManager  # noqa: N812 – external naming

logger = get_logger(__name__)

# -------------------------------------------------------------
# Configuration
# -------------------------------------------------------------
_ORCHESTRATOR_TRACING: bool = (
    os.getenv("ORCHESTRATOR_TRACING", "true").lower() == "true"
)
_LAST_ANNOUNCED_KEY = "last_announced_agent"
_APP_GREETS_ATTR = "greet_counts" 

# -------------------------------------------------------------
# Utility helpers
# -------------------------------------------------------------

def _get_correlation_context(ws: WebSocket, cm: "MemoManager") -> Tuple[str, str]:
    """Extract correlation context from *WebSocket* and *MemoManager*.

    :returns: ``(call_connection_id, session_id)``
    """
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
    return cm.get_value_from_corememory(key, default)


def _cm_set(cm: "MemoManager", **kwargs: Dict[str, Any]) -> None:
    """Bulk update core‑memory with ``key=value`` pairs."""
    for k, v in kwargs.items():
        cm.update_corememory(k, v)


async def _send_agent_greeting(
    cm: "MemoManager", ws: WebSocket, agent_name: str, is_acs: bool
) -> None:
    """Emit a greeting when switching to *agent_name*.

    A per‑session, per‑agent counter lives in ``ws.app.state.greet_counts`` so
    that subsequent returns use "Hi again…".
    """

    # Prevent duplicate greeting on consecutive turns.
    if agent_name == _cm_get(cm, _LAST_ANNOUNCED_KEY):
        return

    # ------------------------------------------------------------------
    # Fetch / update counters stored in app‑state.
    # Structure: {session_id: {agent_name: count}}
    # ------------------------------------------------------------------
    app_counts: Dict[str, Dict[str, int]] = getattr(ws.app.state, _APP_GREETS_ATTR, {})
    if not hasattr(ws.app.state, _APP_GREETS_ATTR):
        ws.app.state.__setattr__(_APP_GREETS_ATTR, app_counts)  # first run

    session_counts = app_counts.get(cm.session_id, {})
    counter = session_counts.get(agent_name, 0)
    session_counts[agent_name] = counter + 1
    app_counts[cm.session_id] = session_counts

    # ------------------------------------------------------------------
    # Compose greeting based on counter.
    # ------------------------------------------------------------------
    caller_name = _cm_get(cm, "caller_name")
    #TODO: Fix logic more dynamic
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

    cm.append_to_history(agent_name, "assistant", greeting)
    _cm_set(cm, **{_LAST_ANNOUNCED_KEY: agent_name})

    # ------------------------------------------------------------------
    # Deliver greeting via correct channel.
    # ------------------------------------------------------------------
    if is_acs:
        logger.info("🎤 ACS greeting #%s for %s: %s", counter + 1, agent_name, greeting)
        await broadcast_message(ws.app.state.clients, greeting, "Assistant")
        try:
             ws.app.state.handler.play_greeting(greeting_text=greeting)  # type: ignore[attr-defined]
        except AttributeError:
            logger.warning("Media handler lacks play_greeting(); sent text only.")
    else:
        logger.info("💬 WS greeting #%s for %s", counter + 1, agent_name)
        await ws.send_text(json.dumps({"type": "status", "message": greeting}))
        await send_tts_audio(greeting, ws, latency_tool=ws.state.lt)


@asynccontextmanager
async def track_latency(timer, label: str, redis_mgr):
    """Context‑manager that starts/stops a latency timer and stores the metric."""
    timer.start(label)
    try:
        yield
    finally:
        timer.stop(label, redis_mgr)


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
    auth_agent = ws.app.state.auth_agent

    async with track_latency(ws.state.lt, "auth_agent", ws.app.state.redis):
        result: Dict[str, Any] | Any = await auth_agent.respond(
            cm, utterance, ws, is_acs=is_acs
        )
        logger.info("🚨 Auth result: %s", result)

    if isinstance(result, dict) and result.get("handoff") == "human_agent":
        reason = result.get("reason") or result.get("escalation_reason")
        _cm_set(
            cm,
            escalated=True,
            escalation_reason=reason,
            active_agent="HumanEscalation",
        )
        logger.warning(
            "🚨 Escalation during auth – session=%s reason=%s", cm.session_id, reason
        )
        return  # session termination handled upstream
    
    if result is not None: 
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

# -------------------------------------------------------------
# 2.  Specialist agents
# -------------------------------------------------------------
async def run_general_agent(
    cm: "MemoManager",
    utterance: str,
    ws: WebSocket,
    *,
    is_acs: bool,
) -> None:
    """Handle a turn with the *GeneralInfoAgent*."""

    agent = ws.app.state.general_info_agent
    caller_name = _cm_get(cm, "caller_name")
    topic = _cm_get(cm, "topic")
    policy_id = _cm_get(cm, "policy_id")

    # Context injection for agent awareness
    # TODO: improve logic
    cm.append_to_history(
        agent.name, 
        "assistant", 
        f"Authenticated caller: {caller_name} (Policy: {policy_id}) | Topic: {topic}"
    )

    async with track_latency(ws.state.lt, "general_agent", ws.app.state.redis):
        resp = await agent.respond(
            cm,
            utterance,
            ws,
            is_acs=is_acs,
            caller_name=caller_name,
            topic=topic,
            policy_id=policy_id,
        )

    await _process_tool_response(cm, resp, ws, is_acs)


async def run_claims_agent(
    cm: "MemoManager",
    utterance: str,
    ws: WebSocket,
    *,
    is_acs: bool,
) -> None:
    """Handle a turn with the *ClaimIntakeAgent*."""

    agent = ws.app.state.claim_intake_agent
    caller_name = _cm_get(cm, "caller_name")
    claim_intent = _cm_get(cm, "claim_intent")
    policy_id = _cm_get(cm, "policy_id")

    # Context injection for agent awareness
    # TODO: improve logic
    cm.append_to_history(
        agent.name, 
        "assistant", 
        f"Authenticated caller: {caller_name} (Policy: {policy_id}) | Claim Intent: {claim_intent}"
    )
    async with track_latency(ws.state.lt, "claim_agent", ws.app.state.redis):
        resp = await agent.respond(
            cm,
            utterance,
            ws,
            is_acs=is_acs,
            caller_name=caller_name,
            claim_intent=claim_intent,
            policy_id=policy_id,
        )

    await _process_tool_response(cm, resp, ws, is_acs)


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

        if new_agent != prev_agent:
            logger.info("🔀 Routed via intent → %s", new_agent)
            await _send_agent_greeting(cm, ws, new_agent, is_acs)
        return  # Skip legacy hand‑off logic if present

    # ─── hand‑off (non‑auth transfers) ──────
    if handoff_type == "ai_agent" and target_agent:
        if "Claim" in target_agent:
            new_agent = "Claims"
            _cm_set(cm, active_agent=new_agent, claim_intent=claim_intent)
        else:
            new_agent = "General"
            _cm_set(cm, active_agent=new_agent, topic=topic)
        logger.info("🔀 Hand‑off → %s", new_agent)
        if new_agent != prev_agent:
            await _send_agent_greeting(cm, ws, new_agent, is_acs)

    elif handoff_type == "human_agent":
        _cm_set(cm, active_agent="HumanEscalation")

    # ─── 3. Claim intake completed ─────────────────────────────
    elif claim_success:
        _cm_set(cm, intake_completed=True, latest_claim_id=resp["claim_id"])


# Mapping from *active_agent* value ➜ handler coroutine.
SPECIALIST_MAP: Dict[str, Callable[..., Any]] = {
    "General": run_general_agent,
    "Claims": run_claims_agent,
}


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

    This is the single public function invoked by the WebSocket layer after a
    new chunk of user speech has been transcribed to text.  The orchestrator is
    responsible for:

    * Broadcasting the user message to supervisor dashboards.
    * Running the authentication agent until success.
    * Delegating to the correct specialist agent.
    * Detecting when a live human transfer is required.
    * Persisting conversation state to Redis for resilience.
    """

    redis_mgr = ws.app.state.redis

    # ------------------------------------------------------------------
    # Send the raw user transcript to connected dashboards.
    # ------------------------------------------------------------------
    try:
        await broadcast_message(ws.app.state.clients, transcript, "User")
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("Broadcast failure: %s", exc)

    try:
        # ------------------------------------------------------------------
        # 1) Authentication (single‑shot per session)
        # ------------------------------------------------------------------
        if not _cm_get(cm, "authenticated", False):
            await run_auth_agent(cm, transcript, ws, is_acs=is_acs)
            if _cm_get(cm, "escalated", False):
                call_connection_id, _ = _get_correlation_context(ws, cm)
                await terminate_session(
                    ws,
                    is_acs=is_acs,
                    call_connection_id=call_connection_id,
                    reason=TerminationReason.HUMAN_HANDOFF,
                )
                return
            return

        # ------------------------------------------------------------------
        # 2) Human escalation short‑circuit
        # ------------------------------------------------------------------
        if _cm_get(cm, "active_agent") == "HumanEscalation":
            await ws.send_text(json.dumps({"type": "live_agent_transfer"}))
            return

        # ------------------------------------------------------------------
        # 3) Dispatch to specialist agent
        # ------------------------------------------------------------------
        active: str = _cm_get(cm, "active_agent") or "General"
        handler = SPECIALIST_MAP.get(active)
        if handler is None:
            logger.warning("Unknown active_agent=%s session=%s", active, cm.session_id)
            return

        await handler(cm, transcript, ws, is_acs=is_acs)

    except Exception:  # pylint: disable=broad-exception-caught
        logger.exception("💥 route_turn crash – session=%s", cm.session_id)
        raise
    finally:
        # Ensure core‑memory is persisted even if a downstream component failed.
        await cm.persist_to_redis_async(redis_mgr)
