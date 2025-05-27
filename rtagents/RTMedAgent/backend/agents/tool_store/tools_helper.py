"""
tools_helper.py

Single source of truth for
 • callable-name → python-function mapping
 • JSON frames that announce tool_start / tool_progress / tool_end
"""

from __future__ import annotations
import asyncio, json, time, uuid
from typing import Any, Callable, Dict
from fastapi import WebSocket

from utils.ml_logging import get_logger
from rtagents.RTMedAgent.backend.agents.tool_store.functions import (
    authenticate_user,
    check_drug_interactions,
    escalate_emergency,
    evaluate_prior_authorization,
    fill_new_prescription,
    get_current_prescriptions,
    lookup_medication_info,
    lookup_side_effects,
    refill_prescription,
    schedule_appointment,
)

log = get_logger("tools_helper")

# --------------------------------------------------------------------------- #
#  public mapping {openai_tool_name: python_callable}
# --------------------------------------------------------------------------- #
function_mapping: Dict[str, Callable[..., Any]] = {
    "schedule_appointment": schedule_appointment,
    "refill_prescription": refill_prescription,
    "lookup_medication_info": lookup_medication_info,
    "evaluate_prior_authorization": evaluate_prior_authorization,
    "escalate_emergency": escalate_emergency,
    "authenticate_user": authenticate_user,
    "fill_new_prescription": fill_new_prescription,
    "lookup_side_effects": lookup_side_effects,
    "get_current_prescriptions": get_current_prescriptions,
    "check_drug_interactions": check_drug_interactions,
}


async def _emit(ws: WebSocket, payload: dict, *, is_acs: bool) -> None:
    """
    • browser `/realtime`  → send JSON directly on that socket
    • phone   `/call/*`    → fan-out to every dashboard on `/relay`

    IMPORTANT: we forward the *raw* JSON (no additional wrapper) so that the
               front-end can treat both transports identically.
    """
    frame = json.dumps(payload)

    if is_acs:
        # never block STT/TTS – fire-and-forget
        for cli in set(ws.app.state.clients):
            asyncio.create_task(cli.send_text(frame))
    else:
        await ws.send_text(frame)


def _frame(
    _type: str,
    call_id: str,
    name: str,
    **extra: Any,
) -> dict:
    return {
        "type": _type,
        "callId": call_id,
        "tool": name,
        "ts": time.time(),
        **extra,
    }


async def push_tool_start(
    ws: WebSocket,
    call_id: str,
    name: str,
    args: dict,
    *,
    is_acs: bool = False,
) -> None:
    await _emit(ws, _frame("tool_start", call_id, name, args=args), is_acs=is_acs)


async def push_tool_progress(
    ws: WebSocket,
    call_id: str,
    name: str,
    pct: int,
    note: str | None = None,
    *,
    is_acs: bool = False,
) -> None:
    await _emit(
        ws, _frame("tool_progress", call_id, name, pct=pct, note=note), is_acs=is_acs
    )


async def push_tool_end(
    ws: WebSocket,
    call_id: str,
    name: str,
    status: str,  # "success" | "error"
    elapsed_ms: float,
    *,
    result: dict | None = None,
    error: str | None = None,
    is_acs: bool = False,
) -> None:
    await _emit(
        ws,
        _frame(
            "tool_end",
            call_id,
            name,
            status=status,
            elapsedMs=round(elapsed_ms, 1),
            result=result,
            error=error,
        ),
        is_acs=is_acs,
    )
