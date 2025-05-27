from fastapi import WebSocket
from utils.ml_logging import get_logger


logger = get_logger("route_turn")


async def route_turn(cm, transcript: str, ws: WebSocket, *, is_acs: bool) -> None:
    """
    Routes a single user utterance through authentication or main dialog,
    then persists the conversation state.

    Adds latency tracking for each step.
    """
    redis_mgr = ws.app.state.redis
    latency_tool = ws.state.lt

    if not cm.get_context("authenticated", False):
        # Processing step for authentication
        latency_tool.start("processing")
        auth_agent = getattr(ws.app.state, "auth_agent", None)
        result = await auth_agent.respond(cm, transcript, ws, is_acs=is_acs)
        latency_tool.stop("processing", redis_mgr)

        if result and result.get("authenticated"):
            cm.update_context("authenticated", True)
            # If authentication is successful, update context and system prompt
            phone_number = result.get("phone_number", None)
            patient_dob = result.get("patient_dob", None)
            patient_id = result.get("patient_id", None)
            first_name = result.get("first_name", None)
            last_name = result.get("last_name", None)
            patient_name = (
                f"{first_name} {last_name}" if first_name and last_name else None
            )
            cm.update_context("phone_number", phone_number)
            cm.update_context("patient_dob", patient_dob)
            cm.update_context("patient_id", patient_id)
            cm.update_context("patient_name", patient_name)
            cm.upsert_system_prompt()
            logger.info(f"Session {cm.session_id} authenticated successfully.")
    else:
        # Processing step for main dialog
        latency_tool.start("processing")
        task_agent = getattr(ws.app.state, "task_agent", None)
        await task_agent.respond(cm, transcript, ws, is_acs=is_acs)
        latency_tool.stop("processing", redis_mgr)

    cm.persist_to_redis(redis_mgr)
