from typing import Dict, Any, List
from utils.ml_logging import get_logger
from azure.core.messaging import CloudEvent
from src.redis.async_manager import AsyncAzureRedisManager
from rtagents.RTAgent.backend.services.acs.acs_call_service import ACSCallService

from rtagents.RTAgent.backend.settings import (
    BASE_URL,
    ACS_RECORDING_CALLBACK_PATH,
    AZURE_STORAGE_ACCOUNT_NAME,
    AZURE_STORAGE_RECORDING_CONTAINER_NAME,
)
import json

logger = get_logger("routers.handlers.acs")

async def handle_participants_updated(
    event: Dict[str, Any], redis_manager: AsyncAzureRedisManager
) -> Dict[str, Any]:
    """Handle participants updated event using AsyncAzureRedisManager"""
    event_data = event.data
    participants = event_data.get("participants", [])
    call_connection_id = event_data.get("callConnectionId") or event_data.get("call_connection_id")
    if not call_connection_id:
        logger.error("❌ No call_connection_id in participants updated event")
        return {"success": False, "error": "Missing call_connection_id"}

    try:
        if not participants:
            logger.warning(f"⚠️ No participants in event for call {call_connection_id}")
            return {"success": True, "participants_count": 0}

        # Update participants in Redis
        await redis_manager.set_value(
            f"call:{call_connection_id}:participants",
            json.dumps({"participants": participants, "event_type": "participants_updated"})
        )
        
        return {
            "success": True,
            "participants_count": len(participants),
            "participants_updated": True
        }
    except Exception as e:
        logger.error(f"❌ Error handling participants updated: {e}")
        return {"success": False, "error": str(e)}


async def initiate_call_recording(
    call_connection_id: str,
    server_call_id: str, 
    participants: List[Dict[str, Any]],
    call_service: ACSCallService,
    redis_manager: AsyncAzureRedisManager
) -> Dict[str, Any]:
    """
    Initiate call recording using AsyncAzureRedisManager
    Following Azure security and compliance patterns
    """
    try:
        # recording_result = await initiate_acs_call_recording(
        #     call_connection_id=call_connection_id,
        #     participants=participants
        # )
        recording_result = await call_service.start_recording_for_participants(
            server_call_id=server_call_id,
            recording_callback_url=f"{BASE_URL.strip('/')}{ACS_RECORDING_CALLBACK_PATH}",
            participants=participants,
            storage_account_name=AZURE_STORAGE_ACCOUNT_NAME,
            recording_container=AZURE_STORAGE_RECORDING_CONTAINER_NAME
        )
        
        if recording_result.get("success"):
            recording_id = recording_result.get("recording_id"),
            # Update recording state in Redis
            await redis_manager.set_value(
                f"call:{call_connection_id}:recording",
                json.dumps({
                    "recording_id": recording_id,
                    "state": "started",
                    "participants_count": len(participants),
                    "storage_account": recording_result.get("storage_account")
                })
            )
            
            logger.info(
                f"✅ Recording initiated successfully: {recording_result.get('recording_id')}",
                extra={
                    "call_connection_id": call_connection_id,
                    "recording_id": recording_result.get("recording_id"),
                    "participants_count": len(participants)
                }
            )
            await redis_manager.set_value(
                f"call:{call_connection_id}:recording",
                json.dumps({
                    "recording_id": recording_id,
                    "state": "started",
                    "participants_count": len(participants),
                    "storage_account": recording_result.get("storage_account")
                })
            )

            logger.info(
                f"✅ Recording initiated successfully: {recording_id}",
                extra={
                    "call_connection_id": call_connection_id,
                    "recording_id": recording_id,
                    "participants_count": len(participants)
                }
            )
            return {
                "recording_initiated": True,
                "recording_id": recording_id,
                "participants_count": len(participants)
            }
        else:
            logger.error(
                f"❌ Recording initiation failed: {recording_result.get('error')}",
                extra={
                    "call_connection_id": call_connection_id,
                    "error": recording_result.get("error")
                }
            )
            
            return {
                "recording_initiated": False,
                "error": recording_result.get("error")
            }
    except Exception as e:
        logger.error(
            f"❌ Exception during recording initiation: {e}",
            extra={
                "call_connection_id": call_connection_id,
                "error": str(e)
            },
            exc_info=True
        )
        
        return {
            "recording_initiated": False,
            "error": str(e)
        }  

async def handle_call_connected(
    event: Dict[str, Any], 
    recording_config: Dict[str, Any],
    redis_manager: AsyncAzureRedisManager, 
    call_service: ACSCallService
) -> Dict[str, Any]:
    """Handle call connected event using AsyncAzureRedisManager"""
    event_data = event.data
    participants_key = f"call:{event_data.get('callConnectionId')}:participants"
    participants_data = await redis_manager.get_value(participants_key)
    participants = json.loads(participants_data).get("participants", []) if participants_data else []

    call_connection_id = event_data.get("callConnectionId")
    server_call_id = event_data.get("serverCallId") 
    if not call_connection_id:
        logger.error("❌ No call_connection_id in call connected event")
        return {"success": False, "error": "Missing call_connection_id"}

    try:
        if not participants:
            single_participant = event_data.get("participant")
            if single_participant:
                participants = [single_participant]

        recording_result = {"recording_initiated": False}
        if recording_config.auto_start_on_connect:
            recording_result = await initiate_call_recording(
                call_connection_id, server_call_id, participants, call_service, redis_manager
            )
        
        logger.info(f"✅ Call connected successfully: {call_connection_id}")
        return {
            "success": True,
            "server_call_id": server_call_id,
            "call_connection_id": call_connection_id,
            "participants_captured": len(participants),
            **recording_result
        }
    except Exception as e:
        logger.error(f"❌ Error handling call connected: {e}")
        return {"success": False, "error": str(e)}

async def handle_call_disconnected(
    event: CloudEvent,
    correlation_id: str,
    redis_manager: AsyncAzureRedisManager,
    call_service: ACSCallService
) -> Dict[str, Any]:
    """Handle call disconnected event and clear session cache."""
    call_connection_id = event.data.get("callConnectionId")
    
    try:
        if not call_connection_id:
            logger.error(
                "Missing call_connection_id in disconnected event",
                extra={"correlation_id": correlation_id}
            )
            return {"status": "error", "message": "Missing call_connection_id"}
        
        # Clear session cache for the call
        # await redis_manager.delete_session(f"call:{call_connection_id}")
        # Check if recording is active in Redis
        recording_key = f"call:{call_connection_id}:recording"
        recording_data = await redis_manager.get_value(recording_key)
        if recording_data:
            recording_info = json.loads(recording_data)
            if recording_info.get("state") == "started":
                try:
                    # Stop the recording
                    stop_result = await call_service.stop_recording(recording_info.get("recording_id"))
                    if stop_result.get("success"):
                        # Update recording state in Redis
                        await redis_manager.set_value(
                            recording_key,
                            json.dumps({**recording_info, "state": "stopped"})
                        )
                        logger.info(
                            "Recording stopped successfully",
                            extra={"call_connection_id": call_connection_id, "recording_id": recording_info.get("recording_id")}
                        )
                    else:
                        logger.error(
                            "Failed to stop recording",
                            extra={"call_connection_id": call_connection_id, "recording_id": recording_info.get("recording_id"), "error": stop_result.get("error")}
                        )
                except Exception as e:
                    logger.error(
                        f"Error stopping recording: {str(e)}",
                        extra={"call_connection_id": call_connection_id, "recording_id": recording_info.get("recording_id")},
                        exc_info=True,
                    )
        
        logger.info(
            "Session cache cleared for disconnected call",
            extra={"call_connection_id": call_connection_id, "correlation_id": correlation_id}
        )
        
        return {"status": "success", "message": "Session cache cleared"}
    except Exception as e:
        logger.error(
            f"Error clearing session cache for disconnected call: {str(e)}",
            extra={"call_connection_id": call_connection_id, "correlation_id": correlation_id},
            exc_info=True,
        )
        return {"status": "error", "error": str(e)}
