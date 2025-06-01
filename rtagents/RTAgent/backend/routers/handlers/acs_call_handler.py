from typing import Dict, Any, List
from utils.ml_logging import get_logger
from azure.core.messaging import CloudEvent
from src.redis.async_manager import AsyncAzureRedisManager
from src.redis.key_manager import DataType, Component
from rtagents.RTAgent.backend.services.acs.acs_call_service import ACSCallService
from rtagents.RTAgent.backend.orchestration.conversation_state import ConversationManager
from rtagents.RTAgent.backend.settings import (
    BASE_URL,
    ACS_RECORDING_CALLBACK_PATH,
    AZURE_STORAGE_ACCOUNT_NAME,
    AZURE_STORAGE_RECORDING_CONTAINER_NAME,
)
import json
from datetime import datetime

logger = get_logger("routers.handlers.acs")

async def handle_participants_updated(
    event: Dict[str, Any], 
    conversation_manager: ConversationManager
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
        await conversation_manager.set_call_participants(participants)
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
    conversation_manager: ConversationManager
) -> Dict[str, Any]:
    """
    Initiate call recording using AsyncAzureRedisManager
    Following Azure security and compliance patterns
    """
    try:
        recording_result = await call_service.start_recording_for_participants(
            server_call_id=server_call_id,
            recording_callback_url=f"{BASE_URL.strip('/')}{ACS_RECORDING_CALLBACK_PATH}",
            participants=participants,
            storage_account_name=AZURE_STORAGE_ACCOUNT_NAME,
            recording_container=AZURE_STORAGE_RECORDING_CONTAINER_NAME        )
        
        if recording_result.get("success"):
            recording_id = recording_result.get("recording_id"),
            # Update recording state in Redis


            await conversation_manager.set_call_recording_state({
                "recording_id": recording_id,
                "state": "started",
                "participants_count": len(participants),
                "storage_account": recording_result.get("storage_account"),
                "started_at": recording_result.get("started_at"),
            })

            logger.info(
                f"✅ Recording initiated successfully: {recording_result.get('recording_id')}",
                extra={
                    "call_connection_id": call_connection_id,
                    "recording_id": recording_result.get("recording_id"),
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
    conversation_manager: ConversationManager,
    call_service: ACSCallService
) -> Dict[str, Any]:    
    """Handle call connected event using ConversationManager"""
    event_data = event.data
    call_connection_id = event_data.get("callConnectionId")
    server_call_id = event_data.get("serverCallId")
    
    if not call_connection_id:
        logger.error("❌ No call_connection_id in call connected event")
        return {"success": False, "error": "Missing call_connection_id"}

    try:
        # Get participants using ConversationManager
        participants = await conversation_manager.get_call_participants()
        
        if not participants:
            single_participant = event_data.get("participant")
            if single_participant:
                participants = [single_participant]
                # Store the participant for future reference
                await conversation_manager.set_call_participants(participants)

        recording_result = {"recording_initiated": False}
        if recording_config.auto_start_on_connect:
            recording_result = await initiate_call_recording(
                call_connection_id, server_call_id, participants, call_service, conversation_manager
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
    call_id: str,
    conversation_manager: ConversationManager,
    call_service: ACSCallService
) -> Dict[str, Any]:
    """Handle call disconnected event and clear session cache using ConversationManager."""
    call_connection_id = event.data.get("callConnectionId")
    
    try:
        if not call_connection_id:
            logger.error(
                "Missing call_connection_id in disconnected event",
                extra={"call_id": call_id}
            )
            return {"status": "error", "message": "Missing call_connection_id"}
        
        # Check if recording is active using ConversationManager
        recording_info = await conversation_manager.get_call_recording_state()
        if recording_info and recording_info.get("state") == "started":
            try:
                # Stop the recording
                stop_result = await call_service.stop_recording(recording_info.get("recording_id"))
                if stop_result.get("success"):
                    # Update recording state using ConversationManager
                    recording_info.update({"state": "stopped", "stopped_at": datetime.now().isoformat()})
                    await conversation_manager.set_call_recording_state(recording_info)
                    
                    logger.info(
                        "Recording stopped successfully",
                        extra={
                            "call_connection_id": call_connection_id, 
                            "recording_id": recording_info.get("recording_id")
                        }
                    )
                else:
                    logger.error(
                        "Failed to stop recording",
                        extra={
                            "call_connection_id": call_connection_id, 
                            "recording_id": recording_info.get("recording_id"), 
                            "error": stop_result.get("error")
                        }
                    )
            except Exception as e:
                logger.error(
                    f"Error stopping recording: {str(e)}",
                    extra={
                        "call_connection_id": call_connection_id, 
                        "recording_id": recording_info.get("recording_id")
                    },
                    exc_info=True,
                )
        
        # Clear all call session data using ConversationManager
        clear_success = await conversation_manager.clear_call_session()
        
        logger.info(
            "Session cache cleared for disconnected call",
            extra={"call_connection_id": call_connection_id, "call_id": call_id}
        )
        
        return {
            "status": "success", 
            "message": "Session cache cleared",
            "cache_cleared": clear_success
        }
    except Exception as e:
        logger.error(
            f"Error clearing session cache for disconnected call: {str(e)}",
            extra={"call_connection_id": call_connection_id, "call_id": call_id},
            exc_info=True,
        )
        return {"status": "error", "error": str(e)}


async def handle_media_streaming_failed(
    event: CloudEvent,
    call_id: str,
    conversation_manager: ConversationManager
) -> Dict[str, Any]:
    """Handle media streaming failed event using ConversationManager."""
    call_connection_id = event.data.get("callConnectionId")
    error_details = event.data.get("errorDetails", {})
    
    try:
        logger.error(
            "Media streaming failed",
            extra={
                "call_connection_id": call_connection_id,
                "call_id": call_id,
                "error_details": error_details
            }
        )
        
        # Store media streaming failure status using ConversationManager
        await conversation_manager.set_media_streaming_status({
            "status": "failed",
            "reason": error_details.get("message", "Unknown error"),
            "timestamp": event.data.get("timestamp"),
            "error_details": error_details
        })
        
        return {
            "status": "error", 
            "message": "Media streaming failed", 
            "error_details": error_details
        }
    except Exception as e:
        logger.error(
            f"Error handling media streaming failed: {str(e)}",
            extra={"call_connection_id": call_connection_id, "call_id": call_id},
            exc_info=True,
        )
        return {"status": "error", "error": str(e)}


async def handle_media_streaming_started(
    event: CloudEvent,
    call_id: str,
    conversation_manager: ConversationManager
) -> Dict[str, Any]:
    """Handle media streaming started event using ConversationManager."""
    call_connection_id = event.data.get("callConnectionId")
    
    try:
        logger.info(
            "Media streaming started",
            extra={"call_connection_id": call_connection_id, "call_id": call_id}
        )
        
        # Store media streaming success status using ConversationManager
        await conversation_manager.set_media_streaming_status({
            "status": "started",
            "timestamp": datetime.now().isoformat()
        })
        
        return {"status": "success", "message": "Media streaming started"}
    except Exception as e:
        logger.error(
            f"Error handling media streaming started: {str(e)}",
            extra={"call_connection_id": call_connection_id, "call_id": call_id},
            exc_info=True,
        )
        return {"status": "error", "error": str(e)}


async def handle_media_streaming_stopped(
    event: CloudEvent,
    call_id: str,
    conversation_manager: ConversationManager
) -> Dict[str, Any]:
    """Handle media streaming stopped event using ConversationManager."""
    call_connection_id = event.data.get("callConnectionId")
    
    try:
        logger.info(
            "Media streaming stopped",
            extra={"call_connection_id": call_connection_id, "call_id": call_id}
        )
        
        # Store media streaming stopped status using ConversationManager
        await conversation_manager.set_media_streaming_status({
            "status": "stopped",
            "timestamp": datetime.now().isoformat()
        })
        
        return {"status": "success", "message": "Media streaming stopped"}
    except Exception as e:
        logger.error(
            f"Error handling media streaming stopped: {str(e)}",
            extra={"call_connection_id": call_connection_id, "call_id": call_id},
            exc_info=True,
        )
        return {"status": "error", "error": str(e)}