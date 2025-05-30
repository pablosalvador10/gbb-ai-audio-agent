"""
services/acs/events/acs_event_manager.py
======================================
Azure Communication Services Event Manager

Centralized event handling for ACS call events with recording capabilities.
Integrates with existing ACS router and provides specific handlers for each event type.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Callable
from enum import Enum

from azure.core.exceptions import HttpResponseError
from azure.core.messaging import CloudEvent
from fastapi import Request, WebSocket
from pydantic import BaseModel
from rtagents.RTAgent.backend.orchestration.conversation_state import (
    ConversationManager,
)
from src.redis.async_manager import AsyncAzureRedisManager


from utils.ml_logging import get_logger

logger = get_logger("services.acs.events.manager")


class ACSEventType(str, Enum):
    """ACS Event types that we handle."""
    CALL_CONNECTED = "Microsoft.Communication.CallConnected"
    CALL_DISCONNECTED = "Microsoft.Communication.CallDisconnected"
    MEDIA_STREAMING_STARTED = "Microsoft.Communication.MediaStreamingStarted"
    MEDIA_STREAMING_STOPPED = "Microsoft.Communication.MediaStreamingStopped"
    RECORDING_STATE_CHANGED = "Microsoft.Communication.RecordingStateChanged"
    PLAY_COMPLETED = "Microsoft.Communication.PlayCompleted"
    PLAY_FAILED = "Microsoft.Communication.PlayFailed"
    PLAY_CANCELED = "Microsoft.Communication.PlayCanceled"
    PARTICIPANTS_UPDATED = "Microsoft.Communication.ParticipantsUpdated"


class RecordingConfig(BaseModel):
    """Configuration for ACS call recording."""
    auto_start_on_connect: bool = True
    auto_stop_on_disconnect: bool = True
    recording_format: str = "wav"  # wav, mp4
    recording_channel: str = "unmixed"  # mixed, unmixed
    storage_container: str = "call-recordings"
    enable_transcription: bool = False


# CallSession functionality has been consolidated into ConversationManager
# All call-specific state is now stored in ConversationManager's context

from typing import List, Dict, Any
from rtagents.RTAgent.backend.services.acs.acs_call_service import ACSCallService

class ACSEventManager:
    def __init__(
            self, 
            call_id: str,
            recording_config: Optional[RecordingConfig] = None, 
            call_service: Optional[ACSCallService] = None,
            app_state: Optional[Any] = None,
            conversation_manager: Optional[ConversationManager] = None,
            redis_mgr: Optional[AsyncAzureRedisManager] = None
        ):
        self.call_id = call_id
        self.call_service = call_service

        self.recording_config = recording_config or RecordingConfig()
        self.app_state = app_state

        # Use specialized session manager instead of raw ConversationManager
        self.conversation_manager = conversation_manager or ConversationManager(session_id=call_id)
        self.redis_mgr = redis_mgr or AsyncAzureRedisManager()

        # Define event handlers mapping
        self.event_handlers: Dict[ACSEventType, Callable] = {
            ACSEventType.CALL_CONNECTED: self._handle_call_connected,
            ACSEventType.CALL_DISCONNECTED: self._handle_call_disconnected,
            ACSEventType.MEDIA_STREAMING_STARTED: self._handle_media_streaming_started,
            ACSEventType.MEDIA_STREAMING_STOPPED: self._handle_media_streaming_stopped,
            ACSEventType.RECORDING_STATE_CHANGED: self._handle_recording_state_changed,
            ACSEventType.PLAY_COMPLETED: self._handle_play_completed,
            ACSEventType.PLAY_FAILED: self._handle_play_failed,
            ACSEventType.PLAY_CANCELED: self._handle_play_canceled,
            ACSEventType.PARTICIPANTS_UPDATED: self._handle_participants_updated,
        }


            
    async def process_callback_events(self, call_connection_id: str, events: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Process ACS callback events using the enhanced session manager
        Following Azure Well-Architected Framework patterns for reliability
        """
        if not events:
            logger.warning("⚠️ No events provided for processing")
            return {"success": False, "error": "No events provided"}
        
        results = []
        correlation_id = events[0].get("correlation_id", "unknown")
        
        try:
            for event in events:
                event_type = event.get("type", "unknown")
                call_connection_id = event.get("call_connection_id")
                
                # Route to appropriate handler based on event type
                if event_type == "Microsoft.Communication.CallConnected":
                    result = await self._handle_call_connected(event)
                elif event_type == "Microsoft.Communication.CallDisconnected":
                    result = await self._handle_call_disconnected(event)
                elif event_type == "Microsoft.Communication.ParticipantsUpdated":
                    result = await self._handle_participants_updated(event)
                elif event_type == "Microsoft.Communication.MediaStreamingStarted":
                    result = await self._handle_media_streaming_started(event)
                elif event_type == "Microsoft.Communication.MediaStreamingStopped":
                    result = await self._handle_media_streaming_stopped(event)
                elif event_type == "Microsoft.Communication.RecordingStateChanged":
                    result = await self._handle_recording_state_changed(event)
                else:
                    logger.warning(f"🔍 Unhandled event type: {event_type}")
                    result = {
                        "success": True,
                        "message": f"Event type {event_type} acknowledged but not processed",
                        "event_type": event_type
                    }
                
                # Add event metadata to result
                result.update({
                    "event_type": event_type,
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id
                })
                
                results.append(result)
            
            # Calculate overall success
            successful_events = sum(1 for r in results if r.get("success", False))
            total_events = len(results)
            
            logger.info(
                f"📊 Processed {successful_events}/{total_events} events successfully",
                extra={
                    "successful_events": successful_events,
                    "total_events": total_events,
                    "correlation_id": correlation_id
                }
            )
            
            return {
                "success": successful_events == total_events,
                "processed_events": successful_events,
                "total_events": total_events,
                "results": results,
                "correlation_id": correlation_id
            }
            
        except Exception as e:
            logger.error(
                f"❌ Critical error in event processing batch",
                extra={
                    "correlation_id": correlation_id,
                    "error": str(e)
                },
                exc_info=True
            )
            
            return {
                "success": False,
                "error": str(e),
                "correlation_id": correlation_id,
                "results": results  # Return partial results if any were processed
            }

    async def _handle_participants_updated(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle participants updated event using specialized session manager"""
        event_data = event.get("data", {})
        participants = event_data.get("participants", [])
        # call_connection_id = event_data.get("callConnectionId")
        call_connection_id = event_data.get("callConnectionId") or event_data.get("call_connection_id")
        if not call_connection_id:
            logger.error("❌ No call_connection_id in participants updated event")
            return {"success": False, "error": "Missing call_connection_id"}

        try:
            # Extract participants from event data
            participants = event_data.get("participants", [])

            if not participants:
                logger.warning(f"⚠️ No participants in event for call {call_connection_id}")
                return {"success": True, "participants_count": 0}

            # Update participants using specialized method
            success = await self.session_manager.update_participants(
                call_connection_id=call_connection_id,
                participants=participants,
                metadata={"event_type": "participants_updated", "source": "acs_event"}
            )
            
            if success:
                return {
                    "success": True,
                    "participants_count": len(participants),
                    "participants_updated": True
                }
            else:
                return {"success": False, "error": "Failed to update participants"}
                
        except Exception as e:
            logger.error(f"❌ Error handling participants updated: {e}")
            return {"success": False, "error": str(e)}

    async def _initiate_call_recording(
        self, 
        call_connection_id: str, 
        participants: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Initiate call recording using the specialized session manager and helpers
        Following Azure security and compliance patterns
        """
        try:
            from ..acs_helpers import initiate_acs_call_recording
            
            # Use the enhanced helper function with session manager
            recording_result = await initiate_acs_call_recording(
                call_connection_id=call_connection_id,
                participants=participants,
                app_state=self.app_state
            )
            
            if recording_result.get("success"):
                # Update recording state using session manager
                await self.session_manager.update_recording_state(
                    call_connection_id=call_connection_id,
                    recording_id=recording_result.get("recording_id"),
                    state="started",
                    metadata={
                        "participants_count": len(participants),
                        "auto_initiated": True,
                        "storage_account": recording_result.get("storage_account")
                    }
                )
                
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
                    "recording_id": recording_result.get("recording_id"),
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

    async def _handle_call_connected(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Handle call connected event using specialized session manager"""
        # Capture participants from event data
        event_data = event.get("data", {})
        participants = event_data.get("participants", [])
        call_connection_id = event_data.get("callConnectionId")
        if not call_connection_id:
            logger.error("❌ No call_connection_id in call connected event")
            return {"success": False, "error": "Missing call_connection_id"}

        try:
            # Set correlation ID for tracing

            
            if not participants:
                # Fallback: single participant format
                single_participant = event_data.get("participant")
                if single_participant:
                    participants = [single_participant]
            

            # Initiate recording if configured
            recording_result = {"recording_initiated": False}
            if self.recording_config.auto_start_on_connect:
                recording_result = await self._initiate_call_recording(
                    call_connection_id, participants
                )
            
            logger.info(f"✅ Call connected successfully: {call_connection_id}")
            return {
                "success": True,
                "call_connection_id": call_connection_id,
                "participants_captured": len(participants),
                **recording_result
            }
            
        except Exception as e:
            logger.error(f"❌ Error handling call connected: {e}")
            return {"success": False, "error": str(e)}

    async def _handle_call_disconnected(
        self,
        request: Request,
        event: CloudEvent,
        correlation_id: str,
    ) -> Dict[str, Any]:
        """Handle call disconnected event and stop recording."""
        
        call_connection_id = event.data.get("callConnectionId")
        
        try:
            session = self.active_sessions.get(call_connection_id)
            if not session:
                logger.warning(
                    "No active session found for disconnected call",
                    extra={
                        "call_connection_id": call_connection_id,
                        "correlation_id": correlation_id,
                    }
                )
                return {"status": "warning", "message": "No active session found"}
                
            # Update session
            session.disconnected_at = datetime.now(timezone.utc)
            
            # Calculate call duration
            duration = None
            if session.connected_at:
                duration = (session.disconnected_at - session.connected_at).total_seconds()
                
            logger.info(
                "Call disconnected",
                extra={
                    "call_connection_id": call_connection_id,
                    "duration_seconds": duration,
                    "correlation_id": correlation_id,
                }
            )
            
            # Stop recording if active and auto-stop is enabled
            recording_result = None
            if (session.recording_id and 
                session.recording_state in ("active", "starting") and
                self.recording_config.auto_stop_on_disconnect):
                recording_result = await self._stop_call_recording(
                    request=request,
                    session=session,
                    correlation_id=correlation_id,
                )
                
            # Clean up session (keep for a short time for final events)
            # In production, you might want to move this to a cleanup task
            # self.active_sessions.pop(call_connection_id, None)
            
            return {
                "status": "success",
                "duration_seconds": duration,
                "recording_stopped": recording_result is not None,
                "recording_url": recording_result.get("download_url") if recording_result else None,
            }
            
        except Exception as e:
            logger.error(
                f"Error handling call disconnected: {str(e)}",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                },
                exc_info=True,
            )
            return {"status": "error", "error": str(e)}
            
    async def _handle_media_streaming_started(
        self,
        request: Request,
        event: CloudEvent,
        correlation_id: str,
    ) -> Dict[str, Any]:
        """Handle media streaming started event."""
        
        call_connection_id = event.data.get("callConnectionId")
        
        try:
            session = self.active_sessions.get(call_connection_id)
            if session:
                session.media_streaming_active = True
                
            logger.info(
                "Media streaming started",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                }
            )
            
            # Optionally start recording when media streaming begins
            # if not already started by call connected event
            recording_result = None
            if (session and 
                not session.recording_id and 
                self.recording_config.auto_start_on_connect):
                recording_result = await self._start_call_recording(
                    request=request,
                    session=session,
                    correlation_id=correlation_id,
                )
                
            return {
                "status": "success", 
                "media_streaming_active": True,
                "recording_started": recording_result is not None,
            }
            
        except Exception as e:
            logger.error(
                f"Error handling media streaming started: {str(e)}",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                },
                exc_info=True,
            )
            return {"status": "error", "error": str(e)}
            
    async def _handle_media_streaming_stopped(
        self,
        request: Request,
        event: CloudEvent,
        correlation_id: str,
    ) -> Dict[str, Any]:
        """Handle media streaming stopped event."""
        
        call_connection_id = event.data.get("callConnectionId")
        
        try:
            session = self.active_sessions.get(call_connection_id)
            if session:
                session.media_streaming_active = False
                
            logger.info(
                "Media streaming stopped",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                }
            )
            
            return {"status": "success", "media_streaming_active": False}
            
        except Exception as e:
            logger.error(
                f"Error handling media streaming stopped: {str(e)}",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                },
                exc_info=True,
            )
            return {"status": "error", "error": str(e)}
            
    async def _handle_recording_state_changed(
        self,
        request: Request,
        event: CloudEvent,
        correlation_id: str,
    ) -> Dict[str, Any]:
        """Handle recording state change events."""
        
        recording_id = event.data.get("recordingId")
        recording_state = event.data.get("recordingState")
        call_connection_id = event.data.get("callConnectionId")
        
        try:
            # Find session by call connection ID
            session = self.active_sessions.get(call_connection_id) if call_connection_id else None
            
            if session:
                session.recording_state = recording_state
                if not session.recording_id:
                    session.recording_id = recording_id
                    
            logger.info(
                "Recording state changed",
                extra={
                    "recording_id": recording_id,
                    "recording_state": recording_state,
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                }
            )
            
            # Handle specific state changes
            result = {"status": "success", "recording_state": recording_state}
            
            if recording_state == "active":
                result["message"] = "Recording is now active"
            elif recording_state in ("inactive", "stopped"):
                result["message"] = "Recording has stopped"
                # Optionally get download URL
                if session and session.recording_id:
                    download_url = await self._get_recording_download_url(
                        request=request,
                        recording_id=session.recording_id,
                        correlation_id=correlation_id,
                    )
                    if download_url:
                        session.recording_url = download_url
                        result["download_url"] = download_url
            elif recording_state == "failed":
                result["message"] = "Recording failed"
                result["status"] = "error"
                
            return result
            
        except Exception as e:
            logger.error(
                f"Error handling recording state change: {str(e)}",
                extra={
                    "recording_id": recording_id,
                    "correlation_id": correlation_id,
                },
                exc_info=True,
            )
            return {"status": "error", "error": str(e)}
            
    async def _handle_play_completed(
        self,
        request: Request,
        event: CloudEvent,
        correlation_id: str,
    ) -> Dict[str, Any]:
        """Handle play completed event."""
        
        call_connection_id = event.data.get("callConnectionId")
        operation_context = event.data.get("operationContext")
        
        logger.info(
            "Play completed",
            extra={
                "call_connection_id": call_connection_id,
                "operation_context": operation_context,
                "correlation_id": correlation_id,
            }
        )
        
        return {"status": "success", "operation": "play_completed"}
        
    async def _handle_play_failed(
        self,
        request: Request,
        event: CloudEvent,
        correlation_id: str,
    ) -> Dict[str, Any]:
        """Handle play failed event."""
        
        call_connection_id = event.data.get("callConnectionId")
        operation_context = event.data.get("operationContext")
        
        logger.warning(
            "Play failed",
            extra={
                "call_connection_id": call_connection_id,
                "operation_context": operation_context,
                "correlation_id": correlation_id,
            }
        )
        
        return {"status": "error", "operation": "play_failed"}
        
    async def _handle_play_canceled(
        self,
        request: Request,
        event: CloudEvent,
        correlation_id: str,
    ) -> Dict[str, Any]:
        """Handle play canceled event."""
        
        call_connection_id = event.data.get("callConnectionId")
        operation_context = event.data.get("operationContext")
        
        logger.info(
            "Play canceled",
            extra={
                "call_connection_id": call_connection_id,
                "operation_context": operation_context,
                "correlation_id": correlation_id,
            }
        )
        
        return {"status": "success", "operation": "play_canceled"}
        
    async def _start_call_recording(
        self,
        request: Request,
        session: CallSession,
        correlation_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Start call recording using ACS Call Automation."""
        
        try:
            # Get ACS caller from app state
            acs_caller = getattr(request.app.state, "acs_caller", None)
            if not acs_caller:
                logger.error("ACS caller not available in app state")
                return None
                
            # For this implementation, we'll use a placeholder for the actual ACS recording API
            # In a real implementation, you would call:
            # recording_result = await acs_caller.start_recording(...)
            
            logger.info(
                "Recording start initiated (placeholder implementation)",
                extra={
                    "call_connection_id": session.call_connection_id,
                    "server_call_id": session.server_call_id,
                    "correlation_id": correlation_id,
                    "format": self.recording_config.recording_format,
                    "channel": self.recording_config.recording_channel,
                }
            )
            
            # Placeholder for actual recording start
            # This would be replaced with actual ACS Recording API calls
            mock_recording_id = f"rec_{uuid.uuid4().hex[:8]}"
            session.recording_id = mock_recording_id
            session.recording_state = "starting"
            
            return {
                "recording_id": mock_recording_id,
                "status": "started",
                "format": self.recording_config.recording_format,
                "channel": self.recording_config.recording_channel,
            }
            
        except Exception as e:
            logger.error(
                f"Failed to start recording: {str(e)}",
                extra={
                    "call_connection_id": session.call_connection_id,
                    "correlation_id": correlation_id,
                },
                exc_info=True,
            )
            return None
            
    async def _stop_call_recording(
        self,
        request: Request,
        session: CallSession,
        correlation_id: str,
    ) -> Optional[Dict[str, Any]]:
        """Stop call recording using ACS Call Automation."""
        
        try:
            # Get ACS caller from app state
            acs_caller = getattr(request.app.state, "acs_caller", None)
            if not acs_caller:
                logger.error("ACS caller not available in app state")
                return None
                
            logger.info(
                "Recording stop initiated (placeholder implementation)",
                extra={
                    "call_connection_id": session.call_connection_id,
                    "recording_id": session.recording_id,
                    "correlation_id": correlation_id,
                }
            )
            
            # Placeholder for actual recording stop
            session.recording_state = "stopped"
            
            # Get download URL
            download_url = await self._get_recording_download_url(
                request=request,
                recording_id=session.recording_id,
                correlation_id=correlation_id,
            )
            
            return {
                "recording_id": session.recording_id,
                "status": "stopped",
                "download_url": download_url,
            }
            
        except Exception as e:
            logger.error(
                f"Failed to stop recording: {str(e)}",
                extra={
                    "call_connection_id": session.call_connection_id,
                    "recording_id": session.recording_id,
                    "correlation_id": correlation_id,
                },
                exc_info=True,
            )
            return None
            
    async def _get_recording_download_url(
        self,
        request: Request,
        recording_id: str,
        correlation_id: str,
    ) -> Optional[str]:
        """Get secure download URL for recording."""
        
        try:
            # Placeholder for actual recording download URL generation
            # In real implementation, this would:
            # 1. Get recording properties from ACS
            # 2. Generate secure download URL (possibly via Azure Blob Storage)
            # 3. Return time-limited SAS URL
            
            logger.info(
                "Recording download URL generated (placeholder)",
                extra={
                    "recording_id": recording_id,
                    "correlation_id": correlation_id,
                }
            )
            
            # Mock URL for demonstration
            return f"https://recordings.blob.core.windows.net/calls/{recording_id}.wav?sas=mock"
            
        except Exception as e:
            logger.error(
                f"Failed to get recording download URL: {str(e)}",
                extra={
                    "recording_id": recording_id,
                    "correlation_id": correlation_id,
                },
                exc_info=True,
            )
            return None
            
    def _get_event_emoji(self, event_type: ACSEventType) -> str:
        """Get emoji for visual event identification."""
        emoji_map = {
            ACSEventType.CALL_CONNECTED: "📞",
            ACSEventType.CALL_DISCONNECTED: "❌",
            ACSEventType.MEDIA_STREAMING_STARTED: "🎙️",
            ACSEventType.MEDIA_STREAMING_STOPPED: "🛑",
            ACSEventType.RECORDING_STATE_CHANGED: "🎬",
            ACSEventType.PLAY_COMPLETED: "✅",
            ACSEventType.PLAY_FAILED: "⚠️",
            ACSEventType.PLAY_CANCELED: "🚫",
        }
        return emoji_map.get(event_type, "ℹ️")
        
    def get_session_info(self, call_connection_id: str) -> Optional[CallSession]:
        """Get session information for a call."""
        return self.active_sessions.get(call_connection_id)
        
    def list_active_sessions(self) -> Dict[str, CallSession]:
        """Get all active call sessions."""
        return self.active_sessions.copy()
        
    async def cleanup_old_sessions(self, max_age_hours: int = 24) -> int:
        """Clean up old inactive sessions."""
        cutoff_time = datetime.now(timezone.utc) - asyncio.timedelta(hours=max_age_hours)
        removed_count = 0
        
        to_remove = []
        for call_id, session in self.active_sessions.items():
            if (session.disconnected_at and 
                session.disconnected_at < cutoff_time):
                to_remove.append(call_id)
                
        for call_id in to_remove:
            del self.active_sessions[call_id]
            removed_count += 1
            
        if removed_count > 0:
            logger.info(f"Cleaned up {removed_count} old call sessions")
            
        return removed_count
            