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


class RecordingConfig(BaseModel):
    """Configuration for ACS call recording."""
    auto_start_on_connect: bool = True
    auto_stop_on_disconnect: bool = True
    recording_format: str = "wav"  # wav, mp4
    recording_channel: str = "unmixed"  # mixed, unmixed
    storage_container: str = "call-recordings"
    enable_transcription: bool = False


class CallSession(BaseModel):
    """Represents an active call session with recording state."""
    call_connection_id: str
    server_call_id: Optional[str] = None
    participant_raw_id: Optional[str] = None
    recording_id: Optional[str] = None
    recording_state: Optional[str] = None
    connected_at: Optional[datetime] = None
    disconnected_at: Optional[datetime] = None
    media_streaming_active: bool = False
    recording_url: Optional[str] = None


class ACSEventManager:
    """
    Centralized manager for ACS call events with recording capabilities.
    
    Features:
    - Event routing to specific handlers
    - Automatic recording start/stop
    - Session state management
    - Error handling and logging
    - Integration with existing ACS router
    """
    
    def __init__(self, recording_config: Optional[RecordingConfig] = None):
        self.recording_config = recording_config or RecordingConfig()
        self.active_sessions: Dict[str, CallSession] = {}
        self.event_handlers: Dict[ACSEventType, Callable] = {
            ACSEventType.CALL_CONNECTED: self._handle_call_connected,
            ACSEventType.CALL_DISCONNECTED: self._handle_call_disconnected,
            ACSEventType.MEDIA_STREAMING_STARTED: self._handle_media_streaming_started,
            ACSEventType.MEDIA_STREAMING_STOPPED: self._handle_media_streaming_stopped,
            ACSEventType.RECORDING_STATE_CHANGED: self._handle_recording_state_changed,
            ACSEventType.PLAY_COMPLETED: self._handle_play_completed,
            ACSEventType.PLAY_FAILED: self._handle_play_failed,
            ACSEventType.PLAY_CANCELED: self._handle_play_canceled,
        }
        
    async def process_callback_events(self, request: Request, events: list) -> Dict[str, Any]:
        """
        Process incoming ACS callback events.
        
        Args:
            request: FastAPI request object with app state
            events: List of raw event data from ACS
            
        Returns:
            Dictionary with processing results
        """
        correlation_id = str(uuid.uuid4())
        processed_events = []
        errors = []
        
        try:
            for raw_event in events:
                try:
                    # Parse cloud event
                    event = CloudEvent.from_dict(raw_event)
                    event_type = ACSEventType(event.type)
                    
                    # Extract common event data
                    call_connection_id = event.data.get("callConnectionId")
                    server_call_id = event.data.get("serverCallId")
                    
                    # Log event with emoji for visual identification
                    emoji = self._get_event_emoji(event_type)
                    logger.info(
                        f"{emoji} Processing ACS event: {event_type.value}",
                        extra={
                            "event_type": event_type.value,
                            "call_connection_id": call_connection_id,
                            "server_call_id": server_call_id,
                            "correlation_id": correlation_id,
                        }
                    )
                    
                    # Route to specific handler
                    if event_type in self.event_handlers:
                        result = await self.event_handlers[event_type](
                            request=request,
                            event=event,
                            correlation_id=correlation_id,
                        )
                        processed_events.append({
                            "event_type": event_type.value,
                            "call_connection_id": call_connection_id,
                            "result": result,
                        })
                    else:
                        logger.warning(
                            f"No handler for event type: {event_type.value}",
                            extra={"correlation_id": correlation_id}
                        )
                        
                except ValueError:
                    # Unknown event type
                    logger.warning(
                        f"Unknown ACS event type: {event.type}",
                        extra={"correlation_id": correlation_id}
                    )
                except Exception as e:
                    error_msg = f"Error processing event: {str(e)}"
                    logger.error(error_msg, exc_info=True, extra={"correlation_id": correlation_id})
                    errors.append(error_msg)
                    
            return {
                "status": "processed",
                "correlation_id": correlation_id,
                "processed_events": processed_events,
                "errors": errors,
            }
            
        except Exception as e:
            logger.error(f"Failed to process callback events: {str(e)}", exc_info=True)
            return {
                "status": "error",
                "correlation_id": correlation_id,
                "error": str(e),
            }
            
    async def _handle_call_connected(
        self,
        request: Request,
        event: CloudEvent,
        correlation_id: str,
    ) -> Dict[str, Any]:
        """Handle call connected event and start recording if configured."""
        
        call_connection_id = event.data.get("callConnectionId")
        server_call_id = event.data.get("serverCallId")
        
        try:
            # Create or update call session
            session = self.active_sessions.get(call_connection_id)
            if not session:
                session = CallSession(
                    call_connection_id=call_connection_id,
                    server_call_id=server_call_id,
                    connected_at=datetime.now(timezone.utc),
                )
                self.active_sessions[call_connection_id] = session
            else:
                session.connected_at = datetime.now(timezone.utc)
                if server_call_id:
                    session.server_call_id = server_call_id
                    
            # Extract participant information
            if "participant" in event.data:
                participant_data = event.data["participant"]
                session.participant_raw_id = participant_data.get("rawId")
                
            logger.info(
                "Call connected successfully",
                extra={
                    "call_connection_id": call_connection_id,
                    "server_call_id": server_call_id,
                    "participant_raw_id": session.participant_raw_id,
                    "correlation_id": correlation_id,
                }
            )
            
            # Start recording if auto-start is enabled
            recording_result = None
            if self.recording_config.auto_start_on_connect:
                recording_result = await self._start_call_recording(
                    request=request,
                    session=session,
                    correlation_id=correlation_id,
                )
                
            return {
                "status": "success",
                "session_created": True,
                "recording_started": recording_result is not None,
                "recording_id": recording_result.get("recording_id") if recording_result else None,
            }
            
        except Exception as e:
            logger.error(
                f"Error handling call connected: {str(e)}",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                },
                exc_info=True,
            )
            return {"status": "error", "error": str(e)}
            
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
