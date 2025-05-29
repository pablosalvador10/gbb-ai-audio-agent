"""
Media Event Handler for ACS Media Streaming Events
==================================================
Handles media streaming started/stopped events and manages audio recording.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict
from datetime import datetime, timezone

from azure.core.messaging import CloudEvent
from fastapi import Request

from .base_handler import BaseEventHandler


class MediaEventHandler(BaseEventHandler):
    """
    Handler for ACS media streaming events
    
    Supported events:
    - Microsoft.Communication.MediaStreamingStarted
    - Microsoft.Communication.MediaStreamingStopped
    """
    
    def __init__(self):
        super().__init__("media_streaming")
        self._streaming_sessions = {}  # Track media streaming sessions
    
    async def _validate_specific_event(self, event: CloudEvent) -> None:
        """
        Validate media-specific event data
        
        Args:
            event: CloudEvent to validate
            
        Raises:
            ValueError: If media event data is invalid
        """
        valid_media_events = {
            "Microsoft.Communication.MediaStreamingStarted",
            "Microsoft.Communication.MediaStreamingStopped",
        }
        
        if event.type not in valid_media_events:
            raise ValueError(f"Unsupported media event type: {event.type}")
            
        # Validate media streaming configuration
        if event.type == "Microsoft.Communication.MediaStreamingStarted":
            streaming_data = event.data.get("mediaStreamingUpdate", {})
            if not streaming_data:
                raise ValueError("Media streaming data is missing")
    
    async def _process_event(
        self, 
        event: CloudEvent, 
        request: Request,
        correlation_id: str
    ) -> Dict[str, Any]:
        """
        Process media streaming events
        
        Args:
            event: CloudEvent to process
            request: FastAPI request object
            correlation_id: Correlation ID for tracking
            
        Returns:
            Dict containing processing results
        """
        event_type = event.type
        
        if event_type == "Microsoft.Communication.MediaStreamingStarted":
            return await self._handle_media_streaming_started(event, request, correlation_id)
        elif event_type == "Microsoft.Communication.MediaStreamingStopped":
            return await self._handle_media_streaming_stopped(event, request, correlation_id)
        else:
            raise ValueError(f"Unsupported event type: {event_type}")
    
    async def _handle_media_streaming_started(
        self, 
        event: CloudEvent, 
        request: Request,
        correlation_id: str
    ) -> Dict[str, Any]:
        """
        Handle media streaming started event and initialize recording
        
        Args:
            event: MediaStreamingStarted CloudEvent
            request: FastAPI request object
            correlation_id: Correlation ID for tracking
            
        Returns:
            Dict containing processing results
        """
        call_connection_id = event.data.get("callConnectionId")
        streaming_data = event.data.get("mediaStreamingUpdate", {})
        
        self.logger.info(
            "🎙️ Media streaming started - initializing recording",
            extra={
                "call_connection_id": call_connection_id,
                "correlation_id": correlation_id,
                "streaming_data": streaming_data,
            }
        )
        
        # Extract streaming configuration
        content_type = streaming_data.get("contentType", "audio")
        audio_format = streaming_data.get("audioFormat", {})
        encoding = audio_format.get("encoding", "PCM")
        sample_rate = audio_format.get("sampleRate", 16000)
        channels = audio_format.get("channels", 1)
        
        # Initialize streaming session tracking
        streaming_session = {
            "call_connection_id": call_connection_id,
            "correlation_id": correlation_id,
            "started_at": datetime.now(timezone.utc),
            "status": "active",
            "content_type": content_type,
            "audio_config": {
                "encoding": encoding,
                "sample_rate": sample_rate,
                "channels": channels,
            },
            "recording_config": None,
            "recording_active": False,
        }
        
        self._streaming_sessions[call_connection_id] = streaming_session
        
        # Initialize recording configuration when streaming starts
        try:
            recording_config = await self._initialize_recording_config(
                call_connection_id, 
                streaming_session,
                request
            )
            
            streaming_session["recording_config"] = recording_config
            
            # Start recording automatically when streaming begins
            recording_result = await self._start_call_recording(
                call_connection_id,
                recording_config,
                request,
                correlation_id
            )
            
            if recording_result.get("success", False):
                streaming_session["recording_active"] = True
                streaming_session["recording_started_at"] = datetime.now(timezone.utc)
                
                self.logger.info(
                    "📹 Call recording started successfully",
                    extra={
                        "call_connection_id": call_connection_id,
                        "correlation_id": correlation_id,
                        "recording_config": recording_config,
                    }
                )
            else:
                self.logger.warning(
                    "⚠️ Failed to start call recording",
                    extra={
                        "call_connection_id": call_connection_id,
                        "correlation_id": correlation_id,
                        "error": recording_result.get("error"),
                    }
                )
        
        except Exception as e:
            self.logger.error(
                "❌ Failed to initialize recording",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                    "error": str(e),
                },
                exc_info=True
            )
            # Continue without recording
        
        # Update call session if available (from CallEventHandler)
        try:
            # Check if we have access to call event handler's active calls
            # This is a design decision - you might want to use a shared state manager
            pass
        except Exception:
            pass
        
        # Broadcast to connected clients
        try:
            if hasattr(request.app.state, 'clients'):
                from rtagents.RTAgent.backend.shared_ws import broadcast_message
                await broadcast_message(
                    request.app.state.clients, 
                    f"🎙️ Media Streaming Started: {call_connection_id}"
                )
        except Exception as e:
            self.logger.warning(
                "⚠️ Failed to broadcast media streaming started message",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                    "error": str(e),
                }
            )
        
        return {
            "action": "media_streaming_started",
            "call_connection_id": call_connection_id,
            "audio_config": streaming_session["audio_config"],
            "recording_initialized": streaming_session["recording_active"],
        }
    
    async def _handle_media_streaming_stopped(
        self, 
        event: CloudEvent, 
        request: Request,
        correlation_id: str
    ) -> Dict[str, Any]:
        """
        Handle media streaming stopped event and finalize recording
        
        Args:
            event: MediaStreamingStopped CloudEvent
            request: FastAPI request object
            correlation_id: Correlation ID for tracking
            
        Returns:
            Dict containing processing results
        """
        call_connection_id = event.data.get("callConnectionId")
        
        self.logger.info(
            "🛑 Media streaming stopped - finalizing recording",
            extra={
                "call_connection_id": call_connection_id,
                "correlation_id": correlation_id,
            }
        )
        
        # Get streaming session info
        streaming_session = self._streaming_sessions.get(call_connection_id, {})
        
        if streaming_session:
            streaming_session["status"] = "stopped"
            streaming_session["stopped_at"] = datetime.now(timezone.utc)
            
            # Calculate streaming duration
            if "started_at" in streaming_session:
                duration = (streaming_session["stopped_at"] - streaming_session["started_at"]).total_seconds()
                streaming_session["duration_seconds"] = duration
                
                self.logger.info(
                    "⏱️ Media streaming duration calculated",
                    extra={
                        "call_connection_id": call_connection_id,
                        "duration_seconds": duration,
                        "correlation_id": correlation_id,
                    }
                )
        
        # Stop recording if active
        recording_stopped = False
        if streaming_session.get("recording_active", False):
            try:
                stop_result = await self._stop_call_recording(
                    call_connection_id,
                    request,
                    correlation_id
                )
                
                if stop_result.get("success", False):
                    streaming_session["recording_active"] = False
                    streaming_session["recording_stopped_at"] = datetime.now(timezone.utc)
                    recording_stopped = True
                    
                    # Calculate recording duration
                    if "recording_started_at" in streaming_session:
                        recording_duration = (
                            streaming_session["recording_stopped_at"] - 
                            streaming_session["recording_started_at"]
                        ).total_seconds()
                        streaming_session["recording_duration_seconds"] = recording_duration
                    
                    self.logger.info(
                        "📹 Call recording stopped successfully",
                        extra={
                            "call_connection_id": call_connection_id,
                            "correlation_id": correlation_id,
                            "recording_duration_seconds": streaming_session.get("recording_duration_seconds"),
                        }
                    )
                else:
                    self.logger.warning(
                        "⚠️ Failed to stop call recording",
                        extra={
                            "call_connection_id": call_connection_id,
                            "correlation_id": correlation_id,
                            "error": stop_result.get("error"),
                        }
                    )
            
            except Exception as e:
                self.logger.error(
                    "❌ Failed to stop recording",
                    extra={
                        "call_connection_id": call_connection_id,
                        "correlation_id": correlation_id,
                        "error": str(e),
                    },
                    exc_info=True
                )
        
        # Broadcast to connected clients
        try:
            if hasattr(request.app.state, 'clients'):
                from rtagents.RTAgent.backend.shared_ws import broadcast_message
                await broadcast_message(
                    request.app.state.clients, 
                    f"🛑 Media Streaming Stopped: {call_connection_id}"
                )
        except Exception as e:
            self.logger.warning(
                "⚠️ Failed to broadcast media streaming stopped message",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                    "error": str(e),
                }
            )
        
        return {
            "action": "media_streaming_stopped",
            "call_connection_id": call_connection_id,
            "streaming_duration_seconds": streaming_session.get("duration_seconds"),
            "recording_stopped": recording_stopped,
            "recording_duration_seconds": streaming_session.get("recording_duration_seconds"),
        }
    
    async def _initialize_recording_config(
        self,
        call_connection_id: str,
        streaming_session: Dict[str, Any],
        request: Request
    ) -> Dict[str, Any]:
        """
        Initialize recording configuration based on streaming parameters
        
        Args:
            call_connection_id: Call connection ID
            streaming_session: Current streaming session info
            request: FastAPI request object
            
        Returns:
            Dict containing recording configuration
        """
        audio_config = streaming_session.get("audio_config", {})
        
        # Default recording configuration aligned with Azure best practices
        recording_config = {
            "format": "wav",  # Use WAV for high quality
            "encoding": audio_config.get("encoding", "PCM"),
            "sample_rate": audio_config.get("sample_rate", 16000),
            "channels": audio_config.get("channels", 1),
            "bit_depth": 16,
            "storage": {
                "type": "azure_blob",  # Store in Azure Blob Storage
                "container": "call-recordings",
                "path": f"calls/{call_connection_id}/audio",
                "filename": f"recording_{int(datetime.now(timezone.utc).timestamp())}.wav",
            },
            "metadata": {
                "call_connection_id": call_connection_id,
                "correlation_id": streaming_session.get("correlation_id"),
                "started_at": streaming_session.get("started_at").isoformat() if streaming_session.get("started_at") else None,
            },
        }
        
        # Add environment-specific configuration
        try:
            # Get storage configuration from app settings
            # This would typically come from Azure Key Vault or app configuration
            if hasattr(request.app.state, 'storage_config'):
                storage_config = request.app.state.storage_config
                recording_config["storage"].update(storage_config)
        except Exception as e:
            self.logger.warning(
                "⚠️ Could not load storage configuration, using defaults",
                extra={
                    "call_connection_id": call_connection_id,
                    "error": str(e),
                }
            )
        
        return recording_config
    
    async def _start_call_recording(
        self,
        call_connection_id: str,
        recording_config: Dict[str, Any],
        request: Request,
        correlation_id: str
    ) -> Dict[str, Any]:
        """
        Start call recording with the given configuration
        
        Args:
            call_connection_id: Call connection ID
            recording_config: Recording configuration
            request: FastAPI request object
            correlation_id: Correlation ID for tracking
            
        Returns:
            Dict containing recording start result
        """
        try:
            # This is where you would integrate with Azure Communication Services Recording API
            # For now, we'll create a placeholder implementation
            
            self.logger.info(
                "🎬 Starting call recording",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                    "recording_config": recording_config,
                }
            )
            
            # TODO: Implement actual Azure Communication Services Recording API integration
            # Example:
            # recording_client = request.app.state.acs_recording_client
            # recording_result = await recording_client.start_recording(
            #     call_locator=ServerCallLocator(call_connection_id),
            #     recording_content=RecordingContent.AUDIO,
            #     recording_format=RecordingFormat.WAV,
            #     recording_channel=RecordingChannel.UNMIXED
            # )
            
            # For now, simulate successful recording start
            await asyncio.sleep(0.1)  # Simulate API call
            
            return {
                "success": True,
                "recording_id": f"rec_{call_connection_id}_{int(datetime.now(timezone.utc).timestamp())}",
                "started_at": datetime.now(timezone.utc).isoformat(),
            }
            
        except Exception as e:
            self.logger.error(
                "❌ Failed to start call recording",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                    "error": str(e),
                },
                exc_info=True
            )
            
            return {
                "success": False,
                "error": str(e),
            }
    
    async def _stop_call_recording(
        self,
        call_connection_id: str,
        request: Request,
        correlation_id: str
    ) -> Dict[str, Any]:
        """
        Stop call recording
        
        Args:
            call_connection_id: Call connection ID
            request: FastAPI request object
            correlation_id: Correlation ID for tracking
            
        Returns:
            Dict containing recording stop result
        """
        try:
            self.logger.info(
                "🛑 Stopping call recording",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                }
            )
            
            # TODO: Implement actual Azure Communication Services Recording API integration
            # Example:
            # recording_client = request.app.state.acs_recording_client
            # await recording_client.stop_recording(recording_id)
            
            # For now, simulate successful recording stop
            await asyncio.sleep(0.1)  # Simulate API call
            
            return {
                "success": True,
                "stopped_at": datetime.now(timezone.utc).isoformat(),
            }
            
        except Exception as e:
            self.logger.error(
                "❌ Failed to stop call recording",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                    "error": str(e),
                },
                exc_info=True
            )
            
            return {
                "success": False,
                "error": str(e),
            }
    
    async def _cleanup_on_error(
        self, 
        event: CloudEvent, 
        request: Request,
        correlation_id: str
    ) -> None:
        """
        Cleanup resources on error for media events
        
        Args:
            event: CloudEvent that failed
            request: FastAPI request object
            correlation_id: Correlation ID for tracking
        """
        call_connection_id = event.data.get("callConnectionId")
        
        if call_connection_id and call_connection_id in self._streaming_sessions:
            try:
                streaming_session = self._streaming_sessions[call_connection_id]
                
                # Stop recording if active
                if streaming_session.get("recording_active", False):
                    await self._stop_call_recording(call_connection_id, request, correlation_id)
                
                # Mark session as failed
                streaming_session["status"] = "failed"
                streaming_session["failed_at"] = datetime.now(timezone.utc)
                
                self.logger.info(
                    "🧹 Cleaned up failed media streaming session",
                    extra={
                        "call_connection_id": call_connection_id,
                        "correlation_id": correlation_id,
                    }
                )
            except Exception as e:
                self.logger.error(
                    "💥 Failed to cleanup media streaming session on error",
                    extra={
                        "call_connection_id": call_connection_id,
                        "correlation_id": correlation_id,
                        "cleanup_error": str(e),
                    }
                )
    
    def get_streaming_sessions(self) -> Dict[str, Dict[str, Any]]:
        """
        Get currently active streaming sessions
        
        Returns:
            Dict of call connection IDs to streaming session info
        """
        return self._streaming_sessions.copy()
    
    def get_streaming_session(self, call_connection_id: str) -> Dict[str, Any]:
        """
        Get specific streaming session info
        
        Args:
            call_connection_id: Call connection ID
            
        Returns:
            Dict containing streaming session info
        """
        return self._streaming_sessions.get(call_connection_id, {})
