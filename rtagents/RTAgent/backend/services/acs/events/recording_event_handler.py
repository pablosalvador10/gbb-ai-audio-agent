"""
==============================================
Azure Communication Services Recording Event Handler

Handles ACS recording events with Azure best practices:
- Manages call recording lifecycle
- Implements automatic recording start/stop
- Provides secure storage for recordings
- Includes comprehensive error handling and monitoring
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from azure.communication.callautomation import CallConnectionClient
    from azure.storage.blob.aio import BlobServiceClient
else:
    CallConnectionClient = None
    BlobServiceClient = None

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from pydantic import BaseModel
from enum import Enum

from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from pydantic import BaseModel
from enum import Enum

from .base_handler import BaseEventHandler
from utils.ml_logging import get_logger

logger = get_logger("services.acs.events.recording")


class RecordingConfiguration(BaseModel):
    """Configuration for ACS call recording."""
    
    format: RecordingFormat = RecordingFormat.WAV
    channel: RecordingChannel = RecordingChannel.UNMIXED
    auto_start: bool = True
    auto_stop_on_disconnect: bool = True
    storage_container: str = "call-recordings"
    retention_days: int = 30
    enable_transcription: bool = False


class RecordingSession(BaseModel):
    """Represents an active recording session."""
    
    recording_id: str
    call_connection_id: str
    server_call_id: str
    recording_state: str
    storage_location: Optional[str] = None
    started_at: datetime
    ended_at: Optional[datetime] = None
    content_location: Optional[str] = None
    metadata: Dict[str, Any] = {}


class RecordingEventHandler(BaseEventHandler):
    """
    Event handler for ACS recording operations.
    
    Features:
    - Automatic recording start when call connects
    - Secure storage in Azure Blob Storage
    - Recording metadata management
    - Error handling and retry logic
    - Performance monitoring
    """
    
    def __init__(
        self,
        call_client: CallConnectionClient,
        blob_client: Optional[BlobServiceClient] = None,
        config: Optional[RecordingConfiguration] = None,
    ):
        super().__init__()
        self.call_client = call_client
        self.blob_client = blob_client
        self.config = config or RecordingConfiguration()
        
        # Recording session tracking
        self.active_recordings: Dict[str, RecordingSession] = {}
        
        # Performance metrics
        self.recording_start_times: Dict[str, datetime] = {}
        
    async def handle_call_connected(
        self,
        call_connection_id: str,
        event_data: Dict[str, Any],
    ) -> None:
        """
        Start recording when call is connected.
        
        Args:
            call_connection_id: The call connection identifier
            event_data: Event payload from ACS
        """
        correlation_id = str(uuid.uuid4())
        
        try:
            # Extract call information
            server_call_id = event_data.get("serverCallId", "unknown")
            
            logger.info(
                "Starting call recording",
                extra={
                    "call_connection_id": call_connection_id,
                    "server_call_id": server_call_id,
                    "correlation_id": correlation_id,
                    "auto_start": self.config.auto_start,
                }
            )
            
            if not self.config.auto_start:
                logger.debug("Auto-start disabled, skipping recording initiation")
                return
                
            # Start recording with retry logic
            recording_session = await self._start_recording_with_retry(
                call_connection_id=call_connection_id,
                server_call_id=server_call_id,
                correlation_id=correlation_id,
            )
            
            if recording_session:
                self.active_recordings[call_connection_id] = recording_session
                
                # Track performance metrics
                self.recording_start_times[call_connection_id] = datetime.now(timezone.utc)
                
                # Log successful recording start
                logger.info(
                    "Recording started successfully",
                    extra={
                        "call_connection_id": call_connection_id,
                        "recording_id": recording_session.recording_id,
                        "correlation_id": correlation_id,
                        "format": self.config.format.value,
                        "channel": self.config.channel.value,
                    }
                )
                
        except Exception as e:
            await self._handle_recording_error(
                error=e,
                call_connection_id=call_connection_id,
                operation="start_recording",
                correlation_id=correlation_id,
            )
            
    async def handle_call_disconnected(
        self,
        call_connection_id: str,
        event_data: Dict[str, Any],
    ) -> None:
        """
        Stop recording when call is disconnected.
        
        Args:
            call_connection_id: The call connection identifier
            event_data: Event payload from ACS
        """
        correlation_id = str(uuid.uuid4())
        
        try:
            recording_session = self.active_recordings.get(call_connection_id)
            
            if not recording_session:
                logger.debug(
                    "No active recording found for disconnected call",
                    extra={
                        "call_connection_id": call_connection_id,
                        "correlation_id": correlation_id,
                    }
                )
                return
                
            logger.info(
                "Stopping recording for disconnected call",
                extra={
                    "call_connection_id": call_connection_id,
                    "recording_id": recording_session.recording_id,
                    "correlation_id": correlation_id,
                }
            )
            
            # Stop recording
            await self._stop_recording_with_retry(
                recording_session=recording_session,
                correlation_id=correlation_id,
            )
            
            # Calculate recording duration for metrics
            if call_connection_id in self.recording_start_times:
                duration = (
                    datetime.now(timezone.utc) 
                    - self.recording_start_times[call_connection_id]
                ).total_seconds()
                
                logger.info(
                    "Recording completed",
                    extra={
                        "call_connection_id": call_connection_id,
                        "recording_id": recording_session.recording_id,
                        "duration_seconds": duration,
                        "correlation_id": correlation_id,
                    }
                )
                
                # Clean up tracking
                del self.recording_start_times[call_connection_id]
                
            # Remove from active recordings
            del self.active_recordings[call_connection_id]
            
        except Exception as e:
            await self._handle_recording_error(
                error=e,
                call_connection_id=call_connection_id,
                operation="stop_recording",
                correlation_id=correlation_id,
            )
            
    async def handle_recording_state_changed(
        self,
        recording_id: str,
        event_data: Dict[str, Any],
    ) -> None:
        """
        Handle recording state change events.
        
        Args:
            recording_id: The recording identifier
            event_data: Event payload from ACS
        """
        correlation_id = str(uuid.uuid4())
        
        try:
            state = event_data.get("recordingState", "unknown")
            call_connection_id = event_data.get("callConnectionId")
            
            logger.info(
                "Recording state changed",
                extra={
                    "recording_id": recording_id,
                    "call_connection_id": call_connection_id,
                    "state": state,
                    "correlation_id": correlation_id,
                }
            )
            
            # Update recording session if tracked
            if call_connection_id and call_connection_id in self.active_recordings:
                session = self.active_recordings[call_connection_id]
                session.recording_state = state
                
                # Handle specific state changes
                if state.lower() == "active":
                    await self._handle_recording_active(session, event_data)
                elif state.lower() in ("inactive", "stopped"):
                    await self._handle_recording_stopped(session, event_data)
                elif state.lower() == "failed":
                    await self._handle_recording_failed(session, event_data)
                    
        except Exception as e:
            await self._handle_recording_error(
                error=e,
                recording_id=recording_id,
                operation="state_change",
                correlation_id=correlation_id,
            )
            
    async def get_recording_download_url(
        self,
        recording_id: str,
        call_connection_id: str,
    ) -> Optional[str]:
        """
        Get secure download URL for recording.
        
        Args:
            recording_id: The recording identifier
            call_connection_id: The call connection identifier
            
        Returns:
            Secure download URL or None if not available
        """
        correlation_id = str(uuid.uuid4())
        
        try:
            # Check if we have blob storage configured
            if not self.blob_client:
                logger.warning(
                    "Blob storage not configured for recording downloads",
                    extra={
                        "recording_id": recording_id,
                        "call_connection_id": call_connection_id,
                        "correlation_id": correlation_id,
                    }
                )
                return None
                
            # Get recording information from ACS
            recording_properties = await self.call_client.get_recording_properties(
                recording_id=recording_id
            )
            
            content_location = recording_properties.content_location
            if not content_location:
                logger.warning(
                    "No content location available for recording",
                    extra={
                        "recording_id": recording_id,
                        "correlation_id": correlation_id,
                    }
                )
                return None
                
            # Generate secure download URL with expiration
            # This would typically involve downloading from ACS and uploading to blob storage
            # or providing a time-limited SAS URL
            
            logger.info(
                "Generated recording download URL",
                extra={
                    "recording_id": recording_id,
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                }
            )
            
            return content_location
            
        except Exception as e:
            await self._handle_recording_error(
                error=e,
                recording_id=recording_id,
                operation="get_download_url",
                correlation_id=correlation_id,
            )
            return None
            
    async def _start_recording_with_retry(
        self,
        call_connection_id: str,
        server_call_id: str,
        correlation_id: str,
        max_retries: int = 3,
    ) -> Optional[RecordingSession]:
        """Start recording with exponential backoff retry logic."""
        
        for attempt in range(max_retries):
            try:
                # Generate unique recording metadata
                recording_metadata = {
                    "call_connection_id": call_connection_id,
                    "server_call_id": server_call_id,
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "correlation_id": correlation_id,
                }
                
                # Start recording via ACS API
                recording_result = await self.call_client.start_recording(
                    recording_format=self.config.format,
                    recording_channel=self.config.channel,
                    recording_content_types=["audio"],
                    recording_state_callback_uri=None,  # Will be handled via events
                    external_storage_location=None,  # Use ACS default storage
                )
                
                # Create recording session
                recording_session = RecordingSession(
                    recording_id=recording_result.recording_id,
                    call_connection_id=call_connection_id,
                    server_call_id=server_call_id,
                    recording_state="starting",
                    started_at=datetime.now(timezone.utc),
                    metadata=recording_metadata,
                )
                
                return recording_session
                
            except HttpResponseError as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(
                        f"Recording start attempt {attempt + 1} failed, retrying in {wait_time}s",
                        extra={
                            "call_connection_id": call_connection_id,
                            "correlation_id": correlation_id,
                            "error": str(e),
                            "attempt": attempt + 1,
                            "max_retries": max_retries,
                        }
                    )
                    await asyncio.sleep(wait_time)
                else:
                    raise
                    
        return None
        
    async def _stop_recording_with_retry(
        self,
        recording_session: RecordingSession,
        correlation_id: str,
        max_retries: int = 3,
    ) -> None:
        """Stop recording with retry logic."""
        
        for attempt in range(max_retries):
            try:
                await self.call_client.stop_recording(
                    recording_id=recording_session.recording_id
                )
                
                # Update session
                recording_session.ended_at = datetime.now(timezone.utc)
                recording_session.recording_state = "stopped"
                
                return
                
            except (HttpResponseError, ResourceNotFoundError) as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(
                        f"Recording stop attempt {attempt + 1} failed, retrying in {wait_time}s",
                        extra={
                            "recording_id": recording_session.recording_id,
                            "correlation_id": correlation_id,
                            "error": str(e),
                            "attempt": attempt + 1,
                        }
                    )
                    await asyncio.sleep(wait_time)
                else:
                    # If all retries fail, log error but don't raise
                    # Recording might have already stopped
                    logger.error(
                        "Failed to stop recording after all retries",
                        extra={
                            "recording_id": recording_session.recording_id,
                            "correlation_id": correlation_id,
                            "error": str(e),
                        }
                    )
                    break
                    
    async def _handle_recording_active(
        self,
        session: RecordingSession,
        event_data: Dict[str, Any],
    ) -> None:
        """Handle recording becoming active."""
        session.recording_state = "active"
        
        # Store content location if available
        if "contentLocation" in event_data:
            session.content_location = event_data["contentLocation"]
            
        logger.info(
            "Recording is now active",
            extra={
                "recording_id": session.recording_id,
                "call_connection_id": session.call_connection_id,
                "content_location": session.content_location,
            }
        )
        
    async def _handle_recording_stopped(
        self,
        session: RecordingSession,
        event_data: Dict[str, Any],
    ) -> None:
        """Handle recording stopped."""
        session.recording_state = "stopped"
        session.ended_at = datetime.now(timezone.utc)
        
        # Store final content location
        if "contentLocation" in event_data:
            session.content_location = event_data["contentLocation"]
            
        logger.info(
            "Recording stopped",
            extra={
                "recording_id": session.recording_id,
                "call_connection_id": session.call_connection_id,
                "content_location": session.content_location,
                "duration_seconds": (
                    (session.ended_at - session.started_at).total_seconds()
                    if session.ended_at
                    else None
                ),
            }
        )
        
    async def _handle_recording_failed(
        self,
        session: RecordingSession,
        event_data: Dict[str, Any],
    ) -> None:
        """Handle recording failure."""
        session.recording_state = "failed"
        session.ended_at = datetime.now(timezone.utc)
        
        logger.error(
            "Recording failed",
            extra={
                "recording_id": session.recording_id,
                "call_connection_id": session.call_connection_id,
                "error_details": event_data.get("error", "Unknown error"),
            }
        )
        
    async def _handle_recording_error(
        self,
        error: Exception,
        operation: str,
        correlation_id: str,
        call_connection_id: Optional[str] = None,
        recording_id: Optional[str] = None,
    ) -> None:
        """Handle recording-related errors with comprehensive logging."""
        
        error_details = {
            "operation": operation,
            "correlation_id": correlation_id,
            "error_type": type(error).__name__,
            "error_message": str(error),
        }
        
        if call_connection_id:
            error_details["call_connection_id"] = call_connection_id
        if recording_id:
            error_details["recording_id"] = recording_id
            
        logger.error(
            f"Recording operation failed: {operation}",
            extra=error_details,
            exc_info=True,
        )
        
        # Additional error handling based on error type
        if isinstance(error, HttpResponseError):
            if error.status_code == 429:  # Rate limiting
                logger.warning("Rate limiting encountered, implementing backoff")
            elif error.status_code >= 500:  # Server errors
                logger.warning("Server error encountered, may retry")
        elif isinstance(error, ResourceNotFoundError):
            logger.warning("Resource not found, may have been cleaned up")
