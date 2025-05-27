

"""
ACS Helper Module

This module provides helpers for working with Azure Communication Services.

# Recording Calls with Azure Communication Services

## Container URL Format

The `AzureBlobContainerRecordingStorage` class requires a container URL with a 
Shared Access Signature (SAS) token, not a connection string. The URL format is:

```
https://<storage-account-name>.blob.core.windows.net/<container-name>?<sas-token>
```

## Required Container Permissions

The SAS token must include the following permissions:
- Read (r)
- Add (a)
- Create (c) 
- Write (w)
- Delete (d)
- List (l)

## Usage Example

```python
# Generate a container URL with SAS token
container_url = acs_caller.generate_container_sas_url(
    account_name="yourstorageaccount",
    container_name="recordings",
    account_key="your_storage_account_key",
    expiry_hours=24
)

# Start recording a call
await acs_caller.start_recording(
    call_id="your_call_id",
    callback_url="https://yourapp.com/recording/callbacks", 
    container_url=container_url
)
```

## Security Best Practices

1. Generate new SAS tokens periodically
2. Set expiry times to the minimum required
3. Use container-level SAS tokens, not account-level
4. Configure storage account network rules to restrict access
5. Enable Azure Storage logging to track access
"""

import logging

from aiohttp import web
from azure.communication.callautomation import (
    AudioFormat,
    CallAutomationClient,
    CallInvite,
    MediaStreamingAudioChannelType,
    MediaStreamingContentType,
    MediaStreamingOptions,
    MediaStreamingTransportType,
    AzureBlobContainerRecordingStorage,
    PhoneNumberIdentifier,
    RecordingChannel,
    RecordingContent, 
    RecordingFormat,
)
from azure.core.exceptions import HttpResponseError
from azure.core.messaging import CloudEvent
from src.blob.blob_helper import save_transcript_to_blob

from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class AcsCaller:
    source_number: str
    acs_connection_string: str
    acs_callback_path: str
    websocket_url: str
    media_streaming_configuration: MediaStreamingOptions
    call_automation_client: CallAutomationClient

    def __init__(
        self,
        source_number: str,
        acs_connection_string: str,
        acs_callback_path: str,
        acs_media_streaming_websocket_path: str,
        media_streaming_configuration: MediaStreamingOptions = None,
        # tts_translator: SpeechCoreTranslator
    ):
        self.source_number = source_number
        self.acs_connection_string = acs_connection_string
        self.acs_callback_path = acs_callback_path  # Should be the full URL
        self.websocket_url = (
            acs_media_streaming_websocket_path  # Should be the full wss:// URL
        )
        logger.info(
            f"AcsCaller initialized. Callback URL: {self.acs_callback_path}, WebSocket URL: {self.websocket_url}"
        )
        if media_streaming_configuration is None:
            self.media_streaming_configuration = MediaStreamingOptions(
                transport_url=self.websocket_url,  # Use the full websocket URL
                transport_type=MediaStreamingTransportType.WEBSOCKET,
                content_type=MediaStreamingContentType.AUDIO,
                audio_channel_type=MediaStreamingAudioChannelType.UNMIXED,
                start_media_streaming=True,
                enable_bidirectional=True,
                audio_format=AudioFormat.PCM16_K_MONO,  # Ensure this matches what your STT expects
            )
        else:
            self.media_streaming_configuration = media_streaming_configuration    # Initialize CallAutomationClient here to reuse it
        try:
            self.call_automation_client = CallAutomationClient.from_connection_string(
                self.acs_connection_string
            )
            logger.info("CallAutomationClient initialized successfully.")
        except Exception as e:
            logger.error(
                f"Failed to initialize CallAutomationClient: {e}", exc_info=True
            )
            self.call_automation_client = None  # Ensure it's None if init fails

    async def initiate_call(self, target_number: str):
        if not self.call_automation_client:
            logger.error("CallAutomationClient not initialized. Cannot initiate call.")
            raise RuntimeError(
                "CallAutomationClient failed to initialize."
            )  # Or handle appropriately

        try:
            # Ensure target and source are correctly formatted identifiers
            self.target_participant = PhoneNumberIdentifier(target_number)
            self.source_caller = PhoneNumberIdentifier(self.source_number)

            # Log the exact parameters being used for the call
            logger.info(f"Initiating call to: {target_number}")
            logger.info(f"Source phone number: {self.source_number}")
            logger.info(f"Callback URI for ACS events: {self.acs_callback_path}")
            logger.info(f"Media Streaming WebSocket URI: {self.websocket_url}")
            logger.info(
                f"Media Streaming Configuration: {self.media_streaming_configuration}"
            )

            CallInvite(
                target=self.target_participant,
                source_caller_id_number=self.source_caller,
            )
            response = self.call_automation_client.create_call(
                target_participant=self.target_participant,
                callback_url=self.acs_callback_path,  # Pass the full callback URL
                media_streaming=self.media_streaming_configuration,
                source_caller_id_number=self.source_caller,
            )
            # Note: create_call is sync. Response contains call_connection_properties like callConnectionId if successful immediately,
            # but the actual connection state comes via callbacks.
            call_connection_id = response.call_connection_id
            logger.info(
                f"create_call request sent successfully. Call Connection ID (initial): {call_connection_id}"
            )
            # Return the result dictionary expected by the FastAPI endpoint in server.py
            return {"status": "created", "call_id": call_connection_id}
        except HttpResponseError as e:
            # Log detailed error information from ACS
            logger.error(
                f"ACS HTTP Error creating call: Status Code={e.status_code}, Reason={e.reason}, Message={e.message}",
                exc_info=True,
            )
            # Consider re-raising or handling specific error codes (e.g., 400 for bad request, 401/403 for auth, 500 for server error)
            raise  # Re-raise the exception to be handled by the caller API endpoint
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during initiate_call: {e}", exc_info=True
            )
            raise  # Re-raise the exception

    async def disconnect_call(self, call_connection_id: str):
        """
        Disconnects the call associated with the given call connection ID.
        """
        if not self.call_automation_client:
            logger.error(
                "CallAutomationClient not initialized. Cannot disconnect call."
            )
            return  # Or raise an error

        try:
            call_connection = self.call_automation_client.get_call_connection(
                call_connection_id
            )
            if call_connection:
                logger.info(
                    f"Attempting to hang up call with connection ID: {call_connection_id}"
                )
                await call_connection.hang_up()  # Hang up for all participants
                logger.info(
                    f"Hang up request sent for call connection ID: {call_connection_id}"
                )
            else:
                logger.warning(
                    f"Could not find call connection object for ID: {call_connection_id}. Cannot hang up."
                )
        except HttpResponseError as e:
            logger.error(
                f"ACS HTTP Error hanging up call {call_connection_id}: Status Code={e.status_code}, Reason={e.reason}, Message={e.message}",
                exc_info=True,
            )
            # Consider specific handling based on status code if needed
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during disconnect_call for {call_connection_id}: {e}",
                exc_info=True,
            )
            # Decide if re-raising is appropriate depending on how this method is called

    async def outbound_call_handler(self, request):
        cloudevent = await request.json()
        handled_events = {}
        for event_dict in cloudevent:
            try:
                event = CloudEvent.from_dict(event_dict)
                if event.data is None or "callConnectionId" not in event.data:
                    logger.warning(
                        f"Received event without data or callConnectionId: {event_dict}"
                    )
                    continue

                call_connection_id = event.data["callConnectionId"]
                logger.info(
                    f"Processing event type: {event.type} for call connection id: {call_connection_id}"
                )

                # Store the event dictionary under its call connection ID
                handled_events.setdefault(call_connection_id, []).append(event_dict)

                # Existing event handling logic (logging)
                if event.type == "Microsoft.Communication.CallConnected":
                    logger.info(
                        f"Call connected event received for call connection id: {call_connection_id}"
                    )
                elif event.type == "Microsoft.Communication.ParticipantsUpdated":
                    logger.info(
                        f"Participants updated event received for call connection id: {call_connection_id}"
                    )
                elif event.type == "Microsoft.Communication.CallDisconnected":
                    logger.info(
                        f"Call disconnect event received for call connection id: {call_connection_id}"
                    )
                # Add handling for other relevant events like PlayAudioResult, RecognizeCompleted, etc. if needed
                else:
                    logger.info(
                        f"Unhandled event type: {event.type} for call connection id: {call_connection_id}"
                    )

            except Exception as e:
                logger.error(
                    f"Error processing event: {event_dict}. Error: {e}", exc_info=True
                )
            # Decide if you want to continue processing other events or stop

        # Return the dictionary of handled events along with a status
        return web.json_response(
            {"status": "events processed", "handled_events": handled_events}, status=200
        )


    def get_call_connection(self, call_connection_id: str):
        """
        Retrieve the call connection details using the call connection ID.
        
        Parameters:
        -----------
        call_connection_id: str
            The ID of the call connection to retrieve
            
        Returns:
        --------
        CallConnectionClient or None
            The call connection client if found, None otherwise
        
        Raises:
        -------
        HttpResponseError
            If there's an authentication or permission issue with the call (includes error code 8527)
        """
        if not call_connection_id:
            logger.error("Cannot get call connection: call_connection_id is empty or None")
            return None
            
        if not self.call_automation_client:
            logger.error("CallAutomationClient not initialized. Cannot get call connection.")
            return None
            
        try:
            call_connection = self.call_automation_client.get_call_connection(call_connection_id)
            if call_connection:
                logger.info(f"Successfully retrieved call connection for ID: {call_connection_id}")
            else:
                logger.warning(f"No call connection found for ID: {call_connection_id}")
            return call_connection
        except HttpResponseError as e:
            # Extract useful error details for ACS-specific errors
            logger.error(f"ACS HTTP Error getting call connection: Status={e.status_code}, Message={e.message}", exc_info=True)
            if hasattr(e, 'error') and hasattr(e.error, 'code') and e.error.code == '8527':
                logger.error("Invalid join identity error (8527): Cannot join call with the provided identity")
            raise  # Re-raise to allow specific handling by the caller
        except Exception as e:
            logger.error(f"Error retrieving call connection: {e}", exc_info=True)
            return None


    async def start_transcript(self, call_id: str):
        """
        Starts a transcript for the given call_id.
        This could be a placeholder for any logic needed to mark the beginning of a transcript.
        """
        logger.info(f"Transcript started for call_id: {call_id}, however, actual implementation is not provided.")

    
    
    async def end_transcript(self, call_id: str, transcript: str):
        """
        Ends the transcript for the given call_id and saves it to Azure Blob Storage.
        """
        logger.info(f"Ending transcript for call_id: {call_id}")
        try:
            await save_transcript_to_blob(call_id, transcript)
            logger.info(f"Transcript for call_id {call_id} saved to blob storage.")
        except Exception as e:
            logger.error(f"Failed to save transcript for call_id {call_id}: {e}", exc_info=True)

    async def start_recording_for_participants(
        self,
        server_call_id: str,
        participants: List[Dict[str, Any]],
        recording_callback_url: str,
        storage_account_name: str,
        recording_container: str,
        minimum_participants: int = 2
    ) -> Dict[str, Any]:
        """
        🎬 Start recording for a call when sufficient participants are present
        
        Following Azure best practices for ACS recording management:
        - Validates participant count before attempting recording
        - Uses unmixed audio channels for better quality
        - Handles authentication and storage configuration errors
        - Returns structured response for proper error handling
        
        Args:
            server_call_id: The ACS server call identifier
            participants: List of participant data from ACS event
            recording_callback_url: Full URL for recording state callbacks
            storage_account_name: Azure Storage account name for recordings
            recording_container: Blob container name for storing recordings
            minimum_participants: Minimum participants required to start recording (default: 2)
            
        Returns:
            Dict containing recording result with success status and details
            
        Raises:
            ValueError: If required configuration is missing or invalid
            HttpResponseError: If ACS recording API call fails
        """
        logger.info(f"🎬 Starting recording process for call {server_call_id}")
        
        # Validate inputs following Azure best practices
        if not server_call_id:
            raise ValueError("server_call_id cannot be empty")
        if not storage_account_name:
            raise ValueError("storage_account_name is required for recording")
        if not recording_container:
            raise ValueError("recording_container is required for recording")
        if not recording_callback_url:
            raise ValueError("recording_callback_url is required for recording")
            
        # Extract and validate participant identifiers
        participant_ids = []
        for participant in participants:
            identifier = participant.get("identifier", {})
            raw_id = identifier.get("rawId")
            if raw_id:
                participant_ids.append(raw_id)
                logger.info(f"👤 Found participant: {raw_id}")
        
        logger.info(f"📊 Total participants for call {server_call_id}: {len(participant_ids)}")
        
        # Check minimum participant requirement
        if len(participant_ids) < minimum_participants:
            return {
                "success": False,
                "reason": "insufficient_participants",
                "participant_count": len(participant_ids),
                "minimum_required": minimum_participants,
                "message": f"Recording requires at least {minimum_participants} participants, found {len(participant_ids)}"
            }
        
        try:
            # Construct container URL following Azure Storage conventions
            container_url = f"https://{storage_account_name}.blob.core.windows.net/{recording_container}"
            logger.info(f"🗂️ Using storage container: {container_url}")
            
            # Create recording storage configuration
            recording_storage = AzureBlobContainerRecordingStorage(
                container_url=container_url
            )
            
            # Start recording with optimal configuration for voice AI
            logger.info(f"🎙️ Initiating recording for call {server_call_id}")
            recording_response = self.call_automation_client.start_recording(
                server_call_id=server_call_id,
                recording_state_callback_url=recording_callback_url,
                recording_content_type=RecordingContent.Audio,
                recording_channel_type=RecordingChannel.Unmixed,  # Better for AI processing
                recording_format_type=RecordingFormat.Wav,         # Optimal for speech processing
                recording_storage=recording_storage
            )
            
            logger.info(f"✅ Recording started successfully for call {server_call_id}")
            
            return {
                "success": True,
                "recording_id": getattr(recording_response, 'recording_id', None),
                "server_call_id": server_call_id,
                "participant_count": len(participant_ids),
                "storage_location": container_url,
                "recording_format": "wav",
                "channel_type": "unmixed",
                "message": "Recording started successfully"
            }
            
        except HttpResponseError as e:
            # Handle ACS-specific authentication and permission errors
            error_code = getattr(e.error, 'code', 'unknown') if hasattr(e, 'error') else 'unknown'
            
            if "8527" in str(e) or "Invalid join identity" in str(e):
                logger.error(f"❌ ACS authentication error for call {server_call_id}: {e.message}")
                logger.error("💡 This typically indicates missing Azure Storage permissions")
                logger.error("💡 Ensure ACS has access to the storage account and container")
                
                return {
                    "success": False,
                    "reason": "authentication_error",
                    "error_code": error_code,
                    "message": "Authentication error - check ACS storage permissions",
                    "troubleshooting": "Verify Azure Storage permissions for ACS service"
                }
            else:
                logger.error(f"❌ ACS HTTP error starting recording: {e.status_code} - {e.message}")
                
                return {
                    "success": False,
                    "reason": "acs_api_error",
                    "error_code": error_code,
                    "status_code": e.status_code,
                    "message": f"ACS API error: {e.message}"
                }
                
        except Exception as e:
            logger.error(f"❌ Unexpected error starting recording for call {server_call_id}: {e}", exc_info=True)
            
            return {
                "success": False,
                "reason": "unexpected_error",
                "message": f"Unexpected error: {str(e)}",
                "server_call_id": server_call_id
            }

    async def stop_recording(
        self,
        server_call_id: str,
        recording_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        🛑 Stop recording for a call
        
        Args:
            server_call_id: The ACS server call identifier
            recording_id: Optional recording identifier for specific recording
            
        Returns:
            Dict containing stop recording result
        """
        logger.info(f"🛑 Stopping recording for call {server_call_id}")
        
        try:
            if not self.call_automation_client:
                raise RuntimeError("CallAutomationClient not initialized")
            
            # Stop the recording
            stop_response = self.call_automation_client.stop_recording(
                server_call_id=server_call_id
            )
            
            logger.info(f"✅ Recording stopped successfully for call {server_call_id}")
            
            return {
                "success": True,
                "server_call_id": server_call_id,
                "recording_id": recording_id,
                "message": "Recording stopped successfully"
            }
            
        except HttpResponseError as e:
            logger.error(f"❌ ACS error stopping recording: {e.status_code} - {e.message}")
            return {
                "success": False,
                "reason": "acs_api_error",
                "status_code": e.status_code,
                "message": f"Failed to stop recording: {e.message}"
            }
        except Exception as e:
            logger.error(f"❌ Unexpected error stopping recording: {e}", exc_info=True)
            return {
                "success": False,
                "reason": "unexpected_error",
                "message": f"Unexpected error: {str(e)}"
            }

    async def get_recording_properties(
        self,
        server_call_id: str
    ) -> Dict[str, Any]:
        """
        📋 Get recording properties for a call
        
        Args:
            server_call_id: The ACS server call identifier
            
        Returns:
            Dict containing recording properties and status
        """
        logger.info(f"📋 Getting recording properties for call {server_call_id}")
        
        try:
            if not self.call_automation_client:
                raise RuntimeError("CallAutomationClient not initialized")
            
            # Get recording properties
            properties = self.call_automation_client.get_recording_properties(
                server_call_id=server_call_id
            )
            
            return {
                "success": True,
                "server_call_id": server_call_id,
                "recording_state": getattr(properties, 'recording_state', 'unknown'),
                "recording_id": getattr(properties, 'recording_id', None),
                "properties": properties
            }
            
        except HttpResponseError as e:
            logger.error(f"❌ ACS error getting recording properties: {e.message}")
            return {
                "success": False,
                "reason": "acs_api_error",
                "message": f"Failed to get recording properties: {e.message}"
            }
        except Exception as e:
            logger.error(f"❌ Error getting recording properties: {e}", exc_info=True)
            return {
                "success": False,
                "reason": "unexpected_error",
                "message": f"Unexpected error: {str(e)}"
            }
