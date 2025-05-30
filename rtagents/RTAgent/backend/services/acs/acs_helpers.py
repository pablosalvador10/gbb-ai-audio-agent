# --- Recording Initiation Helper ---
"""
acs_helpers.py

This module provides helper functions and utilities for integrating with Azure Communication Services (ACS) in the context of real-time media streaming and WebSocket communication. It includes initialization routines, WebSocket URL construction, message broadcasting, and audio data handling for ACS media streaming scenarios.

"""

import json
from base64 import b64encode
from typing import List, Optional, Dict, Any

from fastapi import WebSocket, WebSocketDisconnect
from fastapi.websockets import WebSocketState
from src.acs.acs_helper import AcsCaller
from rtagents.RTAgent.backend.settings import (
    ACS_RECORDING_CALLBACK_PATH,
    AZURE_STORAGE_ACCOUNT_NAME,
    AZURE_STORAGE_RECORDING_CONTAINER_NAME,
    ACS_CALLBACK_PATH,
    ACS_CONNECTION_STRING,
    ACS_SOURCE_PHONE_NUMBER,
    ACS_WEBSOCKET_PATH,
    BASE_URL,
)
from utils.ml_logging import get_logger

# --- Init Logger ---
logger = get_logger()


def initialize_acs_caller_instance() -> Optional[AcsCaller]:
    """Initializes and returns the ACS Caller instance if configured, otherwise None."""
    if not all([ACS_CONNECTION_STRING, ACS_SOURCE_PHONE_NUMBER, BASE_URL]):
        logger.warning(
            "ACS environment variables not fully configured. ACS calling disabled."
        )
        return None

    acs_callback_url = f"{BASE_URL.strip('/')}{ACS_CALLBACK_PATH}"
    acs_websocket_url = construct_websocket_url(BASE_URL, ACS_WEBSOCKET_PATH)

    if not acs_websocket_url:
        logger.error(
            "Could not construct valid ACS WebSocket URL. ACS calling disabled."
        )
        return None

    logger.info("Attempting to initialize AcsCaller...")
    logger.info(f"ACS Callback URL: {acs_callback_url}")
    logger.info(f"ACS WebSocket URL: {acs_websocket_url}")

    try:
        caller_instance = AcsCaller(
            source_number=ACS_SOURCE_PHONE_NUMBER,
            acs_connection_string=ACS_CONNECTION_STRING,
            acs_callback_path=acs_callback_url,
            acs_media_streaming_websocket_path=acs_websocket_url,
        )
        logger.info("AcsCaller initialized successfully.")
        return caller_instance
    except Exception as e:
        logger.error(f"Failed to initialize AcsCaller: {e}", exc_info=True)
        return None


# --- Helper Functions for Initialization ---
def construct_websocket_url(base_url: str, path: str) -> Optional[str]:
    """Constructs a WebSocket URL from a base URL and path."""
    if not base_url:  # Added check for empty base_url
        logger.error("BASE_URL is empty or not provided.")
        return None
    if "<your" in base_url:  # Added check for placeholder
        logger.warning(
            "BASE_URL contains placeholder. Please update environment variable."
        )
        return None

    base_url_clean = base_url.strip("/")
    path_clean = path.strip("/")

    if base_url.startswith("https://"):
        base_url_clean = base_url.replace("https://", "").strip("/")
        return f"wss://{base_url_clean}/{path_clean}"
    elif base_url.startswith("http://"):
        base_url_clean = base_url.replace("http://", "").strip("/")
        return f"ws://{base_url_clean}/{path_clean}"
    else:
        logger.error(
            f"Cannot determine WebSocket protocol (wss/ws) from BASE_URL: {base_url}"
        )
        return None

async def broadcast_message(
    connected_clients: List[WebSocket], message: str, sender: str = "system"
):
    """
    Send a message to all connected WebSocket clients without duplicates.

    Parameters:
    - message (str): The message to broadcast.
    - sender (str): Indicates the sender of the message. Can be 'agent', 'user', or 'system'.
    """
    sent_clients = set()  # Track clients that have already received the message
    payload = {"message": message, "sender": sender}  # Include sender in the payload
    for client in connected_clients:
        if client not in sent_clients:
            try:
                await client.send_text(json.dumps(payload))
                sent_clients.add(client)  # Mark client as sent
            except Exception as e:
                logger.error(f"Failed to send message to a client: {e}")


async def send_pcm_frames(ws: WebSocket, pcm_bytes: bytes, sample_rate: int):
    packet_size = 640 if sample_rate == 16000 else 960
    for i in range(0, len(pcm_bytes), packet_size):
        frame = pcm_bytes[i : i + packet_size]
        # pad last frame
        if len(frame) < packet_size:
            frame += b"\x00" * (packet_size - len(frame))
        b64 = b64encode(frame).decode("ascii")

        payload = {"kind": "AudioData", "audioData": {"data": b64}, "stopAudio": None}
        try:
            await ws.send_text(json.dumps(payload))
        except WebSocketDisconnect:
            logger.warning("WebSocket disconnected while sending PCM frames.")
            break
        except Exception as e:
            logger.error(f"Error while sending PCM frames: {e}")
            break


async def send_data(websocket, buffer):
    if websocket.client_state == WebSocketState.CONNECTED:
        data = {
            "Kind": "AudioData",
            "AudioData": {"data": buffer},
            "StopAudio": None
        }
        # Serialize the server streaming data
        serialized_data = json.dumps(data)
        logger.info(f"Out Streaming Data ---> {serialized_data}")
        # Send the chunk over the WebSocket
        try:
            await websocket.send_json(data)
        except WebSocketDisconnect:
            logger.warning("WebSocket disconnected while sending data.")
        except Exception as e:
            logger.error(f"Error while sending data over WebSocket: {e}")


async def stop_audio(websocket):
    """
    Tells the ACS Media Streaming service to stop accepting incoming audio from client.
    (This does not close the WebSocket; it just pauses the stream.)
    """
    if websocket.client_state.name == "CONNECTED":
        stop_payload = {"Kind": "StopAudio", "AudioData": None, "StopAudio": {}}
        await websocket.send_json(stop_payload)
        logger.info("🛑 Sent StopAudio command to ACS WebSocket.")


async def resume_audio(websocket):
    """
    Tells the ACS Media Streaming service to resume accepting incoming audio from client.
    (This resumes the stream without needing to reconnect.)
    """
    if websocket.client_state.name == "CONNECTED":
        start_payload = {"Kind": "StartAudio", "AudioData": None, "StartAudio": {}}
        await websocket.send_json(start_payload)
        logger.info("🎙️ Sent StartAudio command to ACS WebSocket.")



async def initiate_acs_call_recording(
    call_connection_id: str,
    participants: Optional[List[Dict[str, Any]]] = None,
    call_service: Optional[AcsCaller] = None
) -> Dict[str, Any]:
    """
    Initiate ACS call recording using the enhanced session manager
    Following Azure security, compliance, and operational excellence patterns
    """
    try:
        if not participants:
            logger.error(
                f"❌ No participants found for recording initiation",
                extra={"call_connection_id": call_connection_id}
            )
            return {
                "success": False,
                "error": "No participants available for recording",
                "recording_id": None
            }
        
        # Get recording configuration from settings with Azure Key Vault integration
        
        storage_account_name = AZURE_STORAGE_ACCOUNT_NAME
        
        if not storage_account_name:
            logger.error(
                f"❌ Storage account name not configured",
                extra={"call_connection_id": call_connection_id}
            )
            return {
                "success": False,
                "error": "Storage account not configured",
                "recording_id": None
            }
        
        # Initialize ACS caller with Managed Identity
        call_service
        
        # Start recording with comprehensive error handling
        recording_result = await call_service.start_recording_for_participants(
            call_connection_id=call_connection_id,
            recording_callback_url=f"{BASE_URL.strip('/')}{ACS_RECORDING_CALLBACK_PATH}",
            participants=participants,
            storage_account_name=storage_account_name,
            recording_container=AZURE_STORAGE_RECORDING_CONTAINER_NAME
        )
        
        if recording_result.get("success"):
            recording_id = recording_result.get("recording_id")
            
            logger.info(
                f"✅ Recording started successfully",
                extra={
                    "call_connection_id": call_connection_id,
                    "recording_id": recording_id,
                    "participants_count": len(participants),
                    "storage_account": storage_account_name
                }
            )
            
            return {
                "success": True,
                "recording_id": recording_id,
                "participants_count": len(participants),
                "storage_account": storage_account_name
            }
        else:
            error_message = recording_result.get("error", "Unknown recording error")
            
            logger.error(
                f"❌ Recording failed",
                extra={
                    "call_connection_id": call_connection_id,
                    "error": error_message,
                    "participants_count": len(participants)
                }
            )
            
            return {
                "success": False,
                "error": error_message,
                "recording_id": None
            }
            
    except Exception as e:
        logger.error(
            f"❌ Exception in recording initiation",
            extra={
                "call_connection_id": call_connection_id,
                "error": str(e)
            },
            exc_info=True
        )
        
        return {
            "success": False,
            "error": str(e),
            "recording_id": None
        }