"""
acs_helpers.py

This module provides helper functions and utilities for integrating with Azure Communication Services (ACS) in the context of real-time media streaming and WebSocket communication. It includes initialization routines, WebSocket URL construction, message broadcasting, and audio data handling for ACS media streaming scenarios.

"""

import asyncio
import hashlib
import json
from base64 import b64encode
from typing import List, Optional


class MediaCancelledException(Exception):
    """Exception raised when media playback is cancelled due to interrupt."""

    pass


from azure.communication.callautomation import SsmlSource, TextSource
from azure.core.exceptions import HttpResponseError
from fastapi import WebSocket
from fastapi.websockets import WebSocketDisconnect, WebSocketState
from websockets.exceptions import ConnectionClosedError

from apps.rtagent.backend.settings import (
    ACS_CALL_CALLBACK_PATH,
    ACS_CONNECTION_STRING,
    ACS_SOURCE_PHONE_NUMBER,
    ACS_WEBSOCKET_PATH,
    AZURE_SPEECH_ENDPOINT,
    AZURE_STORAGE_CONTAINER_URL,
    BASE_URL,
    GREETING_VOICE_TTS,
)
from src.acs.acs_helper import AcsCaller
from utils.ml_logging import get_logger

# --- Init Logger ---
logger = get_logger()


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
        # Remove the https:// prefix before constructing wss://
        base_url_clean = base_url.replace("https://", "").strip("/")
        ws_url = f"wss://{base_url_clean}/{path_clean}"
        logger.info(f"🔗 Constructed WebSocket URL: {ws_url}")
        return ws_url
    elif base_url.startswith("http://"):
        logger.warning(
            "BASE_URL starts with http://. ACS Media Streaming usually requires wss://."
        )
        # Remove the http:// prefix before constructing ws://
        base_url_clean = base_url.replace("http://", "").strip("/")
        ws_url = f"ws://{base_url_clean}/{path_clean}"
        logger.info(f"🔗 Constructed WebSocket URL: {ws_url}")
        return ws_url
    else:
        logger.error(
            f"Cannot determine WebSocket protocol (wss/ws) from BASE_URL: {base_url}"
        )
        return None


def initialize_acs_caller_instance() -> Optional[AcsCaller]:
    """Initializes and returns the ACS Caller instance if configured, otherwise None."""
    if not all([ACS_CONNECTION_STRING, ACS_SOURCE_PHONE_NUMBER, BASE_URL]):
        logger.warning(
            "ACS environment variables not fully configured. ACS calling disabled."
        )
        return None

    acs_callback_url = f"{BASE_URL.strip('/')}{ACS_CALL_CALLBACK_PATH}"
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
            callback_url=acs_callback_url,
            websocket_url=acs_websocket_url,
            acs_connection_string=ACS_CONNECTION_STRING,
            cognitive_services_endpoint=AZURE_SPEECH_ENDPOINT,
            recording_storage_container_url=AZURE_STORAGE_CONTAINER_URL,
        )
        logger.info("AcsCaller initialized successfully.")
        return caller_instance
    except Exception as e:
        logger.error(f"Failed to initialize AcsCaller: {e}", exc_info=True)
        return None


# --- Helper Functions for WebSocket and Media Operations ---
async def broadcast_message(
    connected_clients: List[WebSocket], message: str, sender: str = "system"
):
    """
    Send a message to all connected WebSocket clients without duplicates.

    Uses message deduplication based on message content and sender to prevent
    the same message from being sent multiple times to the same clients.

    Parameters:
    - connected_clients (List[WebSocket]): List of connected WebSocket clients
    - message (str): The message to broadcast.
    - sender (str): Indicates the sender of the message. Can be 'Assistant', 'User', or 'System'.
    """
    if not connected_clients or not message.strip():
        return

    # Create a message hash for deduplication
    message_hash = hashlib.md5(f"{sender}:{message.strip()}".encode()).hexdigest()

    # Store recent message hashes to prevent duplicates (using a simple in-memory cache)
    if not hasattr(broadcast_message, "_recent_messages"):
        broadcast_message._recent_messages = {}

    # Clean old entries (keep only last 100 messages)
    if len(broadcast_message._recent_messages) > 100:
        # Remove oldest 50 entries
        old_keys = list(broadcast_message._recent_messages.keys())[:50]
        for key in old_keys:
            del broadcast_message._recent_messages[key]

    # Check if this exact message was recently broadcasted
    import time

    current_time = time.time()
    if message_hash in broadcast_message._recent_messages:
        last_sent = broadcast_message._recent_messages[message_hash]
        # If the same message was sent within the last 2 seconds, skip it
        if current_time - last_sent < 2.0:
            logger.debug(
                f"Skipping duplicate broadcast message: {sender}: {message[:50]}..."
            )
            return

    # Mark this message as sent
    broadcast_message._recent_messages[message_hash] = current_time

    payload = {"message": message.strip(), "sender": sender}
    sent_count = 0
    failed_count = 0

    for client in connected_clients:
        try:
            if client.client_state == WebSocketState.CONNECTED:
                await client.send_text(json.dumps(payload))
                sent_count += 1
            else:
                logger.debug(f"Skipping disconnected client in broadcast")
        except Exception as e:
            failed_count += 1
            logger.error(f"Failed to send broadcast message to client: {e}")

    logger.debug(
        f"Broadcasted message to {sent_count} clients (failed: {failed_count}): {sender}: {message[:50]}..."
    )


# async def send_pcm_frames(ws: WebSocket, pcm_bytes: list, sample_rate: int):

#     packet_size = 640 if sample_rate == 16000 else 960
#     for i in range(0, len(pcm_bytes), packet_size):
#         frame = pcm_bytes[i : i + packet_size]
#         # pad last frame
#         if len(frame) < packet_size:
#             frame += b"\x00" * (packet_size - len(frame))
#         b64 = b64encode(frame).decode("ascii")

#         payload = {"kind": "AudioData", "AudioData": {"data": b64}, "StopAudio": None}
#         await send_data(ws, json.dumps(payload))


async def send_pcm_frames(
    ws: WebSocket,
    b64_frames: list[str],
    # redis,
    # call_id: str
):
    try:
        import sys

        for b64 in b64_frames:
            # interrupt_flag = await redis.get(f"session:{call_id}:interrupt")
            # if interrupt_flag and interrupt_flag.decode("utf-8") == "true":
            #     logger.info("Voice interruption detected — stopping playback.")
            #     return

            payload = {
                "kind": "AudioData",
                "AudioData": {"data": b64},
                "StopAudio": None,
            }

            await ws.send_json(payload)
            # await asyncio.sleep(0.02)

    except asyncio.CancelledError:
        logger.info("TTS task cancelled")
    except (WebSocketDisconnect, ConnectionClosedError) as e:
        logger.warning(f"WebSocket disconnected during TTS stream: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in send_pcm_frames: {e}", exc_info=True)


async def send_data(websocket, buffer):
    if websocket.client_state == WebSocketState.CONNECTED:
        data = {"Kind": "AudioData", "AudioData": {"data": buffer}, "StopAudio": None}
        # Serialize the server streaming data
        serialized_data = json.dumps(data)
        print(f"Out Streaming Data ---> {serialized_data}")
        # Send the chunk over the WebSocket
        await websocket.send_json(data)


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


async def play_response(
    ws: WebSocket,
    response_text: str,
    use_ssml: bool = False,
    voice_name: str = GREETING_VOICE_TTS,
    locale: str = "en-US",
    participants: list = None,
    max_retries: int = 5,
    initial_backoff: float = 0.5,
):
    """
    Plays `response_text` into the given ACS call, using the SpeechConfig.
    Sets bot_speaking=True at start, False when done or on error.

    :param ws:                 WebSocket connection with app state
    :param response_text:      Plain text or SSML to speak
    :param use_ssml:           If True, wrap in SsmlSource; otherwise TextSource
    :param voice_name:         Valid Azure TTS voice name (default: en-US-JennyNeural)
    :param locale:             Voice locale (default: en-US)
    :param participants:       List of call participants for target identification
    :param max_retries:        Maximum retry attempts for 8500 errors
    :param initial_backoff:    Initial backoff time in seconds
    """
    # 1) Get the call-specific client
    call_connection_id = ws.headers.get("x-ms-call-connection-id")
    acs_caller = ws.app.state.acs_caller
    call_conn = acs_caller.get_call_connection(call_connection_id=call_connection_id)
    cm = ws.app.state.cm

    # If participants is empty or None, try to get target_participant from ws.app.state
    if not participants:
        logger.warning(
            f"No participants provided for call {call_connection_id}. Attempting to use ws.app.state.target_participant."
        )
        target_participant = getattr(ws.app.state, "target_participant", None)
        if target_participant:
            participants = [target_participant]
            logger.info(
                f"Using target_participant from ws.app.state for call {call_connection_id}."
            )
        else:
            logger.error(
                f"No target_participant found in ws.app.state for call {call_connection_id}. Cannot play media."
            )
            return

    if not call_conn:
        logger.error(
            f"Could not get call connection object for {call_connection_id}. Cannot play media."
        )
        return

    # 2) Validate and sanitize response text
    if not response_text or not response_text.strip():
        logger.info(
            f"Skipping media playback for call {call_connection_id} because response_text is empty."
        )
        return  # 3) Set bot_speaking flag at start

    try:
        # Sanitize and prepare the response text
        sanitized_text = response_text.strip().replace("\n", " ").replace("\r", " ")
        sanitized_text = " ".join(sanitized_text.split())

        # Log the sanitized text (first 100 chars) for debugging
        text_preview = (
            sanitized_text[:100] + "..."
            if len(sanitized_text) > 100
            else sanitized_text
        )
        logger.info(f"🔧 Playing text: '{text_preview}'")

        # 4) Build the correct play_source object
        if use_ssml:
            source = SsmlSource(ssml_text=sanitized_text)
            logger.debug(f"Created SsmlSource for call {call_connection_id}")
        else:
            source = TextSource(
                text=sanitized_text, voice_name=voice_name, source_locale=locale
            )
            logger.debug(
                f"Created TextSource for call {call_connection_id} with voice {voice_name}"
            )  # 5) Retry loop for 8500 errors
        for attempt in range(max_retries):
            try:
                # Run the synchronous play_media call in a thread pool to avoid blocking
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: call_conn.play_media(
                        play_source=source,
                        # play_to=participants,
                        interrupt_call_media_operation=True,
                    ),
                )
                logger.info(
                    f"✅ Successfully played media on attempt {attempt + 1} to play response: {sanitized_text}"
                )
                return response

            except HttpResponseError as e:
                # Check for cancellation-related errors that indicate interrupt
                cancellation_indicators = [
                    "cancelled",
                    "disconnected",
                    "call ended",
                    "media cancelled",
                    "operation cancelled",
                    "connection closed",
                ]

                error_message = str(e).lower()
                if any(
                    indicator in error_message for indicator in cancellation_indicators
                ):
                    logger.warning(
                        f"🚫 Media cancellation detected for call {call_connection_id}: {e}"
                    )
                    await cm.set_media_cancelled(True)
                    raise MediaCancelledException(f"Media playback cancelled: {e}")

                # Check for 8500 error code or message indicating media operation is already active
                logger.warning(
                    f"⏳ Media active (8500) error on attempt {attempt + 1} for call {call_connection_id}. "
                )
                if (
                    getattr(e, "status_code", None) == 8500
                    or "already in media operation" in str(e)
                    or "Media operation is already active" in str(e)
                ):
                    if attempt < max_retries - 1:  # Don't wait on the last attempt
                        wait_time = initial_backoff * (2**attempt)
                        logger.warning(
                            f"⏳ Media active (8500) error on attempt {attempt + 1} for call {call_connection_id}. "
                            f"Retrying after {wait_time:.1f}s..."
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(
                            f"🚨 Failed to play media after {max_retries} retries for call {call_connection_id}"
                        )
                        raise RuntimeError(
                            f"Failed to play media after {max_retries} retries for call {call_connection_id}"
                        )
                else:
                    logger.error(f"❌ Unexpected ACS error during play_media: {e}")
                    raise
            except Exception as e:
                logger.error(f"❌ Unexpected exception during play_media: {e}")
                raise
        # If we reach here, all retries failed
        logger.error(
            f"🚨 Failed to play media after {max_retries} retries for call {call_connection_id}"
        )
        raise RuntimeError(
            f"Failed to play media after {max_retries} retries for call {call_connection_id}"
        )

    except Exception as e:
        logger.error(f"❌ Error in play_response for call {call_connection_id}: {e}")
        raise
    finally:
        # 6) Always clear bot_speaking flag when done (success or error)
        if cm:
            cm.update_context("bot_speaking", False)
            await cm.persist_to_redis_async(ws.app.state.redis)
            logger.debug(f"🔄 Cleared bot_speaking flag for call {call_connection_id}")


# async def play_response_with_queue(
async def play_response_with_queue(
    ws: WebSocket,
    response_text: str,
    use_ssml: bool = False,
    voice_name: str = GREETING_VOICE_TTS,
    locale: str = "en-US",
    participants: list = None,
    max_retries: int = 5,
    initial_backoff: float = 0.5,
    transcription_resume_delay: float = 1.0,
):
    """
    Enhanced play_response that supports message queuing for sequential playback.
    If the bot is already speaking, messages are queued and played in order.

    :param ws:                        WebSocket connection with app state
    :param response_text:             Plain text or SSML to speak
    :param use_ssml:                  If True, wrap in SsmlSource; otherwise TextSource
    :param voice_name:                Valid Azure TTS voice name (default: en-US-JennyNeural)
    :param locale:                    Voice locale (default: en-US)
    :param participants:              List of call participants for target identification
    :param max_retries:               Maximum retry attempts for 8500 errors
    :param initial_backoff:           Initial backoff time in seconds
    :param transcription_resume_delay: Extra delay after media ends to ensure transcription resumes
    """
    cm = ws.app.state.cm
    call_connection_id = ws.headers.get("x-ms-call-connection-id")

    # Check if bot is currently speaking
    bot_speaking = cm.get_context("bot_speaking", False)
    logger.info(
        f"Queue processing: {cm.is_queue_processing()}, "
        f"Bot speaking: {bot_speaking}, "
        f"Queue Size: {cm.get_queue_size()}"
    )
    if bot_speaking or cm.is_queue_processing():
        # Bot is speaking or queue is being processed, add to queue
        logger.info(
            f"🎵 Bot is speaking or queue processing for call {call_connection_id}. Adding message to queue."
        )
        await cm.enqueue_message(
            response_text=response_text,
            use_ssml=use_ssml,
            voice_name=voice_name,
            locale=locale,
            participants=participants,
            max_retries=max_retries,
            initial_backoff=initial_backoff,
            transcription_resume_delay=transcription_resume_delay,
        )

        # Start queue processing if not already running
        if not cm.is_queue_processing():
            asyncio.create_task(process_message_queue(ws))
        return

    # Bot is not speaking, play immediately and then process queue
    await _play_response_direct(
        ws,
        response_text,
        use_ssml,
        voice_name,
        locale,
        participants,
        max_retries,
        initial_backoff,
        transcription_resume_delay,
    )

    # After direct playback, process any queued messages
    if cm.get_queue_size() > 0 and not cm.is_queue_processing():
        asyncio.create_task(process_message_queue(ws))


async def process_message_queue(ws: WebSocket):
    """
    Process messages from the queue sequentially.

    :param ws: WebSocket connection with app state
    """
    cm = ws.app.state.cm
    call_connection_id = ws.headers.get("x-ms-call-connection-id")

    await cm.set_queue_processing_status(True)
    logger.info(f"🎬 Started queue processing for call {call_connection_id}")

    try:
        while True:
            # Check if media was cancelled due to interrupt
            if cm.is_media_cancelled():
                logger.info(
                    f"🚫 Media cancelled detected for call {call_connection_id}. Stopping queue processing."
                )
                break

            message_data = await cm.get_next_message()
            if not message_data:
                break

            logger.info(f"🎵 Processing queued message for call {call_connection_id}")

            try:
                # Play the queued message
                await _play_response_direct(
                    ws=ws,
                    response_text=message_data["response_text"],
                    use_ssml=message_data["use_ssml"],
                    voice_name=message_data["voice_name"] or GREETING_VOICE_TTS,
                    locale=message_data["locale"],
                    participants=message_data["participants"],
                    max_retries=message_data["max_retries"],
                    initial_backoff=message_data["initial_backoff"],
                    transcription_resume_delay=message_data.get(
                        "transcription_resume_delay", 1.0
                    ),
                )
            except MediaCancelledException:
                logger.info(
                    f"🚫 Media playback cancelled for call {call_connection_id}. Stopping queue processing."
                )
                break

            # Small delay between messages to allow for proper state transitions
            await asyncio.sleep(0.1)

    except Exception as e:
        logger.error(
            f"❌ Error processing message queue for call {call_connection_id}: {e}",
            exc_info=True,
        )
    finally:
        await cm.set_queue_processing_status(False)
        logger.info(f"🎬 Finished queue processing for call {call_connection_id}")


async def _play_response_direct(
    ws: WebSocket,
    response_text: str,
    use_ssml: bool = False,
    voice_name: str = GREETING_VOICE_TTS,
    locale: str = "en-US",
    participants: list = None,
    max_retries: int = 5,
    initial_backoff: float = 0.5,
    transcription_resume_delay: float = 1.0,
):
    """
    Direct implementation of play_response without queuing logic.
    This is the core playback function that handles the actual TTS.

    :param ws:                        WebSocket connection with app state
    :param response_text:             Plain text or SSML to speak
    :param use_ssml:                  If True, wrap in SsmlSource; otherwise TextSource
    :param voice_name:                Valid Azure TTS voice name (default: en-US-JennyNeural)
    :param locale:                    Voice locale (default: en-US)
    :param participants:              List of call participants for target identification
    :param max_retries:               Maximum retry attempts for 8500 errors
    :param initial_backoff:           Initial backoff time in seconds
    :param transcription_resume_delay: Extra delay after media ends to ensure transcription resumes
    """
    # 1) Get the call-specific client
    call_connection_id = ws.headers.get("x-ms-call-connection-id")
    acs_caller = ws.app.state.acs_caller
    call_conn = acs_caller.get_call_connection(call_connection_id=call_connection_id)
    cm = ws.app.state.cm

    # If participants is empty or None, try to get target_participant from ws.app.state
    if not participants:
        logger.warning(
            f"No participants provided for call {call_connection_id}. Attempting to use target participant in state."
        )
        target_participant = getattr(ws.app.state, "target_participant", None)
        if target_participant:
            participants = [target_participant]
            logger.info(
                f"Using target_participant from ws.app.state for call {call_connection_id}."
            )
        else:
            logger.error(
                f"No target_participant found in ws.app.state for call {call_connection_id}. Cannot play media."
            )
            return

    if not call_conn:
        logger.error(
            f"Could not get call connection object for {call_connection_id}. Cannot play media."
        )
        return

    # 2) Validate and sanitize response text
    if not response_text or not response_text.strip():
        logger.info(
            f"Skipping media playback for call {call_connection_id} because response_text is empty."
        )
        return

    # 3) Set bot_speaking flag and transcription_paused indicator at start
    # .   Note: This is now managed on the CallConnected event callback (routers/acs.py)
    # if cm:
    # cm.update_context("bot_speaking", True)
    # cm.update_context("transcription_paused_for_media", True)
    # await cm.persist_to_redis_async(ws.app.state.redis)

    try:
        # Sanitize and prepare the response text
        sanitized_text = response_text.strip().replace("\n", " ").replace("\r", " ")
        sanitized_text = " ".join(sanitized_text.split())

        # Log the sanitized text (first 100 chars) for debugging
        text_preview = (
            sanitized_text[:100] + "..."
            if len(sanitized_text) > 100
            else sanitized_text
        )
        logger.info(f"🔧 Playing text: '{text_preview}'")

        # 4) Build the correct play_source object
        if use_ssml:
            source = SsmlSource(ssml_text=sanitized_text)
            logger.debug(f"Created SsmlSource for call {call_connection_id}")
        else:
            source = TextSource(
                text=sanitized_text, voice_name=voice_name, source_locale=locale
            )
            logger.debug(
                f"Created TextSource for call {call_connection_id} with voice {voice_name}"
            )

        # 5) Retry loop for 8500 errors
        for attempt in range(max_retries):
            try:
                # Run the synchronous play_media call in a thread pool to avoid blocking
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: call_conn.play_media(
                        play_source=source,
                        # play_to=participants,
                        interrupt_call_media_operation=True,
                    ),
                )
                logger.info(
                    f"✅ Successfully played media on attempt {attempt + 1} to play response: {sanitized_text}"
                )

                # Add delay for transcription to resume
                if transcription_resume_delay > 0:
                    await asyncio.sleep(transcription_resume_delay)

                return response

            except HttpResponseError as e:
                # Check for cancellation-related errors that indicate interrupt
                cancellation_indicators = [
                    "cancelled",
                    "disconnected",
                    "call ended",
                    "media cancelled",
                    "operation cancelled",
                    "connection closed",
                ]

                error_message = str(e).lower()
                if any(
                    indicator in error_message for indicator in cancellation_indicators
                ):
                    logger.warning(
                        f"🚫 Media cancellation detected for call {call_connection_id}: {e}"
                    )
                    await cm.set_media_cancelled(True)
                    raise MediaCancelledException(f"Media playback cancelled: {e}")

                # Check for 8500 error code or message indicating media operation is already active
                logger.warning(
                    f"⏳ Media active (8500) error on attempt {attempt + 1} for call {call_connection_id}. "
                )
                if (
                    getattr(e, "status_code", None) == 8500
                    or "already in media operation" in str(e)
                    or "Media operation is already active" in str(e)
                ):
                    if attempt < max_retries - 1:  # Don't wait on the last attempt
                        wait_time = initial_backoff * (2**attempt)
                        logger.warning(
                            f"⏳ Media active (8500) error on attempt {attempt + 1} for call {call_connection_id}. "
                            f"Retrying after {wait_time:.1f}s..."
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(
                            f"🚨 Failed to play media after {max_retries} retries for call {call_connection_id}"
                        )
                        raise RuntimeError(
                            f"Failed to play media after {max_retries} retries for call {call_connection_id}"
                        )
                else:
                    logger.error(f"❌ Unexpected ACS error during play_media: {e}")
                    raise
            except Exception as e:
                logger.error(f"❌ Unexpected exception during play_media: {e}")
                raise

        # If we reach here, all retries failed
        logger.error(
            f"🚨 Failed to play media after {max_retries} retries for call {call_connection_id}"
        )
        raise RuntimeError(
            f"Failed to play media after {max_retries} retries for call {call_connection_id}"
        )

    except Exception as e:
        logger.error(
            f"❌ Error in _play_response_direct for call {call_connection_id}: {e}"
        )
        raise
    finally:
        # 6) Always clear bot_speaking flag when done (success or error)
        if cm:
            # cm.update_context("bot_speaking", False)
            # cm.update_context("transcription_paused_for_media", False)
            # await cm.persist_to_redis_async(ws.app.state.redis)
            logger.debug(f"🔄 Cleared bot_speaking flag for call {call_connection_id}")
