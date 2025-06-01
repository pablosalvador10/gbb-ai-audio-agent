from rtagents.RTAgent.backend.orchestration.orchestrator import route_turn

from shared_ws import (
    broadcast_message,
    send_response_to_acs,
)
from azure.cognitiveservices.speech.audio import AudioStreamFormat, PushAudioInputStream
from helpers import check_for_stopwords

from utils.ml_logging import get_logger

logger = get_logger("routers.acs.handlers.acs_ws_handler")

import asyncio
import json
from base64 import b64decode
from fastapi import WebSocket, WebSocketDisconnect

async def audio_receiver(
    ws: WebSocket,
    push_stream,  # type: PushAudioInputStream
    cid: str,
    user_raw_id_lookup: dict,
    logger,
):
    """
    Receives audio from ACS WebSocket and streams it into the STT push stream.
    """
    user_raw_id = user_raw_id_lookup.get(cid)

    while True:
        try:
            raw = await asyncio.wait_for(ws.receive_text(), timeout=5.0)
            data = json.loads(raw)
            kind = data.get("kind")

            if kind == "AudioData":
                if not user_raw_id and cid in user_raw_id_lookup:
                    user_raw_id = user_raw_id_lookup[cid]

                if user_raw_id and data["audioData"]["participantRawID"] != user_raw_id:
                    continue  # Skip bot audio

                audio_bytes = b64decode(data["audioData"]["data"])
                push_stream.write(audio_bytes)

            elif kind == "CallConnected":
                pid = data["callConnected"]["participant"]["rawID"]
                user_raw_id_lookup[cid] = pid
                user_raw_id = pid

        except (asyncio.TimeoutError, WebSocketDisconnect, json.JSONDecodeError):
            logger.info(f"[{cid}] WebSocket closed or invalid data received.")
            break
        except Exception as e:
            logger.exception(f"[{cid}] Error in audio_receiver: {e}")
            break

async def stt_transcript_consumer(
    queue: asyncio.Queue,
    cm,
    ws: WebSocket,
    clients,
    acs,
    cid: str,
    logger,
    greet_set: set,
    tts_client,
):
    """
    Reads transcriptions from queue and manages assistant logic.
    """
    if cid not in greet_set:
        greet = (
            "Hello from XMYX Healthcare Company! Before I can assist you, "
            "let’s verify your identity. How may I address you?"
        )
        await broadcast_message(clients, greet, "Assistant")
        await send_response_to_acs(ws, greet)
        await cm.append_to_history("assistant", greet)
        greet_set.add(cid)

    while True:
        try:
            spoken = await queue.get()
            queue.task_done()

            # Stop overlapping TTS responses
            tts_client.stop_speaking()
            for task in list(getattr(ws.app.state, "tts_tasks", [])):
                task.cancel()

            await broadcast_message(clients, spoken, "User")

            if check_for_stopwords(spoken):
                await broadcast_message(clients, "Goodbye!", "Assistant")
                await send_response_to_acs(ws, "Goodbye!", blocking=True)
                await asyncio.sleep(1)
                await acs.disconnect_call(cid)
                break

            await cm.append_to_history("user", spoken)
            await route_turn(cm, spoken, ws, is_acs=True)

        except Exception as e:
            logger.exception(f"[{cid}] Error in stt_transcript_consumer: {e}")
            break


async def stt_transcript_consumer(
    queue: asyncio.Queue,
    cm,
    ws: WebSocket,
    clients,
    acs,
    cid: str,
    logger,
    greet_set: set,
    tts_client,
):
    """
    Reads transcriptions from queue and manages assistant logic.
    """
    if cid not in greet_set:
        greet = (
            "Hello from XMYX Healthcare Company! Before I can assist you, "
            "let’s verify your identity. How may I address you?"
        )
        await broadcast_message(clients, greet, "Assistant")
        await send_response_to_acs(ws, greet)
        await cm.append_to_history("assistant", greet)
        greet_set.add(cid)

    while True:
        try:
            spoken = await queue.get()
            queue.task_done()

            # Stop overlapping TTS responses
            tts_client.stop_speaking()
            for task in list(getattr(ws.app.state, "tts_tasks", [])):
                task.cancel()

            await broadcast_message(clients, spoken, "User")

            if check_for_stopwords(spoken):
                await broadcast_message(clients, "Goodbye!", "Assistant")
                await send_response_to_acs(ws, "Goodbye!", blocking=True)
                await asyncio.sleep(1)
                await acs.disconnect_call(cid)
                break

            await cm.append_to_history("user", spoken)
            await route_turn(cm, spoken, ws, is_acs=True)

        except Exception as e:
            logger.exception(f"[{cid}] Error in stt_transcript_consumer: {e}")
            break