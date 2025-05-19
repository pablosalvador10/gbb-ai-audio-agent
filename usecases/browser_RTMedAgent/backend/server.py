"""
Real-time voice agent backend.

Exposes:
  • /realtime   – bi-directional WebSocket for STT/LLM/TTS
  • /health     – simple liveness probe
"""
import asyncio
import json
import os
import time
import uuid
from base64 import b64decode
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional

from azure.core.messaging import CloudEvent
from openai import AzureOpenAI
from fastapi import (FastAPI, HTTPException, Request, WebSocket,
                     WebSocketDisconnect)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.websockets import WebSocketState
from src.speech.text_to_speech import SpeechSynthesizer
from usecases.browser_RTMedAgent.backend.acs_helpers import (
    broadcast_message, initialize_acs_caller_instance, send_pcm_frames)
from usecases.browser_RTMedAgent.backend.conversation_state import \
    ConversationManager
from usecases.browser_RTMedAgent.backend.helpers import (add_space,
                                                         check_for_stopwords)
from usecases.browser_RTMedAgent.backend.settings import (ACS_CALL_PATH,
                                                          ACS_CALLBACK_PATH,
                                                          ACS_WEBSOCKET_PATH,
                                                          TTS_END)
from usecases.browser_RTMedAgent.backend.tools_helper import (function_mapping,
                                                              push_tool_end,
                                                              push_tool_start)
from usecases.browser_RTMedAgent.backend.tools import (available_tools)
from utils.ml_logging import get_logger

# List to store connected WebSocket clients
connected_clients: List[WebSocket] = []

from typing import Dict

from azure.cognitiveservices.speech.audio import (AudioStreamFormat,
                                                  PushAudioInputStream)
from azure.core.exceptions import HttpResponseError
# --- ACS Integration ---
from pydantic import BaseModel  # For request body validation
from src.speech.speech_to_text import SpeechCoreTranslator

# --- Global Clients (Initialized in lifespan) ---
az_openai_client = AzureOpenAI(
    api_version="2025-02-01-preview",
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_KEY"),
)
logger = get_logger()


# --- Set Up App & Middleware ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup Logic ---
    logger.info("Application startup...")

    # Initialize SpeechCoreTranslator
    try:
        app.state.stt_client = SpeechCoreTranslator()
        logger.info("SpeechCoreTranslator initialized successfully.")
    except Exception as e:
        logger.error(f"Failed to initialize SpeechCoreTranslator: {e}", exc_info=True)
        app.state.stt_client = None  # Store None if failed

    # Initialize AcsCaller
    app.state.acs_caller = (
        initialize_acs_caller_instance()
    )  # Call the modified function
    app.state.greeted_call_ids = set()  # Initialize greeted call IDs set
    # Initialize potentially unused TTS client (consider removing if confirmed unused)
    try:
        app.state.tts_client = SpeechSynthesizer()
        logger.info("SpeechSynthesizer initialized.")
    except Exception as e:
        logger.error(f"Failed to initialize SpeechSynthesizer: {e}", exc_info=True)
        app.state.tts_client = None

    logger.info("Startup complete.")
    yield  # Application runs here
    # --- Shutdown Logic ---
    logger.info("Application shutting down...")
    # if hasattr(app.state, 'acs_caller') and app.state.acs_caller:
    #     try:
    #         await app.state.acs_caller.close() # Ensure close is async in AcsCaller
    #     except Exception as e:
    #         logger.error(f"Error closing AcsCaller: {e}", exc_info=True)
    # # Add other cleanup if needed
    logger.info("Shutdown complete.")


app = FastAPI(lifespan=lifespan)  # Apply lifespan manager
allowed_origins = [
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "https://localhost:5173",
    "https://127.0.0.1:5173",
    "http://localhost",
    "http://127.0.0.1",
    "https://localhost",
    "https://127.0.0.1",
    # Add any other origins if necessary
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,  # Use the defined list
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- AOAI Processing---
async def process_gpt_response(
    cm: ConversationManager,
    user_prompt: str,
    websocket: WebSocket,
    is_acs: bool = False,
) -> Optional[Dict[str, Any]]:
    """
    Stream GPT response, TTS chunks, handle tools.

    Returns
    -------
    dict | None
        Tool output (only for ``authenticate_user``) or ``None``.
    """
    cm.hist.append({"role": "user", "content": user_prompt})
    logger.info(f"🎙️ Processing prompt: {user_prompt}")

    try:
        stream_start = time.perf_counter()
        response = az_openai_client.chat.completions.create(
            stream=True,
            messages=cm.hist,
            tools=available_tools,
            tool_choice="auto",
            max_tokens=4096,
            temperature=0.5,
            top_p=1.0,
            model=os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT_ID", ""),
        )

        collected: List[str] = []
        final_collected: List[str] = []
        prev_ts = stream_start
        tool_started = False
        tool_name = tool_id = args = ""

        for chunk in response:
            now = time.perf_counter()
            logger.info(f"🔸 Chunk arrived after: {(now - prev_ts)*1000:.1f} ms")
            prev_ts = now

            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta

            if delta.tool_calls:
                tc = delta.tool_calls[0]
                tool_id = tc.id or tool_id
                tool_name = tc.function.name or tool_name
                args += tc.function.arguments or ""
                tool_started = True
                continue

            if delta.content:
                collected.append(delta.content)
                if delta.content in TTS_END:
                    text_streaming = add_space("".join(collected).strip())
                    if is_acs:
                        # Send TTS audio to ACS WebSocket
                        await broadcast_message(text_streaming, "Assistant")
                        send_response_to_acs(websocket, text_streaming)
                    else:
                        await send_tts_audio(text_streaming, websocket)
                        await websocket.send_text(
                            json.dumps(
                                {
                                    "type": "assistant_streaming",
                                    "content": text_streaming,
                                }
                            )
                        )
                    final_collected.append(text_streaming)
                    collected.clear()

        # ── flush any residual text ───────────────────────────────────────────
        if collected:
            pending = "".join(collected).strip()
            if is_acs:
                # Send TTS audio to ACS WebSocket
                await broadcast_message(text_streaming, "Assistant")
                send_response_to_acs(websocket, pending)
            else:
                await send_tts_audio(pending, websocket)
                await websocket.send_text(
                    json.dumps({"type": "assistant_streaming", "content": pending})
                )
            final_collected.append(pending)

        logger.info(
            f"💬 GPT full stream time: "
            f"{(time.perf_counter() - stream_start)*1000:.1f} ms"
        )
        text = "".join(final_collected).strip()
        if text:
            cm.hist.append({"role": "assistant", "content": text})
            await push_final(websocket, "assistant", text, is_acs)
            logger.info(f"🧠 Assistant responded: {text}")

        if tool_started:
            cm.hist.append(
                {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": tool_id,
                            "type": "function",
                            "function": {"name": tool_name, "arguments": args},
                        }
                    ],
                }
            )
            return await handle_tool_call(tool_name, tool_id, args, cm, websocket)

    except asyncio.CancelledError:
        logger.info(f"🔚 process_gpt_response cancelled for input: '{user_prompt[:40]}'")
        raise

    return None


async def process_tool_followup(
    cm: ConversationManager, websocket: WebSocket, is_acs: bool
) -> None:
    """Stream follow-up after tool execution."""
    collected: List[str] = []
    final_collected: List[str] = []

    try:
        response = az_openai_client.chat.completions.create(
            stream=True,
            messages=cm.hist,
            temperature=0.5,
            top_p=1.0,
            max_tokens=4096,
            model=os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT_ID"),
        )

        for chunk in response:
            if not chunk.choices:
                continue
            delta = chunk.choices[0].delta
            if getattr(delta, "content", None):
                collected.append(delta.content)
                if delta.content in TTS_END:
                    text_streaming = add_space("".join(collected).strip())
                    await send_tts_audio(text_streaming, websocket)
                    await websocket.send_text(
                        json.dumps(
                            {
                                "type": "assistant_streaming",
                                "content": text_streaming,
                            }
                        )
                    )
                    final_collected.append(text_streaming)
                    collected.clear()

        # ── flush tail chunk ─────────────────────────────────────────────────
        if collected:
            pending = "".join(collected).strip()
            if is_acs:
                # Send TTS audio to ACS WebSocket
                await broadcast_message(pending, "Assistant")

                send_response_to_acs(websocket, pending)
            else:
                await send_tts_audio(pending, websocket)
                await websocket.send_text(
                    json.dumps({"type": "assistant_streaming", "content": pending})
                )
            final_collected.append(pending)

        final_text = "".join(final_collected).strip()
        if final_text:
            cm.hist.append({"role": "assistant", "content": final_text})
            if is_acs:
                try:
                    # Use the instance from app.state
                    pcm = app.state.tts_client.synthesize_to_base64_frames(
                        text=final_text, sample_rate=16000
                    )
                    await send_pcm_frames(
                        websocket, pcm, sample_rate=16000
                    )  # Send PCM frames to ACS WebSocket
                except Exception as e:
                    logger.error(
                        f"Failed to play final TTS via ACS for call {cm.cid}: {e}",
                        exc_info=True,
                    )
            else:
                await push_final(websocket, "assistant", final_text)
                logger.info(f"🧠 Assistant said: {final_text}")

    except asyncio.CancelledError:
        logger.info("🔚 process_tool_followup cancelled")
        raise


async def handle_tool_call(  # unchanged signature
    tool_name: str,
    tool_id: str,
    function_call_arguments: str,
    cm: ConversationManager,
    websocket: WebSocket,
    is_acs: bool = False,
) -> Any:
    """
    Execute the mapped function tool, stream life-cycle events, preserve
    legacy timing logs, and follow up with GPT.
    """
    call_id = str(uuid.uuid4())[:8]  # for UI tracking

    try:
        # -------- arguments & lookup -------------------------------------------------
        params = json.loads(function_call_arguments.strip() or "{}")
        fn = function_mapping.get(tool_name)
        if fn is None:
            raise ValueError(f"Unknown tool '{tool_name}'")

        # -------- notify UI that we’re starting --------------------------------------
        await push_tool_start(websocket, call_id, tool_name, params)

        # -------- run the tool (your original timing log preserved) -----------------
        t0 = time.perf_counter()
        result_json = await fn(params)  # async/await OK
        t1 = time.perf_counter()
        elapsed_ms = (t1 - t0) * 1000

        logger.info(f"⚙️ Tool '{tool_name}' exec time: {elapsed_ms:.1f} ms")

        result = (
            json.loads(result_json) if isinstance(result_json, str) else result_json
        )

        # -------- record in chat history --------------------------------------------
        cm.hist.append(
            {
                "tool_call_id": tool_id,
                "role": "tool",
                "name": tool_name,
                "content": json.dumps(result),
            }
        )

        # -------- notify UI that we’re done ------------------------------------------
        await push_tool_end(
            websocket,
            call_id,
            tool_name,
            "success",
            elapsed_ms,
            result=result,
        )

        # -------- ask GPT to follow up with the result ------------------------------
        await process_tool_followup(cm, websocket, is_acs)
        return result

    except Exception as exc:  # noqa: BLE001
        elapsed_ms = (time.perf_counter() - t0) * 1000 if "t0" in locals() else 0.0
        logger.error(f"Tool '{tool_name}' error: {exc}")

        # tell the UI the tool failed
        await push_tool_end(
            websocket,
            call_id,
            tool_name,
            "error",
            elapsed_ms,
            error=str(exc),
        )
        return {}


# --- TTS ---
async def send_tts_audio(text: str, websocket: WebSocket) -> None:
    """Fire-and-forget TTS synthesis and log enqueue latency."""
    start = time.perf_counter()
    try:
        app.state.tts_client.start_speaking_text(text)
    except Exception as exc:  # noqa: BLE001
        logger.error(f"Error synthesizing TTS: {exc}")
    logger.info(f"🗣️ TTS enqueue time: {(time.perf_counter() - start)*1000:.1f} ms")


async def receive_and_filter(websocket: WebSocket) -> Optional[str]:
    """Receive one WS frame; swallow interrupts; return raw payload."""
    start = time.perf_counter()
    raw: str = await websocket.receive_text()
    logger.info(f"📥 WS receive time: {(time.perf_counter() - start)*1000:.1f} ms")
    try:
        msg: Dict[str, Any] = json.loads(raw)
        if msg.get("type") == "interrupt":
            logger.info("🛑 Interrupt received – stopping TTS")
            app.state.tts_client.stop_speaking()
            return None
    except json.JSONDecodeError:
        pass
    return raw


async def push_final(
    websocket: WebSocket, role: str, content: str, is_acs: bool = False
) -> None:
    """Emit a single non-streaming message so the UI can close the bubble."""
    if is_acs:
        # For ACS, we need to send the message in a different format
        await send_response_to_acs(websocket, content)
    else:
        await websocket.send_text(json.dumps({"type": role, "content": content}))


# --- API Endpoint to Initiate Call ---\napp.post(ACS_CALL_PATH)(initiate_acs_phone_call)\napp.post(ACS_CALLBACK_PATH)(handle_acs_callbacks)
class CallRequest(BaseModel):
    target_number: str  # Define expected request body


async def send_response_to_acs(websocket: WebSocket, response: str) -> None:
    """Send a response to the ACS WebSocket."""
    pcm = app.state.tts_client.synthesize_to_base64_frames(
        text=response, sample_rate=16000
    )
    await send_pcm_frames(websocket, pcm_bytes=pcm, sample_rate=16000)


@app.post(ACS_CALL_PATH)
async def initiate_acs_phone_call(
    call_request: CallRequest, request: Request
):  # Inject request to access app.state
    acs_caller_instance = request.app.state.acs_caller
    if not acs_caller_instance:
        raise HTTPException(
            status_code=503, detail="ACS Caller not initialized or configured."
        )
    try:
        # Use the instance from app.state
        result = await acs_caller_instance.initiate_call(call_request.target_number)
        # Check if the call was successfully connected
        if result.get("status") == "created":
            # Notify the frontend about the call connection
            call_connection_id = result.get("call_id")
            if call_connection_id:
                logger.info(
                    f"Call initiated successfully via API. Call ID: {call_connection_id}"
                )
                return JSONResponse(
                    content={"message": "Call initiated", "callId": call_connection_id},
                    status_code=200,
                )
            else:
                logger.error(
                    "Call initiation succeeded but no callConnectionId returned."
                )
                raise HTTPException(
                    status_code=500, detail="Call initiated but failed to get Call ID."
                )
            # Log the failure reason if available

        else:
            logger.warning(
                f"Call initiation failed: {result.get('detail', 'Unknown error')}"
            )
            return JSONResponse(content={"status": "failed"}, status_code=400)

    except HttpResponseError as e:
        logger.error(f"ACS HTTP Error initiating call: {e}", exc_info=True)
        raise HTTPException(
            status_code=e.status_code or 500, detail=f"ACS Error: {e.message}"
        )
    except RuntimeError as e:
        logger.error(f"Runtime error during call initiation: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to initiate call: {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Failed to initiate call: {str(e)}"
        )


# --- ACS Callback Handler ---


@app.post(ACS_CALLBACK_PATH)
async def handle_acs_callbacks(request: Request):
    acs_caller_instance = request.app.state.acs_caller
    if not acs_caller_instance:
        logger.error("ACS Caller not initialized, cannot handle callback.")
        return JSONResponse(
            status_code=503, content={"error": "ACS Caller not initialized"}
        )
    try:
        cloudevent = await request.json()

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
                # Updated event handling logic with broadcasting and emojis
                if event.type == "Microsoft.Communication.CallConnected":
                    logger.info(
                        f"📞 Call connected event received for call connection id: {call_connection_id}"
                    )
                    await broadcast_message("📞 Call connected")
                elif event.type == "Microsoft.Communication.ParticipantsUpdated":
                    logger.info(
                        f"👥 Participants updated event received for call connection id: {call_connection_id}"
                    )
                    await broadcast_message("👥 Participants updated")
                elif event.type == "Microsoft.Communication.CallDisconnected":
                    logger.info(
                        f"❌ Call disconnect event received for call connection id: {call_connection_id}"
                    )
                    await broadcast_message("❌ Call disconnected")
                elif event.type == "Microsoft.Communication.MediaStreamingStarted":
                    logger.info(
                        f"🎙️ Media streaming started for call connection id: {call_connection_id}"
                    )
                    await broadcast_message("🎙️ Media streaming started")
                elif event.type == "Microsoft.Communication.MediaStreamingStopped":
                    logger.info(
                        f"🛑 Media streaming stopped for call connection id: {call_connection_id}"
                    )
                    await broadcast_message("🛑 Media streaming stopped")
                elif event.type == "Microsoft.Communication.MediaStreamingFailed":
                    logger.error(
                        f"⚠️ Media streaming failed for call connection id: {call_connection_id}. Details: {event.data}"
                    )
                    await broadcast_message("⚠️ Media streaming failed")
                else:
                    logger.info(
                        f"ℹ️ Unhandled event type: {event.type} for call connection id: {call_connection_id}"
                    )
                    await broadcast_message(f"ℹ️ Unhandled event type: {event.type}")

            except Exception as e:
                logger.error(
                    f"Error processing event: {event_dict}. Error: {e}", exc_info=True
                )
            # Decide if you want to continue processing other events or stop

        # Notify the frontend about the callback event
        return JSONResponse(content={"status": "callback received"}, status_code=200)
    except Exception as e:
        logger.error(f"Error processing ACS callback event: {e}", exc_info=True)
        return JSONResponse(
            status_code=500, content={"error": f"Failed to process callback: {str(e)}"}
        )


# Map from callConnectionId → human caller’s raw ACS identifier
call_user_raw_ids: Dict[str, str] = {}
# Audio metadata storage for persisting configurations


@app.websocket(ACS_WEBSOCKET_PATH)
async def acs_websocket_endpoint(websocket: WebSocket):
    """Handles the bidirectional audio stream for an ACS call."""
    # Access initialized instances from app state
    speech_core_instance = app.state.stt_client
    acs_caller_instance = app.state.acs_caller

    if not speech_core_instance:
        logger.error("SpeechCoreTranslator not available. Cannot process ACS audio.")
        # Close connection gracefully if possible (though accept wasn't called yet)
        # await websocket.close(code=1011) # Cannot close before accept
        return
    if not acs_caller_instance:
        logger.error("ACS Caller not available. Cannot process ACS audio.")
        # await websocket.close(code=1011)
        return

    await websocket.accept()
    call_connection_id = websocket.headers.get("x-ms-call-connection-id", "UnknownCall")
    logger.info(f"▶ ACS media WebSocket accepted for call {call_connection_id}")

    loop = asyncio.get_event_loop()
    message_queue = asyncio.Queue()
    cm = ConversationManager(auth=False)  # ACS calls usually start unauthenticated
    cm.cid = call_connection_id
    user_identifier = call_user_raw_ids.get(
        call_connection_id
    )  # Get initial mapping if available

    try:
        # --- Setup Audio Stream and Recognizer ---
        fmt = AudioStreamFormat(
            samples_per_second=16000, bits_per_sample=16, channels=1
        )  # Corrected sample rate
        push_stream = PushAudioInputStream(stream_format=fmt)
        # Use the instance from app.state
        recognizer = speech_core_instance.create_realtime_recognizer(
            push_stream=push_stream,
            loop=loop,
            message_queue=message_queue,
            language="en-US",
        )
        recognizer.start_continuous_recognition_async()
        logger.info(f"🎙️ Continuous recognition started for call {call_connection_id}")

        # --- Play greeting only if not already played for this call ---
        # Note: Assumes 'greeted_call_ids' set is initialized in app.state during startup
        # and cleaned up (e.g., on CallDisconnected event).
        greeted_call_ids = app.state.greeted_call_ids

        if (
            call_connection_id != "UnknownCall"
            and call_connection_id not in greeted_call_ids
        ):
            initial_greeting = "Hello from XMYX Healthcare Company! Before I can assist you, let’s verify your identity. How may I address you?"
            logger.info(f"Playing initial greeting for call {call_connection_id}")
            # Don't await here, let it play while listening starts
            # Use the instance from app.state
            await broadcast_message(initial_greeting, "Assistant")
            await send_response_to_acs(websocket, initial_greeting)

            cm.hist.append({"role": "assistant", "content": initial_greeting})
            greeted_call_ids.add(call_connection_id)  # Mark as greeted
        else:
            logger.info(
                f"Skipping initial greeting for already greeted call {call_connection_id}"
            )

        # --- Main Loop ---
        while True:
            # --- Check for recognized speech ---
            try:
                try:
                    recognized_text = message_queue.get_nowait()
                except asyncio.QueueEmpty:
                    recognized_text = None
                if recognized_text:
                    logger.info(
                        f"Processing recognized text for call {call_connection_id}: {recognized_text}"
                    )
                    await broadcast_message(recognized_text, "User")

                    if check_for_stopwords(recognized_text):
                        logger.info(
                            f"Stop word detected in call {call_connection_id}. Ending conversation."
                        )
                        # Optionally play a goodbye message
                        await broadcast_message("Goodbye!", "Assistant")

                        await send_response_to_acs(websocket, "Goodbye!")
                        await asyncio.sleep(
                            1
                        )  # Allow time for TTS to potentially start

                        await acs_caller_instance.disconnect_call(
                            call_connection_id
                        )  # Disconnect the call
                        break  # Exit the main loop

                    await process_gpt_response(
                        cm, recognized_text, websocket, is_acs=True
                    )
                    message_queue.task_done()
            except asyncio.TimeoutError:
                pass
            except Exception as q_err:
                logger.error(
                    f"Error getting from message queue for call {call_connection_id}: {q_err}",
                    exc_info=True,
                )

            # --- Receive and process incoming WebSocket data from ACS ---
            try:
                raw_data = await asyncio.wait_for(websocket.receive_text(), timeout=5.0)
                data = json.loads(raw_data)
            except asyncio.TimeoutError:
                # No data received from ACS for a while, check if connection is still alive
                if websocket.client_state != WebSocketState.CONNECTED:
                    logger.warning(
                        f"ACS WebSocket {call_connection_id} disconnected while waiting for data."
                    )
                    break
                continue  # Continue loop if connected but no data
            except WebSocketDisconnect:
                logger.info(f"ACS WebSocket disconnected for call {call_connection_id}")
                break
            except json.JSONDecodeError:
                logger.warning(
                    f"Received invalid JSON from ACS for call {call_connection_id}"
                )
                continue
            except Exception as e:
                logger.error(
                    f"Error receiving from ACS WebSocket {call_connection_id}: {e}",
                    exc_info=True,
                )
                break  # Exit loop on unexpected error

            # --- Handle Different Message Kinds ---
            kind = data.get("kind")
            if kind == "AudioData":
                raw_id = data.get("audioData", {}).get("participantRawID")
                if not user_identifier and call_connection_id in call_user_raw_ids:
                    user_identifier = call_user_raw_ids[call_connection_id]
                if user_identifier and raw_id != user_identifier:
                    continue

                try:
                    b64 = data.get("audioData", {}).get("data")
                    if b64:
                        push_stream.write(b64decode(b64))
                except Exception as e:
                    logger.error(
                        f"Error processing audio data chunk for call {call_connection_id}: {e}",
                        exc_info=True,
                    )

            elif kind == "CallConnected":
                connected_participant_id = (
                    data.get("callConnected", {}).get("participant", {}).get("rawID")
                )
                if (
                    connected_participant_id
                    and call_connection_id not in call_user_raw_ids
                ):
                    call_user_raw_ids[call_connection_id] = connected_participant_id
                    user_identifier = connected_participant_id

            elif (
                kind == "PlayCompleted"
                or kind == "PlayFailed"
                or kind == "PlayCanceled"
            ):
                logger.info(
                    f"Received {kind} event via WebSocket for call {call_connection_id}"
                )
                # Handle media playback status if needed

    except WebSocketDisconnect:
        logger.info(f"ACS WebSocket {call_connection_id} disconnected.")
    except Exception as e:
        logger.error(
            f"Unhandled error in ACS WebSocket handler for call {call_connection_id}: {e}",
            exc_info=True,
        )
    finally:
        logger.info(
            f"🧹 Cleaning up ACS WebSocket handler for call {call_connection_id}."
        )
        if recognizer:
            try:
                # Use wait_for to prevent hanging if stop takes too long
                await asyncio.wait_for(
                    asyncio.to_thread(recognizer.stop_continuous_recognition_async),
                    timeout=5.0,
                )
                logger.info(
                    f"🎙️ Continuous recognition stopped for call {call_connection_id}"
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"Timeout stopping recognizer for call {call_connection_id}"
                )
            except Exception as e:
                logger.error(
                    f"Error stopping recognizer for call {call_connection_id}: {e}",
                    exc_info=True,
                )
        if push_stream:
            push_stream.close()
            logger.info(f"Audio push stream closed for call {call_connection_id}")
        if websocket.client_state == WebSocketState.CONNECTED:
            await websocket.close()
            logger.info(
                f"ACS WebSocket connection closed for call {call_connection_id}"
            )
        # Remove the call ID mapping on disconnect
        if call_connection_id in call_user_raw_ids:
            try:  # Protect against potential KeyError if deleted elsewhere
                del call_user_raw_ids[call_connection_id]
                logger.info(f"Removed call ID mapping for {call_connection_id}")
            except KeyError:
                logger.warning(
                    f"Call ID mapping for {call_connection_id} already removed."
                )


# --- Main Flow Conversation---


async def main_conversation(websocket: WebSocket, cm: ConversationManager) -> None:
    """Main multi-turn loop after authentication."""
    while True:
        raw = await receive_and_filter(websocket)
        if raw is None:
            continue
        try:
            prompt = json.loads(raw).get("text", raw)
        except json.JSONDecodeError:
            prompt = raw.strip()
        if not prompt:
            continue
        if check_for_stopwords(prompt):
            goodbye = "Thank you for using our service. Goodbye."
            await websocket.send_text(json.dumps({"type": "exit", "message": goodbye}))
            await send_tts_audio(goodbye, websocket)
            return

        total_start = time.perf_counter()
        await process_gpt_response(cm, prompt, websocket)
        logger.info(
            f"📊 phase:main | cid:{cm.cid} | "
            f"total:{(time.perf_counter() - total_start)*1000:.1f}ms"
        )


async def authentication_conversation(
    websocket: WebSocket, cm: ConversationManager
) -> Optional[Dict[str, Any]]:
    """Run the authentication sub-dialogue."""
    greeting = (
        "Hello from XMYX Healthcare Company! Before I can assist you, "
        "let’s verify your identity. How may I address you?"
    )
    await websocket.send_text(json.dumps({"type": "status", "message": greeting}))
    await send_tts_audio(greeting, websocket)
    cm.hist.append({"role": "assistant", "content": greeting})

    while True:
        raw = await receive_and_filter(websocket)
        if raw is None:
            continue
        try:
            prompt = json.loads(raw).get("text", raw)
        except json.JSONDecodeError:
            prompt = raw.strip()
        if not prompt:
            continue
        if check_for_stopwords(prompt):
            bye = "Thank you for calling. Goodbye."
            await websocket.send_text(json.dumps({"type": "exit", "message": bye}))
            await send_tts_audio(bye, websocket)
            return None

        auth_start = time.perf_counter()
        result = await process_gpt_response(cm, prompt, websocket)
        logger.info(
            f"[Latency Summary] phase:auth | cid:{cm.cid} | "
            f"total:{(time.perf_counter() - auth_start)*1000:.1f}ms"
        )
        if result and result.get("authenticated"):
            return result


# --- Websocket EntryPoints---


# Standalone WebSocket endpoint
@app.websocket("/relay")
async def relay_websocket(websocket: WebSocket):
    if websocket not in connected_clients:
        await websocket.accept()
        connected_clients.append(websocket)
        # logger.info("WebSocket connected to relay.")
    try:
        while True:
            # Keep the connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        connected_clients.remove(websocket)
        logger.info("WebSocket disconnected from relay.")


@app.websocket("/realtime")
async def websocket_endpoint(websocket: WebSocket) -> None:  # noqa: D401
    """Handle authentication flow, then main conversation."""
    await websocket.accept()
    cm = ConversationManager(auth=True)
    caller_ctx = await authentication_conversation(websocket, cm)
    if caller_ctx:
        cm = ConversationManager(auth=False)
        await main_conversation(websocket, cm)


# --------------------------------------------------------------------------- #
#  Health, Liveness, and Readiness Probes
# --------------------------------------------------------------------------- #
@app.get("/healthz")
async def liveness():
    """Liveness probe: checks if the event loop is responsive."""
    try:
        await asyncio.sleep(0)
        return {"status": "alive"}
    except Exception:
        raise HTTPException(status_code=500, detail="App loop unresponsive")

@app.get("/readyz")
async def readiness():
    """
    Readiness probe for Azure Container Apps.
    Returns 200 if all critical components are ready, else 503.
    """
    components = {
        "acs_caller": getattr(app.state, "acs_caller", None) is not None,
        "tts_client": getattr(app.state, "tts_client", None) is not None,
        "stt_client": getattr(app.state, "stt_client", None) is not None,
    }
    if all(components.values()):
        return {
            "ready": True,
            "message": "All critical components are ready.",
            "components": components,
        }
    return JSONResponse(
        status_code=503,
        content={
            "ready": False,
            "unavailable": [k for k, v in components.items() if not v],
        },
    )

@app.get("/startup")
async def startup_probe():
    """
    Startup probe: checks if TTS and STT clients are loaded.
    Returns 200 if ready, else 503.
    """
    if getattr(app.state, "tts_client", None) is None:
        raise HTTPException(status_code=503, detail="TTS not loaded")
    if getattr(app.state, "stt_client", None) is None:
        raise HTTPException(status_code=503, detail="STT not loaded")
    return {"started": True}

@app.websocket("/loadtest")
async def loadtest_websocket(websocket: WebSocket):
    """
    WebSocket endpoint for load-testing server resiliency and scale.
    Echoes back messages and periodically sends heartbeat pings.
    """
    await websocket.accept()
    logger.info("Loadtest WebSocket client connected.")
    try:
        while True:
            try:
                data = await asyncio.wait_for(websocket.receive_text(), timeout=10.0)
                # Echo back the received message
                await websocket.send_text(json.dumps({
                    "type": "echo",
                    "message": data,
                    "ts": time.time(),
                }))
            except asyncio.TimeoutError:
                # Send heartbeat ping if no message received in timeout window
                await websocket.send_text(json.dumps({
                    "type": "heartbeat",
                    "message": "ping",
                    "ts": time.time(),
                }))
    except WebSocketDisconnect:
        logger.info("Loadtest WebSocket client disconnected.")
    except Exception as e:
        logger.error(f"Loadtest WebSocket error: {e}", exc_info=True)

# --------------------------------------------------------------------------- #
#  local dev entry-point
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8010))
    uvicorn.run(app, host="0.0.0.0", port=port, proxy_headers=True)
