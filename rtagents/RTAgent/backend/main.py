"""
voice_agent.main
================
Entrypoint that stitches everything together:

• config / CORS
• shared objects on `app.state`  (Speech, Redis, ACS, TTS, dashboard-clients)
• route registration (routers package)
"""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from utils.ml_logging import get_logger
import os
from rtagents.RTMedAgent.backend.settings import (
    ALLOWED_ORIGINS,
    AOAI_STT_KEY,
    AOAI_STT_ENDPOINT,
    AZURE_COSMOS_CONNECTION_STRING,
    AZURE_COSMOS_DB_DATABASE_NAME,
    AZURE_COSMOS_DB_COLLECTION_NAME,
    VOICE_TTS,
    RATE,
    CHANNELS,
    FORMAT,
    CHUNK,
    VAD_THRESHOLD,
    PREFIX_PADDING_MS,
    SILENCE_DURATION_MS,
)
from services import (
    SpeechSynthesizer,
    SpeechCoreTranslator,
    CosmosDBMongoCoreManager,
    AzureRedisManager,
)
from rtagents.RTMedAgent.backend.services.acs.acs_caller import (
    initialize_acs_caller_instance,
)
from routers import router as api_router
from rtagents.RTMedAgent.backend.agents.base import RTAgent

logger = get_logger("main")

# --------------------------------------------------------------------------- #
#  App factory
# --------------------------------------------------------------------------- #
app = FastAPI()
app.state.clients = set()  # /relay dashboard sockets
app.state.greeted_call_ids = set()  # to avoid double greetings

# ---------------- Middleware ------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------- Startup / Shutdown ---------------------------------------
@app.on_event("startup")
async def on_startup() -> None:
    logger.info("🚀 startup…")

    # Speech SDK
    app.state.stt_client = SpeechCoreTranslator()
    app.state.tts_client = SpeechSynthesizer(voice=VOICE_TTS)

    # Redis connection
    app.state.redis = AzureRedisManager()

    # Cosmos DB connection
    app.state.cosmos = CosmosDBMongoCoreManager(
        connection_string=AZURE_COSMOS_CONNECTION_STRING,
        database_name=AZURE_COSMOS_DB_DATABASE_NAME,
        collection_name=AZURE_COSMOS_DB_COLLECTION_NAME,
    )

    # Gpt4o-transcribe config
    app.state.aoai_stt_cfg = {
        "url": f"{AOAI_STT_ENDPOINT.replace('https','wss')}"
        "/openai/realtime?api-version=2025-04-01-preview&intent=transcription",
        "headers": {"api-key": AOAI_STT_KEY},
        "rate": RATE,
        "channels": CHANNELS,  # Mono audio
        "format_": FORMAT,  # PCM16
        "chunk": CHUNK,  # Size of audio chunks to process
        # VAD settings
        "vad": {
            "threshold": VAD_THRESHOLD,
            # Prefix padding in milliseconds to avoid cutting off speech
            "prefix_padding_ms": PREFIX_PADDING_MS,
            # Silence duration in milliseconds to consider the end of speech
            "silence_duration_ms": SILENCE_DURATION_MS,
        },
    }
    # Outbound ACS caller (may be None if env vars missing)
    app.state.acs_caller = initialize_acs_caller_instance()
    app.state.auth_agent = RTAgent(
        config_path="rtagents/RTMedAgent/backend/agents/agent_store/auth_agent.yaml"
    )
    app.state.task_agent = RTAgent(
        config_path="rtagents/RTMedAgent/backend/agents/agent_store/task_agent.yaml"
    )
    logger.info("startup complete")


@app.on_event("shutdown")
async def on_shutdown() -> None:
    logger.info("🛑 shutdown…")
    # (Close Redis, ACS sessions, etc. if your helpers expose close() methods)


# ---------------- Routers ---------------------------------------------------
app.include_router(api_router)

# --------------------------------------------------------------------------- #
#  CLI entry-point
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",  # Use import string to support reload
        host="0.0.0.0",
        port=8010,
        reload=True,
    )
