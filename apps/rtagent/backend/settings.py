"""
utils/settings.py
=================
Central place for every environment variable, constant path, and
used by the voice-agent service.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import List
import yaml

from dotenv import load_dotenv

from src.enums.stream_modes import StreamMode
import logging

# Load environment variables from .env file
load_dotenv(override=True)
logger = logging.getLogger("settings")


AZURE_CLIENT_ID: str = os.getenv("AZURE_CLIENT_ID", "")
AZURE_TENANT_ID: str = os.getenv("AZURE_TENANT_ID", "")
# ------------------------------------------------------------------------------
# Azure OpenAI
# ------------------------------------------------------------------------------
AZURE_OPENAI_ENDPOINT: str = os.getenv("AZURE_OPENAI_ENDPOINT", "")
AZURE_OPENAI_KEY: str = os.getenv("AZURE_OPENAI_KEY", "")
AZURE_OPENAI_CHAT_DEPLOYMENT_ID: str = os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT_ID", "")

# ------------------------------------------------------------------------------
# Azure Speech Services
# ------------------------------------------------------------------------------
AZURE_SPEECH_REGION: str = os.getenv("AZURE_SPEECH_REGION", "")
AZURE_SPEECH_ENDPOINT: str = os.getenv("AZURE_SPEECH_ENDPOINT") or os.environ.get(
    "AZURE_OPENAI_STT_TTS_ENDPOINT", ""
)
AZURE_SPEECH_KEY: str = os.getenv("AZURE_SPEECH_KEY") or os.environ.get(
    "AZURE_OPENAI_STT_TTS_KEY", ""
)
AZURE_SPEECH_RESOURCE_ID: str = os.getenv("AZURE_SPEECH_RESOURCE_ID", "")
# ------------------------------------------------------------------------------
# Azure Communication Services (ACS)
# ------------------------------------------------------------------------------
ACS_ENDPOINT: str = os.getenv("ACS_ENDPOINT", "")
ACS_CONNECTION_STRING: str = os.getenv("ACS_CONNECTION_STRING", "")
ACS_SOURCE_PHONE_NUMBER: str = os.getenv("ACS_SOURCE_PHONE_NUMBER", "")
BASE_URL: str = os.getenv("BASE_URL", "")

# Blob Container URL for recording storage
AZURE_STORAGE_CONTAINER_URL: str = os.getenv("AZURE_STORAGE_CONTAINER_URL", "")

# ACS_STREAMING_MODE: StreamMode = StreamMode.MEDIA
ACS_STREAMING_MODE: StreamMode = StreamMode(
    os.getenv("ACS_STREAMING_MODE", "media").lower()
)


# V1 Endpoints
ACS_CALL_OUTBOUND_PATH: str = "/api/v1/calls/initiate"
ACS_CALL_INBOUND_PATH: str = "/api/v1/calls/answer"
ACS_CALL_CALLBACK_PATH: str = "/api/v1/calls/callbacks"

# V1 WebSocket Endpoints
ACS_WEBSOCKET_PATH: str = "/api/v1/media/stream"

# Legacy Endpoint Configs
# # API route fragments
# ACS_CALL_OUTBOUND_PATH: str = "/api/call/initiate"
# ACS_CALL_INBOUND_PATH:  str = "/api/call/answer"
# ACS_CALL_CALLBACK_PATH: str = "/api/call/callbacks"

# # WSS route fragments
# ACS_WEBSOCKET_PATH:     str = "/ws/call/stream"

# Cosmos DB
AZURE_COSMOS_CONNECTION_STRING: str = os.getenv("AZURE_COSMOS_CONNECTION_STRING", "")
AZURE_COSMOS_DATABASE_NAME: str = os.getenv("AZURE_COSMOS_DATABASE_NAME", "")
AZURE_COSMOS_COLLECTION_NAME: str = os.getenv("AZURE_COSMOS_COLLECTION_NAME", "")


# ------------------------------------------------------------------------------
# Azure Identity / Authentication
# ------------------------------------------------------------------------------
ENABLE_AUTH_VALIDATION: bool = os.getenv("ENABLE_AUTH_VALIDATION", "false").lower() in (
    "true",
    "1",
    "yes",
    "on",
)
BACKEND_AUTH_CLIENT_ID: str = os.getenv("BACKEND_AUTH_CLIENT_ID", "")

ENTRA_JWKS_URL = (
    f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/discovery/v2.0/keys"
)
ENTRA_ISSUER = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}/v2.0"
ENTRA_AUDIENCE = f"api://{BACKEND_AUTH_CLIENT_ID}"
ENTRA_EXEMPT_PATHS = [
    ACS_CALL_CALLBACK_PATH,
    ACS_WEBSOCKET_PATH,
    "/health",
    "/readiness",
]

# ACS Websocket and HTTP Callback Auth Config
ACS_JWKS_URL = "https://acscallautomation.communication.azure.com/calling/keys"
ACS_ISSUER = "https://acscallautomation.communication.azure.com"
ACS_AUDIENCE = os.getenv("ACS_AUDIENCE", "")  # ACS Immutable Resource ID

# Allowed client IDs (GUIDs) from environment variable, comma-separated
ALLOWED_CLIENT_IDS: List[str] = [
    cid.strip() for cid in os.getenv("ALLOWED_CLIENT_IDS", "").split(",") if cid.strip()
]

# ------------------------------------------------------------------------------
# SST behaviour
# ------------------------------------------------------------------------------
# Transcribe Audio Configuration
RATE: int = 16000  # Sample rate for audio processing
# Channels for audio processing
CHANNELS: int = 1  # Mono audio
FORMAT: int = 16  # PCM16 format for audio
# Chunk size for audio processing
CHUNK: int = 1024  # Size of audio chunks to process
# ------------------------------------------------------------------------------
GREETING: str = """Hi there from XYZ Insurance! What can I help you with today?"""
VAD_SEMANTIC_SEGMENTATION: bool = False  # Use semantic segmentation for VAD
SILENCE_DURATION_MS: int = 1300  # Duration of silence to end speech detection
RECOGNIZED_LANGUAGE: List[str] = [
    "en-US",
    "es-ES",
    "fr-FR",
    "ko-KR",
    "it-IT",
]  # Default language for speech recognition
AUDIO_FORMAT: str = "pcm"  # Audio format for speech recognition

# AGENT CONFIG
AGENT_AUTH_CONFIG: str = (
    "apps/rtagent/backend/src/agents/agent_store/auth_agent.yaml"  # Default agent name
)
AGENT_CLAIM_INTAKE_CONFIG: str = (
    "apps/rtagent/backend/src/agents/agent_store/claim_intake_agent.yaml"
)
AGENT_GENERAL_INFO_CONFIG: str = (
    "apps/rtagent/backend/src/agents/agent_store/general_info_agent.yaml"
)

# ------------------------------------------------------------------------------
# TTS behaviour
# ------------------------------------------------------------------------------
# Cache for loaded voices to avoid repeated file reads
_voice_cache = {}


# Load voice from agent config YAML file
def get_agent_voice(agent_config_path: str) -> str:
    """Extract voice from agent YAML configuration. Cached to avoid repeated file reads."""
    # Return cached voice if already loaded
    if agent_config_path in _voice_cache:
        return _voice_cache[agent_config_path]

    try:
        logger.info(f"Loading agent voice from: {agent_config_path}")
        with open(agent_config_path, "r", encoding="utf-8") as file:
            agent_config = yaml.safe_load(file)
            voice_config = agent_config.get("voice", {})
            if isinstance(voice_config, dict):
                voice_name = voice_config.get("voice_name") or voice_config.get("name")
                if voice_name:
                    _voice_cache[agent_config_path] = voice_name
                    return voice_name
            elif isinstance(voice_config, str):
                _voice_cache[agent_config_path] = voice_config
                return voice_config
        logger.warning("No valid voice configuration found, using default")
        _voice_cache[agent_config_path] = "en-US-AvaMultilingualNeural"
        return "en-US-AvaMultilingualNeural"
    except FileNotFoundError as e:
        logger.error(f"Agent config file not found: {agent_config_path} - {e}")
        _voice_cache[agent_config_path] = "en-US-AvaMultilingualNeural"
        return "en-US-AvaMultilingualNeural"
    except Exception as e:
        logger.error(f"Failed to load voice from {agent_config_path}: {e}")
        _voice_cache[agent_config_path] = "en-US-AvaMultilingualNeural"
        return "en-US-AvaMultilingualNeural"


# Get voice from environment variable or auth agent config
GREETING_VOICE_TTS = get_agent_voice(AGENT_AUTH_CONFIG)
# en-US-AvaMultilingualNeural4 (Female)
# en-US-AndrewMultilingualNeural4 (Male)
# en-US-EmmaMultilingualNeural4 (Female)
# en-US-AlloyTurboMultilingualNeural4 (Male)
# en-US-EchoTurboMultilingualNeural4 (Male)
# en-US-FableTurboMultilingualNeural4 (Neutral)
# en-US-OnyxTurboMultilingualNeural4 (Male)
# en-US-NovaTurboMultilingualNeural4 (Female)
# en-US-ShimmerTurboMultilingualNeural4 (Female)
# en-US-BrianMultilingualNeural4 (Male)
# en-US-AvaNeural (Female)
# en-US-AndrewNeural (Male)
# en-US-EmmaNeural (Female)
# ------------------------------------------------------------------------------
# Premium Neural HD Voices:
# - only available in Central India, East Asia, East US, Southeast Asia, and West US.
# ============================================================================ #
# en-US-Adam:DragonHDLatestNeural (Male)
# en-US-Andrew:DragonHDLatestNeural (Male)
# en-US-Andrew2:DragonHDLatestNeural (Male)
# en-US-Ava:DragonHDLatestNeural (Female)
# en-US-Brian:DragonHDLatestNeural (Male)
# en-US-Davis:DragonHDLatestNeural (Male)
# en-US-Emma:DragonHDLatestNeural (Female)
# en-US-Emma2:DragonHDLatestNeural (Female)
# en-US-Steffan:DragonHDLatestNeural (Male)
# en-US-Alloy:DragonHDLatestNeural1 (Male)
# en-US-Andrew3:DragonHDLatestNeural1 (Male)
# en-US-Aria:DragonHDLatestNeural1 (Female)
# en-US-Ava3:DragonHDLatestNeural1 (Female)
# en-US-Bree:DragonHDLatestNeural1 (Female)
# en-US-Jenny:DragonHDLatestNeural1 (Female)
# en-US-MultiTalker-Ava-Andrew:DragonHDLatestNeural1 (Neutral)
# en-US-MultiTalker-Ava-Steffan:DragonHDLatestNeural1 (Neutral)
# en-US-Nova:DragonHDLatestNeural1 (Female)
# en-US-Phoebe:DragonHDLatestNeural1 (Female)
# en-US-Serena:DragonHDLatestNeural1 (Female)
# -------------------------------------------------------------------------------
# Standard Neural Voices:
# ============================================================================= #
# en-US-AvaMultilingualNeural (Female)
# en-US-AndrewMultilingualNeural (Male)
# en-US-EmmaMultilingualNeural (Female)
# en-US-BrianMultilingualNeural (Male)

# ------------------------------------------------------------------------------
# Available OpenAI/Turbo Voices (North Central US is one of only 2 regions that support these):
# ============================================================================= #
# en-US-AlloyTurboMultilingualNeural (Male)
# en-US-EchoTurboMultilingualNeural (Male)
# en-US-FableTurboMultilingualNeural (Neutral)
# en-US-OnyxTurboMultilingualNeural (Male)
# en-US-NovaTurboMultilingualNeural (Female)
# en-US-ShimmerTurboMultilingualNeural (Female)
# ------------------------------------------------------------------------------
STOP_WORDS: List[str] = ["goodbye", "exit", "see you later", "bye"]
# Character(s) that mark a chunk boundary for TTS streaming:
TTS_END: set[str] = {";", ".", "?", "!"}

# Allowed CORS origins for the FastAPI app:
ALLOWED_ORIGINS: list[str] = ["*"]
