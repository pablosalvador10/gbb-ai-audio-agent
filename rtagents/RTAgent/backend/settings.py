"""
utils/settings.py
=================
Central place for every environment variable, constant path, and
used by the voice-agent service.
"""

from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime
from typing import List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ------------------------------------------------------------------------------
# Azure OpenAI
# ------------------------------------------------------------------------------
AZURE_OPENAI_ENDPOINT: str = os.getenv("AZURE_OPENAI_ENDPOINT", "")
AZURE_OPENAI_KEY: str = os.getenv("AZURE_OPENAI_KEY", "")
AZURE_OPENAI_CHAT_DEPLOYMENT_ID: str = os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT_ID", "")
AOAI_STT_KEY = os.environ["AZURE_OPENAI_STT_TTS_KEY"]
AOAI_STT_ENDPOINT = os.environ["AZURE_OPENAI_STT_TTS_ENDPOINT"]

# ------------------------------------------------------------------------------
# Azure Communication Services (ACS)
# ------------------------------------------------------------------------------
ACS_CONNECTION_STRING: str = os.getenv("ACS_CONNECTION_STRING", "")
ACS_SOURCE_PHONE_NUMBER: str = os.getenv("ACS_SOURCE_PHONE_NUMBER", "")
BASE_URL: str = os.getenv("BASE_URL", "")

# API route fragments (keep them in one place so routers can import)
ACS_CALL_PATH = "/api/call"
ACS_CALLBACK_PATH: str = "/call/callbacks"
ACS_WEBSOCKET_PATH: str = "/call/stream"

# Cosmos DB
AZURE_COSMOS_CONNECTION_STRING: str = os.getenv("AZURE_COSMOS_CONNECTION_STRING", "")
AZURE_COSMOS_DB_DATABASE_NAME: str = os.getenv("AZURE_COSMOS_DB_DATABASE_NAME", "")
AZURE_COSMOS_DB_COLLECTION_NAME: str = os.getenv("AZURE_COSMOS_DB_COLLECTION_NAME", "")

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
# VAD (Voice Activity Detection) settings
VAD_THRESHOLD: float = 0.5  # Threshold for VAD to detect speech
# Prefix padding in milliseconds to avoid cutting off speech
PREFIX_PADDING_MS: int = 400  # Padding before speech starts
# Silence duration in milliseconds to consider the end of speech
SILENCE_DURATION_MS: int = 1300  # Duration of silence to end speech detection

# ------------------------------------------------------------------------------
# TTS behaviour
# ------------------------------------------------------------------------------
VOICE_TTS = "en-US-JennyMultilingualNeural"

# ------------------------------------------------------------------------------
STOP_WORDS: List[str] = ["goodbye", "exit", "see you later", "bye"]
# Character(s) that mark a chunk boundary for TTS streaming:
TTS_END: set[str] = {".", "!", "?", ";"}

# Allowed CORS origins for the FastAPI app:
ALLOWED_ORIGINS: list[str] = [
    "http://localhost",
    "http://127.0.0.1",
    "https://localhost",
    "https://127.0.0.1",
    "http://localhost:5173",
    "http://127.0.0.1:5173",
    "https://localhost:5173",
    "https://127.0.0.1:5173",
]
