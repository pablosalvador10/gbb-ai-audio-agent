"""
services/openai_client.py
-------------------------
Single shared Azure OpenAI client.  Import `client` anywhere you need
to talk to the Chat Completion API; it will be created once at
import-time.
"""

from openai import AzureOpenAI
from rtagents.RTMedAgent.backend.settings import (
    AZURE_OPENAI_ENDPOINT,
    AZURE_OPENAI_KEY,
)

client = AzureOpenAI(
    api_version="2025-02-01-preview",
    azure_endpoint=AZURE_OPENAI_ENDPOINT,
    api_key=AZURE_OPENAI_KEY,
)

__all__ = ["client"]
