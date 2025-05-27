"""Centralised connection pools used by ConversationMemory.

* **REDIS**  – hot working‑set, expiring keys keep RAM usage bounded.
* **COSMOS** – durable long‑term store (optional – skip if creds missing).
"""

from __future__ import annotations

import os
from pathlib import Path

import redis.asyncio as aioredis
from azure.cosmos.aio import CosmosClient

# --------------------------------------------------------------------------- #
#  Redis                                                                    #
# --------------------------------------------------------------------------- #

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS = aioredis.from_url(
    REDIS_URL,
    decode_responses=True,
    max_connections=20,
)

# --------------------------------------------------------------------------- #
#  Cosmos DB (NoSQL API)                                                     #
# --------------------------------------------------------------------------- #

_COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
_COSMOS_KEY = os.getenv("COSMOS_KEY")
_COSMOS_DB = os.getenv("COSMOS_DB", "rtvoice")
_COSMOS_CONTAINER = os.getenv("COSMOS_CONTAINER", "history")

COSMOS: CosmosClient | None = None
if _COSMOS_ENDPOINT and _COSMOS_KEY:
    COSMOS = CosmosClient(_COSMOS_ENDPOINT, credential=_COSMOS_KEY)

# Utility so other modules can import the DB/container names without reading env
COSMOS_DB_NAME = _COSMOS_DB
COSMOS_CONTAINER_NAME = _COSMOS_CONTAINER
