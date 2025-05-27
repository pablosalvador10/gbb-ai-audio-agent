"""ConversationMemory – shared timeline for a single call.

• **append()**   – O(1) push to Redis.
• **history()**  – single LRANGE fetch (ordered list of dicts).
• **flush()**    – one upsert to Cosmos at call end for durability.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, List

from azure.cosmos.exceptions import CosmosResourceNotFoundError

from .stores import COSMOS, COSMOS_CONTAINER_NAME, COSMOS_DB_NAME, REDIS

__all__ = ["ConversationMemory"]


class ConversationMemory:
    """Short‑term, low‑latency message buffer **per conversation**."""

    _TTL_SEC = 120  # Redis key idle expiry

    def __init__(self, cid: str):
        self.cid = cid
        self._redis_key = f"{cid}:hist"

    # ---------------------------- write path ----------------------------- #

    async def append(self, role: str, content: str | None, **extra: Any) -> None:
        doc = {
            "ts": datetime.utcnow().isoformat(timespec="milliseconds"),
            "role": role,
            "content": content,
            **extra,
        }
        await REDIS.rpush(self._redis_key, json.dumps(doc))
        await REDIS.expire(self._redis_key, self._TTL_SEC)

    # ----------------------------- read path ----------------------------- #

    async def history(self) -> List[Dict[str, Any]]:
        """Return current chat list in order."""
        raw = await REDIS.lrange(self._redis_key, 0, -1)
        return [json.loads(x) for x in raw]

    # ------------------------- durability hook --------------------------- #

    async def flush(self) -> None:
        """Persist Redis list → Cosmos DB (NoSQL)."""
        if COSMOS is None:
            return  # not configured – skip

        items = await self.history()
        if not items:
            return

        container = COSMOS.get_database_client(COSMOS_DB_NAME).get_container_client(
            COSMOS_CONTAINER_NAME
        )
        body = {"id": self.cid, "messages": items}
        try:
            await container.upsert_item(body)
        except CosmosResourceNotFoundError:  # pragma: no cover
            raise RuntimeError(
                "Cosmos container not found – ensure infra bootstrap ran."
            )

    async def clear(self) -> None:
        """Delete the Redis working set (called after successful flush)."""
        await REDIS.delete(self._redis_key)
