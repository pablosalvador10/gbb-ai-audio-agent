"""memory_modules.py

Low‑latency memory back‑plane used by the voice‑agent agents.

* **Redis** (in‑memory, TTL ≤ 60 s)  – hot working‑set for each live call.
* **Cosmos DB** (NoSQL API)          – durable conversation history.

`ConversationMemory` offers an *async* CRUD interface that agents can use
without caring which store is underneath.  The class automatically writes to
Redis first and lazily persists the final timeline to Cosmos on `flush()`.

Dependencies
------------
    pip install redis[asyncio] azure-cosmos
"""
