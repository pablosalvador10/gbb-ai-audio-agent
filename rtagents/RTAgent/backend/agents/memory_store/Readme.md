## Memory Architecture Overview

### 1. Turn-level Cache (Redis)

- **Purpose:** Supports the STT → LLM → TTS loop with ultra-low-latency access, ensuring the hot path is never blocked.
- **Implementation:** Use an in-memory cache (Redis or even a Python `dict` inside a FastAPI worker).
- **TTL:** Set a 60-second TTL; the orchestrator drains the cache into Cosmos DB when the call ends.

---

### 2. Conversation History (Cosmos DB)

**Requirements:**
- Low write-latency for every user utterance
- Ordered retrieval for chat prompt reconstruction
- Global distribution (for scaling across multiple AKS regions)

**Benefits:**
- Single-digit millisecond point reads
- Automatic TTL management
- Virtually unlimited RU throughput

> A typical 20-message prompt (~6 kB JSON) is fetched in one indexed query—comfortably < 15 ms p95 even at 1K RPS. This is faster and more cost-effective than querying Search for every turn.

---

### 3. Long-term Agent Memory (Vector Store)

- **Purpose:** Enables semantic (not key-based) lookup.
- **Implementation:** Use AI Search vector indexing, which supports:
    - Cosine/dot-product similarity on 1–2K-dimension vectors
    - Hybrid (BM25 + vector) queries in a single call
    - Sharding up to billions of documents

**Performance:**
- 50–80 ms median query cost (acceptable since it’s called once per user turn or less frequently, capped at top-K results)
- Persist canonical text in the same Search index or in Blob/Cosmos DB—retrieval latency is negligible compared to vector scoring.
