"""
MemoManager — conversation state + live refresh + latency aggregates (Suite v2)

Responsibilities
---------------
• Owns per-session memory: CoreMemory, ChatHistory, message queue flags
• Syncs to/from Redis (small payloads; aggregates only for latency)
• Exposes lightweight latency helpers (sync/async stages, turn correlation)
• Optional auto-refresh loop to keep local state in sync with Redis

Notes
-----
• Latency data is stored under key 'latency_data' with aggregates only
  (count/avg/min/max/p50/p90/p95/p99 per component & per agent).
• No raw samples are persisted to avoid large Redis values.
• 'track(...)' is an async context manager; 'track_sync(...)' is sync.
• Use 'start_turn(agent="voice_rta")' to correlate samples to turns/agents.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections import deque
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Dict, List, Optional

from utils.ml_logging import get_logger
from src.agenticmemory.playback_queue import MessageQueue
from src.agenticmemory.types import ChatHistory, CoreMemory
from src.redis.manager import AzureRedisManager
from src.latency.tool_suite import LatencyTracker

logger = get_logger("src.stateful.memo_manager")


__all__ = ["MemoManager"]


class MemoManager:
    """
    Owns a conversation session: core memory, chat history & runtime state.

    Key features:
    - Small Redis footprint (aggregates for latency, JSON for history/context)
    - Sync + async persistence APIs
    - Live refresh (manual or automatic)
    - Latency Suite v2 integration (p50/p90/p95/p99, per-component & per-agent)
    """

    _CORE_KEY = "corememory"
    _HISTORY_KEY = "chat_history"
    _LATENCY_KEY = "latency_data"

    def __init__(
        self,
        session_id: Optional[str] = None,
        auto_refresh_interval: Optional[float] = None,
        redis_mgr: Optional[AzureRedisManager] = None,
        enable_latency: bool = True,
    ) -> None:
        self.session_id: str = session_id or str(uuid.uuid4())[:8]
        self.chatHistory: ChatHistory = ChatHistory()
        self.corememory: CoreMemory = CoreMemory()

        # Message queue (TTS playback etc.)
        self.message_queue = MessageQueue()
        self._is_tts_interrupted: bool = False

        # Live refresh plumbing
        self.auto_refresh_interval = auto_refresh_interval
        self.last_refresh_time: float = 0.0
        self._refresh_task: Optional[asyncio.Task] = None
        self._redis_manager: Optional[AzureRedisManager] = redis_mgr

        # Latency tracker (Suite v2)
        self._latency_tracker: Optional[LatencyTracker] = (
            LatencyTracker(self.session_id) if enable_latency else None
        )
        logger.info(
            "MemoManager init: session=%s latency=%s",
            self.session_id,
            "on" if self._latency_tracker else "off",
        )

    # ------------------------------------------------------------------
    # Latency helpers (Suite v2)
    # ------------------------------------------------------------------
    @property
    def latency_tracker(self) -> Optional[LatencyTracker]:
        """Access the underlying LatencyTracker (if enabled)."""
        return self._latency_tracker

    def start_turn(self, turn_id: str = "", agent: str = "") -> str:
        """
        Start a new logical turn and set agent correlation for subsequent samples.
        Returns the effective turn_id (may be auto-generated).
        """
        if self._latency_tracker:
            return self._latency_tracker.start_turn(turn_id or None, agent)
        return ""

    def track_tool(self, tool_name: str, duration_ms: float) -> None:
        """Record an explicit duration for a tool execution."""
        if self._latency_tracker:
            self._latency_tracker.track_tool(tool_name, duration_ms)

    def get_latency_summary(self) -> Dict[str, Any]:
        """
        Return the aggregate latency summary (no raw samples).
        Includes per-component percentiles & per-agent aggregates.
        """
        if not self._latency_tracker:
            return {
                "session_id": self.session_id,
                "enabled": False,
                "total_measurements": 0,
            }
        return self._latency_tracker.get_summary(include_raw=False)

    # Context managers for instrumentation
    def track(self, component: str):
        """
        Async context manager for a pipeline component.

        Example:
            async with mm.track("llm_infer"):
                reply = await llm.infer(text)
        """
        if self._latency_tracker:
            return self._latency_tracker.stage_async(component)

        @asynccontextmanager
        async def _noop():
            yield

        return _noop()

    def track_sync(self, component: str):
        """
        Sync context manager for a pipeline component.

        Example:
            with mm.track_sync("turn_e2e"):
                run_turn()
        """
        if self._latency_tracker:
            @contextmanager
            def _cm():
                with self._latency_tracker.stage(component):
                    yield
            return _cm()

        @contextmanager
        def _noop():
            yield

        return _noop()

    # ------------------------------------------------------------------
    # History & CoreMemory sugar
    # ------------------------------------------------------------------
    @property
    def histories(self) -> Dict[str, List[Dict[str, str]]]:
        """All message threads by agent (read-write)."""
        return self.chatHistory.get_all()

    @histories.setter
    def histories(self, value: Dict[str, List[Dict[str, str]]]) -> None:
        self.chatHistory._threads = value  # trusted internal assignment

    @property
    def context(self) -> Dict[str, Any]:
        """Core memory store (read-write)."""
        return self.corememory._store

    @context.setter
    def context(self, value: Dict[str, Any]) -> None:
        self.corememory._store = value

    @property
    def history(self) -> ChatHistory:
        """Single-history alias for minimal diffs elsewhere."""
        return self.chatHistory

    def append_to_history(self, agent: str, role: str, content: str) -> None:
        self.history.append(role, content, agent)

    def get_history(self, agent_name: str) -> List[Dict[str, str]]:
        return self.history.get_agent(agent_name)

    def clear_history(self, agent_name: Optional[str] = None) -> None:
        self.history.clear(agent_name)

    # ------------------------------------------------------------------
    # Slots & tool outputs (convenience over corememory)
    # ------------------------------------------------------------------
    def update_slots(self, slots: Dict[str, Any]) -> None:
        if not slots:
            return
        current = self.corememory.get("slots", {})
        current.update(slots)
        self.corememory.set("slots", current)
        logger.debug("Updated slots: %s", list(slots.keys()))

    def get_slot(self, name: str, default: Any = None) -> Any:
        return self.corememory.get("slots", {}).get(name, default)

    def persist_tool_output(self, tool_name: str, result: Dict[str, Any]) -> None:
        if not tool_name or not result:
            return
        tool_outputs = self.corememory.get("tool_outputs", {})
        tool_outputs[tool_name] = result
        self.corememory.set("tool_outputs", tool_outputs)
        logger.debug("Persisted tool output for '%s'", tool_name)

    def get_tool_output(self, tool_name: str, default: Any = None) -> Any:
        return self.corememory.get("tool_outputs", {}).get(tool_name, default)

    # ------------------------------------------------------------------
    # TTS interrupt flag (local + optional live)
    # ------------------------------------------------------------------
    def is_tts_interrupted(self) -> bool:
        return self._is_tts_interrupted

    def set_tts_interrupted(self, value: bool) -> None:
        self.set_context("tts_interrupted", value)
        self._is_tts_interrupted = value

    async def set_tts_interrupted_live(
        self, redis_mgr: Optional[AzureRedisManager], session_id: str, value: bool
    ) -> None:
        await self.set_live_context_value(
            redis_mgr or self._redis_manager, f"tts_interrupted:{session_id}", value
        )

    async def is_tts_interrupted_live(
        self,
        redis_mgr: Optional[AzureRedisManager] = None,
        session_id: Optional[str] = None,
    ) -> bool:
        if redis_mgr and session_id:
            self._is_tts_interrupted = await self.get_live_context_value(
                redis_mgr, f"tts_interrupted:{session_id}", False
            )
            return self._is_tts_interrupted
        return self.get_context(f"tts_interrupted:{session_id}", False)

    # ------------------------------------------------------------------
    # CoreMemory helpers
    # ------------------------------------------------------------------
    def get_context(self, key: str, default: Any = None) -> Any:
        return self.corememory.get(key, default)

    def set_context(self, key: str, value: Any) -> None:
        self.corememory.set(key, value)

    def update_context(self, key: str, value: Any) -> None:
        current = self.corememory.get(key)
        if isinstance(current, dict) and isinstance(value, dict):
            current.update(value)
            self.corememory.set(key, current)
        else:
            self.corememory.set(key, value)

    def ensure_system_prompt(self, agent_name: str, system_prompt: str) -> None:
        history = self.histories.setdefault(agent_name, [])
        if not history or history[0].get("role") != "system":
            history.insert(0, {"role": "system", "content": system_prompt})
        else:
            history[0]["content"] = system_prompt

    def get_value_from_corememory(self, key: str, default: Any = None) -> Any:
        return self.corememory.get(key, default)

    def set_corememory(self, key: str, value: Any) -> None:
        self.corememory.set(key, value)

    def update_corememory(self, key: str, value: Any) -> None:
        self.corememory.set(key, value)

    # ------------------------------------------------------------------
    # MessageQueue passthroughs
    # ------------------------------------------------------------------
    async def enqueue_message(
        self,
        response_text: str,
        use_ssml: bool = False,
        voice_name: Optional[str] = None,
        locale: str = "en-US",
        participants: Optional[List[Any]] = None,
        max_retries: int = 5,
        initial_backoff: float = 0.5,
        transcription_resume_delay: float = 1.0,
    ) -> None:
        message_data = {
            "response_text": response_text,
            "use_ssml": use_ssml,
            "voice_name": voice_name,
            "locale": locale,
            "participants": participants,
            "max_retries": max_retries,
            "initial_backoff": initial_backoff,
            "transcription_resume_delay": transcription_resume_delay,
            "timestamp": asyncio.get_event_loop().time(),
        }
        await self.message_queue.enqueue(message_data)

    async def get_next_message(self) -> Optional[Dict[str, Any]]:
        return await self.message_queue.dequeue()

    async def clear_queue(self) -> None:
        await self.message_queue.clear()

    def get_queue_size(self) -> int:
        return self.message_queue.size()

    async def set_queue_processing_status(self, is_processing: bool) -> None:
        await self.message_queue.set_processing(is_processing)

    def is_queue_processing(self) -> bool:
        return self.message_queue.is_processing_queue()

    async def set_media_cancelled(self, cancelled: bool) -> None:
        await self.message_queue.set_media_cancelled(cancelled)

    def is_media_cancelled(self) -> bool:
        return self.message_queue.is_media_cancelled()

    async def reset_queue_on_interrupt(self) -> None:
        await self.message_queue.reset_on_interrupt()

    # ------------------------------------------------------------------
    # Redis persistence
    # ------------------------------------------------------------------
    @staticmethod
    def build_redis_key(session_id: str) -> str:
        return f"session:{session_id}"

    def to_redis_dict(self) -> Dict[str, str]:
        """
        Serialize session to Redis. Keeps payloads compact:
        - corememory: JSON string
        - chat_history: JSON string
        - latency_data: aggregates only (no raw samples)
        """
        rd = {
            self._CORE_KEY: self.corememory.to_json(),
            self._HISTORY_KEY: self.chatHistory.to_json(),
        }
        if self._latency_tracker:
            try:
                agg = self._latency_tracker.serialize_for_redis()
                rd[self._LATENCY_KEY] = json.dumps(agg)
            except Exception as e:
                logger.warning("latency serialize failed: %s", e)
        return rd

    @classmethod
    def from_redis(cls, session_id: str, redis_mgr: AzureRedisManager) -> "MemoManager":
        """
        Load a MemoManager from Redis (latency aggregates restored only for
        correlation state; raw samples are not rehydrated).
        """
        key = cls.build_redis_key(session_id)
        data = redis_mgr.get_session_data(key) or {}
        mm = cls(session_id=session_id, redis_mgr=redis_mgr, enable_latency=True)

        if cls._CORE_KEY in data:
            mm.corememory.from_json(data[cls._CORE_KEY])

        if cls._HISTORY_KEY in data:
            mm.chatHistory.from_json(data[cls._HISTORY_KEY])

        if cls._LATENCY_KEY in data and mm._latency_tracker:
            try:
                latency_agg = json.loads(data[cls._LATENCY_KEY])
                mm._latency_tracker.restore_from_redis(latency_agg)
                logger.info("Restored latency context for session %s", session_id)
            except Exception as e:
                logger.warning("Failed to restore latency context: %s", e)

        return mm

    @classmethod
    def from_redis_with_manager(
        cls, session_id: str, redis_mgr: AzureRedisManager
    ) -> "MemoManager":
        """Alias that ensures redis manager is stored on the instance."""
        return cls.from_redis(session_id, redis_mgr)

    def persist_to_redis(
        self, redis_mgr: AzureRedisManager, ttl_seconds: Optional[int] = None
    ) -> None:
        key = self.build_redis_key(self.session_id)
        redis_mgr.store_session_data(key, self.to_redis_dict())
        if ttl_seconds:
            try:
                redis_mgr.redis_client.expire(key, ttl_seconds)  # type: ignore
            except Exception as e:
                logger.debug("expire() not available on redis client: %s", e)

        # Log with tiny latency factoid (count only)
        lat_count = 0
        if self._latency_tracker:
            try:
                lat_count = self._latency_tracker.get_summary().get("total_measurements", 0)
            except Exception:
                lat_count = -1
        logger.info(
            "Persisted session %s — histories: %s, ctx_keys=%s, latency_count=%s",
            self.session_id,
            {a: len(h) for a, h in self.histories.items()},
            list(self.context.keys()),
            lat_count,
        )

    async def persist_to_redis_async(
        self, redis_mgr: AzureRedisManager, ttl_seconds: Optional[int] = None
    ) -> None:
        key = self.build_redis_key(self.session_id)
        await redis_mgr.store_session_data_async(key, self.to_redis_dict())
        if ttl_seconds:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, redis_mgr.redis_client.expire, key, ttl_seconds)  # type: ignore
            except Exception as e:
                logger.debug("async expire not available: %s", e)

        logger.info(
            "Persisted (async) session %s — histories: %s, ctx_keys=%s",
            self.session_id,
            {a: len(h) for a, h in self.histories.items()},
            list(self.context.keys()),
        )

    async def persist(self, redis_mgr: Optional[AzureRedisManager] = None) -> None:
        mgr = redis_mgr or self._redis_manager
        if not mgr:
            raise ValueError("No Redis manager available")
        await self.persist_to_redis_async(mgr)

    async def persist_latency_data(self, redis_mgr: Optional[AzureRedisManager] = None) -> bool:
        """Convenience: persist with emphasis on saving latest latency aggregates."""
        try:
            await self.persist(redis_mgr)
            return True
        except Exception as e:
            logger.error("Failed to persist latency data: %s", e)
            return False

    # ------------------------------------------------------------------
    # Live refresh (manual + auto)
    # ------------------------------------------------------------------
    async def refresh_from_redis_async(self, redis_mgr: AzureRedisManager) -> bool:
        key = self.build_redis_key(self.session_id)
        try:
            data = await redis_mgr.get_session_data_async(key)
            if not data:
                logger.warning("No live data for session %s", self.session_id)
                return False

            if self._HISTORY_KEY in data:
                new_histories = json.loads(data[self._HISTORY_KEY])
                if new_histories != self.histories:
                    self.histories = new_histories
                    logger.info("Refreshed histories for session %s", self.session_id)

            if self._CORE_KEY in data:
                self.context = json.loads(data[self._CORE_KEY])

            if self._LATENCY_KEY in data and self._latency_tracker:
                try:
                    latency_agg = json.loads(data[self._LATENCY_KEY])
                    self._latency_tracker.restore_from_redis(latency_agg)
                except Exception as e:
                    logger.debug("Latency context restore skipped: %s", e)

            logger.info("Refreshed live data for session %s", self.session_id)
            return True
        except Exception as e:
            logger.error("Failed to refresh live data for session %s: %s", self.session_id, e)
            return False

    def refresh_from_redis(self, redis_mgr: AzureRedisManager) -> bool:
        key = self.build_redis_key(self.session_id)
        try:
            data = redis_mgr.get_session_data(key)
            if not data:
                logger.warning("No live data for session %s", self.session_id)
                return False

            if self._HISTORY_KEY in data:
                new_histories = json.loads(data[self._HISTORY_KEY])
                if new_histories != self.histories:
                    self.histories = new_histories
                    logger.info("Refreshed histories for session %s", self.session_id)

            if self._CORE_KEY in data:
                self.context = json.loads(data[self._CORE_KEY])

            if self._LATENCY_KEY in data and self._latency_tracker:
                try:
                    latency_agg = json.loads(data[self._LATENCY_KEY])
                    self._latency_tracker.restore_from_redis(latency_agg)
                except Exception as e:
                    logger.debug("Latency context restore skipped: %s", e)

            logger.info("Refreshed live data for session %s", self.session_id)
            return True
        except Exception as e:
            logger.error("Failed to refresh live data for session %s: %s", self.session_id, e)
            return False

    async def get_live_context_value(
        self, redis_mgr: AzureRedisManager, key: str, default: Any = None
    ) -> Any:
        try:
            redis_key = self.build_redis_key(self.session_id)
            data = await redis_mgr.get_session_data_async(redis_key)
            if data and self._CORE_KEY in data:
                context = json.loads(data[self._CORE_KEY])
                return context.get(key, default)
            return default
        except Exception as e:
            logger.error("get_live_context_value('%s') failed: %s", key, e)
            return default

    async def set_live_context_value(
        self, redis_mgr: AzureRedisManager, key: str, value: Any
    ) -> bool:
        try:
            self.context[key] = value
            await self.persist_to_redis_async(redis_mgr)
            logger.debug("Set live context: %s = %s", key, value)
            return True
        except Exception as e:
            logger.error("set_live_context_value('%s') failed: %s", key, e)
            return False

    def enable_auto_refresh(
        self, redis_mgr: AzureRedisManager, interval_seconds: float = 30.0
    ) -> None:
        self._redis_manager = redis_mgr
        self.auto_refresh_interval = interval_seconds
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
        self._refresh_task = asyncio.create_task(self._auto_refresh_loop())
        logger.info(
            "Enabled auto-refresh every %.1fs for session %s",
            interval_seconds,
            self.session_id,
        )

    def disable_auto_refresh(self) -> None:
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
        self._refresh_task = None
        self._redis_manager = None
        logger.info("Disabled auto-refresh for session %s", self.session_id)

    async def _auto_refresh_loop(self) -> None:
        while self.auto_refresh_interval and self._redis_manager:
            try:
                await asyncio.sleep(self.auto_refresh_interval)
                ok = await self.refresh_from_redis_async(self._redis_manager)
                if ok:
                    self.last_refresh_time = asyncio.get_event_loop().time()
            except asyncio.CancelledError:
                logger.info("Auto-refresh cancelled for session %s", self.session_id)
                break
            except Exception as e:
                logger.error("Auto-refresh error for session %s: %s", self.session_id, e)

    async def check_for_changes(self, redis_mgr: AzureRedisManager) -> Dict[str, bool]:
        changes = {"corememory": False, "chat_history": False, "queue": False}
        try:
            key = self.build_redis_key(self.session_id)
            data = await redis_mgr.get_session_data_async(key)
            if not data:
                return changes

            if self._CORE_KEY in data:
                remote_context = json.loads(data[self._CORE_KEY])
                local_context_clean = {
                    k: v for k, v in self.context.items() if k != "message_queue"
                }
                remote_context_clean = {
                    k: v for k, v in remote_context.items() if k != "message_queue"
                }
                changes["corememory"] = local_context_clean != remote_context_clean
                if "message_queue" in remote_context:
                    remote_queue = remote_context["message_queue"]
                    local_queue = list(self.message_queue.queue)
                    changes["queue"] = local_queue != remote_queue

            if self._HISTORY_KEY in data:
                remote_histories = json.loads(data[self._HISTORY_KEY])
                changes["chat_history"] = self.histories != remote_histories

        except Exception as e:
            logger.error("check_for_changes error (session %s): %s", self.session_id, e)

        return changes

    async def selective_refresh(
        self,
        redis_mgr: AzureRedisManager,
        refresh_context: bool = True,
        refresh_histories: bool = True,
        refresh_queue: bool = False,
    ) -> Dict[str, bool]:
        updated = {"corememory": False, "chat_history": False, "queue": False}
        try:
            key = self.build_redis_key(self.session_id)
            data = await redis_mgr.get_session_data_async(key)
            if not data:
                return updated

            if refresh_context and self._CORE_KEY in data:
                new_context = json.loads(data[self._CORE_KEY])
                if not refresh_queue:
                    new_context.pop("message_queue", None)
                self.context.update(new_context)
                updated["corememory"] = True
                logger.debug("Updated context for session %s", self.session_id)

            if refresh_histories and self._HISTORY_KEY in data:
                self.histories = json.loads(data[self._HISTORY_KEY])
                updated["chat_history"] = True
                logger.debug("Updated histories for session %s", self.session_id)

            if refresh_queue and self._CORE_KEY in data:
                context = json.loads(data[self._CORE_KEY])
                if "message_queue" in context:
                    async with self.message_queue.lock:  # type: ignore[attr-defined]
                        self.message_queue.queue = deque(context["message_queue"])
                        updated["queue"] = True
                        logger.debug("Updated message queue for session %s", self.session_id)

        except Exception as e:
            logger.error("selective_refresh error (session %s): %s", self.session_id, e)

        return updated

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------
    def __del__(self):
        try:
            if hasattr(self, "_refresh_task") and self._refresh_task and not self._refresh_task.done():
                self._refresh_task.cancel()
        except Exception:
            # Avoid destructor exceptions
            pass
