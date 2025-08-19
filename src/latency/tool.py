from __future__ import annotations

import time
from typing import Any, Dict, Optional, Callable
from contextlib import contextmanager, asynccontextmanager

from utils.ml_logging import get_logger
from src.latency.tool_suite import LatencyTracker

logger = get_logger("latency_tool")


class LatencyTool:
    """
    Thin wrapper that:
      - exposes start/stop for imperative use,
      - gives sync/async context managers, and
      - persists aggregates via MemoManager → Redis.
    """

    def __init__(self, memo_manager: Any, tracker: Optional[LatencyTracker] = None):
        self.cm = memo_manager
        self.tracker: LatencyTracker = tracker or getattr(memo_manager, "_latency_tracker", None)
        self._inflight: Dict[str, float] = {}
        if self.tracker:
            logger.info(f"LatencyTool ready for session {self.tracker.session_id}")
        else:
            logger.warning("LatencyTool initialized without tracker — no persistence.")

    # ---- imperative ---------------------------------------------------
    def start(self, stage: str) -> None:
        self._inflight[stage] = time.perf_counter()

    def stop(self, stage: str, redis_mgr: Optional[Any] = None) -> float:
        start = self._inflight.pop(stage, None)
        if start is None:
            logger.warning(f"stop({stage}) called without matching start()")
            return 0.0
        dur_ms = (time.perf_counter() - start) * 1000.0
        if self.tracker:
            self.tracker.record(stage, dur_ms)
            # small: only persist aggregates; avoid blocking
            if redis_mgr:
                try:
                    self.cm.persist_to_redis(redis_mgr)
                except Exception as e:
                    logger.error(f"Persist error after {stage}: {e}")
        logger.info(f"{stage} latency: {dur_ms:.2f} ms")
        return dur_ms

    # ---- contexts -----------------------------------------------------
    @contextmanager
    def stage(self, component: str):
        if self.tracker:
            with self.tracker.stage(component):
                yield
        else:
            yield

    @asynccontextmanager
    async def stage_async(self, component: str):
        if self.tracker:
            async with self.tracker.stage_async(component):
                yield
        else:
            yield

    # ---- decorators ---------------------------------------------------
    def measure(self, component: str) -> Callable:
        if not self.tracker:
            def _noop(fn): return fn
            return _noop
        return self.tracker.measure(component)

    # ---- summaries ----------------------------------------------------
    def summary(self) -> Optional[Dict[str, Any]]:
        return self.tracker.get_summary() if self.tracker else None
