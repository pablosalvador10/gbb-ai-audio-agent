# latency_tool.py
import time
from typing import Any, Dict, List

from utils.ml_logging import get_logger

logger = get_logger("latency_tool")


class LatencyTool:
    def __init__(self, cm):
        self.cm = cm
        self._inflight: Dict[str, float] = {}

    # ------------------------------------------------------------------ #
    def start(self, stage: str) -> None:
        """Mark the beginning of *stage*."""
        self._inflight[stage] = time.perf_counter()

    def stop(self, stage: str, redis_mgr) -> None:
        """
        Mark the end of *stage* and immediately push the sample
        into Redis for real-time monitoring.
        """
        start = self._inflight.pop(stage, None)
        if start is None:
            logger.warning(
                "[LatencyTool] stop(%s) called without matching start", stage
            )
            return

        end = time.perf_counter()
        self.cm.note_latency(stage, start, end)
        self.cm.persist_to_redis(redis_mgr)
        logger.info("[LatencyTool] %s latency: %.3f s", stage, end - start)

    # convenience – fetch already-summarised numbers
    def summary(self):
        return self.cm.latency_summary()
