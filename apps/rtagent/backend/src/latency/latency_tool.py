# latency_tool.py - Clean Production Version
import time
from typing import Any, Dict, Optional, TYPE_CHECKING

from utils.ml_logging import get_logger

if TYPE_CHECKING:
    from src.latency.minimal_suite import MinimalLatencyTracker

logger = get_logger("latency_tool")


class LatencyTool:
    """Production latency tool using MinimalLatencyTracker for persistence."""
    
    def __init__(self, cm, minimal_tracker: Optional['MinimalLatencyTracker'] = None):
        self.cm = cm
        self._inflight: Dict[str, float] = {}
        
        # Use provided tracker or auto-detect from MemoManager
        self.minimal_tracker = minimal_tracker or getattr(cm, '_latency_tracker', None)
        
        if self.minimal_tracker:
            logger.info(f"LatencyTool initialized with persistent tracking for session {getattr(self.minimal_tracker, 'session_id', 'unknown')}")
        else:
            logger.warning("LatencyTool initialized without persistent tracking - latency data will not persist")

    def start(self, stage: str) -> None:
        """Mark the beginning of a latency measurement stage."""
        self._inflight[stage] = time.perf_counter()

    def stop(self, stage: str, redis_mgr=None) -> None:
        """Mark the end of a stage and persist the measurement."""
        start = self._inflight.pop(stage, None)
        if start is None:
            logger.warning(f"stop({stage}) called without matching start")
            return

        end = time.perf_counter()
        duration_ms = (end - start) * 1000
        
        # Track in persistent minimal tracker
        if self.minimal_tracker:
            self.minimal_tracker.track_tool(stage, duration_ms)
            
            # Auto-persist if redis manager provided
            if redis_mgr and hasattr(self.cm, 'persist_to_redis'):
                try:
                    self.cm.persist_to_redis(redis_mgr)
                    logger.debug(f"Persisted latency data after {stage}")
                except Exception as e:
                    logger.error(f"Failed to persist session: {e}")
        
        logger.info(f"{stage} latency: {duration_ms:.2f}ms")

    def track(self, component: str):
        """Get async context manager for tracking a component."""
        if self.minimal_tracker:
            return self.minimal_tracker.track(component)
        elif hasattr(self.cm, 'track'):
            return self.cm.track(component)
        else:
            logger.warning(f"No tracker available for component: {component}")
            return None

    def get_summary(self) -> Optional[Dict[str, Any]]:
        """Get latency summary from persistent tracker."""
        if self.minimal_tracker:
            try:
                return self.minimal_tracker.get_summary()
            except Exception as e:
                logger.error(f"Failed to get latency summary: {e}")
        
        # Fallback to legacy summary if available
        if hasattr(self.cm, 'latency_summary'):
            try:
                return self.cm.latency_summary()
            except Exception as e:
                logger.error(f"Failed to get legacy summary: {e}")
        
        return None
