"""
Latency Suite — ultra-light, production-safe latency tracking.
"""

from __future__ import annotations

import os
import time
import math
import threading
import statistics
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Callable, TypeVar, Awaitable
from contextlib import contextmanager, asynccontextmanager

T = TypeVar("T")
ENABLED = os.getenv("LATENCY_SUITE", "true").lower() == "true"
DEFAULT_MAX_MEASUREMENTS = int(os.getenv("LAT_MAX_MEAS", "5000"))
OTEL_ENABLED = os.getenv("LATENCY_OTEL", "false").lower() == "true"

try:
    if OTEL_ENABLED:
        from opentelemetry import trace  # type: ignore
        _tracer = trace.get_tracer("latency-suite-v2")
    else:
        _tracer = None
except Exception:
    _tracer = None
    OTEL_ENABLED = False


@dataclass
class LatencyMeasurement:
    component: str
    duration_ms: float
    turn_id: str = ""
    agent: str = ""


@dataclass
class ComponentStats:
    count: int = 0
    avg_ms: float = 0.0
    min_ms: float = 0.0
    max_ms: float = 0.0
    p50_ms: float = 0.0
    p90_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0

    @classmethod
    def from_values(cls, values: List[float]) -> "ComponentStats":
        if not values:
            return cls()
        values.sort()
        n = len(values)
        return cls(
            count=n,
            avg_ms=round(statistics.mean(values), 2),
            min_ms=round(values[0], 2),
            max_ms=round(values[-1], 2),
            p50_ms=round(_percentile(values, 50), 2),
            p90_ms=round(_percentile(values, 90), 2),
            p95_ms=round(_percentile(values, 95), 2),
            p99_ms=round(_percentile(values, 99), 2),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "count": self.count,
            "avg_ms": self.avg_ms,
            "min_ms": self.min_ms,
            "max_ms": self.max_ms,
            "p50_ms": self.p50_ms,
            "p90_ms": self.p90_ms,
            "p95_ms": self.p95_ms,
            "p99_ms": self.p99_ms,
        }


def _percentile(sorted_vals: List[float], p: float) -> float:
    """Linear interpolation percentile (expects sorted input)."""
    if not sorted_vals:
        return 0.0
    k = (p / 100.0) * (len(sorted_vals) - 1)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return d0 + d1


class RingBuffer:
    """Fixed-size ring buffer (thread-safe append, iterate snapshot for read)."""

    __slots__ = ("_data", "_size", "_idx", "_count", "_lock")

    def __init__(self, size: int):
        self._size = max(1, size)
        self._data: List[Optional[LatencyMeasurement]] = [None] * self._size
        self._idx = 0
        self._count = 0
        self._lock = threading.Lock()

    def append(self, item: LatencyMeasurement) -> None:
        with self._lock:
            self._data[self._idx] = item
            self._idx = (self._idx + 1) % self._size
            self._count = min(self._count + 1, self._size)

    def snapshot(self) -> List[LatencyMeasurement]:
        with self._lock:
            if self._count == 0:
                return []
            # Return in insertion order (oldest->newest)
            start = (self._idx - self._count) % self._size
            out: List[LatencyMeasurement] = []
            for i in range(self._count):
                m = self._data[(start + i) % self._size]
                if m is not None:
                    out.append(m)
            return out


class LatencyTracker:
    """
    Minimal, production-safe latency tracker.

    Use:
        tracker = LatencyTracker(session_id)
        turn_id = tracker.start_turn("turn-1", agent="stt")
        with tracker.stage("stt_capture"):   # sync
            ...
        async with tracker.stage_async("llm_infer"):  # async
            await infer()

        # ad-hoc measurement value
        tracker.record("tool_retriever", 12.3)

        # summary (aggregates only)
        summary = tracker.get_summary()
    """

    def __init__(self, session_id: str, max_measurements: int = DEFAULT_MAX_MEASUREMENTS):
        self.session_id = session_id
        self._measurements = RingBuffer(size=max_measurements)
        self.current_turn_id: str = ""
        self.current_agent: str = ""
        self._overhead_ns = self._calibrate_overhead_ns()

    # ---- turn / agent correlation ------------------------------------------------
    def start_turn(self, turn_id: Optional[str] = None, agent: str = "") -> str:
        if not ENABLED:
            self.current_turn_id, self.current_agent = "", agent
            return ""
        self.current_turn_id = turn_id or f"{self.session_id}:t{int(time.time()*1000)}"
        self.current_agent = agent
        return self.current_turn_id

    # ---- record helpers ----------------------------------------------------------
    def record(self, component: str, duration_ms: float) -> None:
        if not ENABLED:
            return
        self._measurements.append(
            LatencyMeasurement(
                component=component,
                duration_ms=float(duration_ms),
                turn_id=self.current_turn_id,
                agent=self.current_agent,
            )
        )

    def track_tool(self, tool_name: str, duration_ms: float) -> None:
        self.record(f"tool_{tool_name}", duration_ms)

    # ---- context managers (sync + async) ----------------------------------------
    @contextmanager
    def stage(self, component: str):
        """Sync context manager."""
        if not ENABLED:
            yield
            return
        span = _tracer.start_span(component) if _tracer else None  # type: ignore
        start = time.perf_counter_ns()
        try:
            yield
        finally:
            end = time.perf_counter_ns()
            dur_ms = (end - start - self._overhead_ns) / 1_000_000.0
            self.record(component, max(0.0, dur_ms))
            if span:
                span.end()  # type: ignore

    @asynccontextmanager
    async def stage_async(self, component: str):
        """Async context manager."""
        if not ENABLED:
            yield
            return
        span = _tracer.start_span(component) if _tracer else None  # type: ignore
        start = time.perf_counter_ns()
        try:
            yield
        finally:
            end = time.perf_counter_ns()
            dur_ms = (end - start - self._overhead_ns) / 1_000_000.0
            self.record(component, max(0.0, dur_ms))
            if span:
                span.end()  # type: ignore

    # ---- decorators (sync + async) ----------------------------------------------
    def measure(self, component: str) -> Callable[[Callable[..., T]], Callable[..., T]]:
        """
        Decorator for sync or async callables:
            @tracker.measure("llm_infer")
            async def run(...):
                ...
        """
        def _wrap(fn: Callable[..., T]) -> Callable[..., T]:
            if _is_coroutine(fn):
                async def _ac(*args, **kwargs):
                    async with self.stage_async(component):
                        return await fn(*args, **kwargs)
                return _ac  # type: ignore
            else:
                def _sc(*args, **kwargs):
                    with self.stage(component):
                        return fn(*args, **kwargs)
                return _sc  # type: ignore
        return _wrap

    # ---- summaries ---------------------------------------------------------------
    def get_summary(self, include_raw: bool = False) -> Dict[str, Any]:
        if not ENABLED:
            return {"session_id": self.session_id, "enabled": False, "total_measurements": 0}

        items = self._measurements.snapshot()
        if not items:
            return {
                "session_id": self.session_id,
                "enabled": True,
                "total_measurements": 0,
                "overhead_ns": self._overhead_ns,
            }

        by_component: Dict[str, List[float]] = {}
        by_agent: Dict[str, List[float]] = {}
        turns = set()

        for m in items:
            by_component.setdefault(m.component, []).append(m.duration_ms)
            if m.agent:
                by_agent.setdefault(m.agent, []).append(m.duration_ms)
            if m.turn_id:
                turns.add(m.turn_id)

        comp_stats = {k: ComponentStats.from_values(v).to_dict() for k, v in by_component.items()}
        agent_stats = {k: ComponentStats.from_values(v).to_dict() for k, v in by_agent.items()}

        out = {
            "session_id": self.session_id,
            "enabled": True,
            "total_measurements": len(items),
            "total_turns": len(turns),
            "components": comp_stats,
            "agents": agent_stats,
            "overhead_ns": self._overhead_ns,
        }

        if include_raw:
            out["measurements"] = [
                {"component": m.component, "duration_ms": m.duration_ms, "turn_id": m.turn_id, "agent": m.agent}
                for m in items
            ]
        return out

    def serialize_for_redis(self) -> Dict[str, Any]:
        """Small payload: aggregates only; avoid raw for size reasons."""
        s = self.get_summary(include_raw=False)
        # include correlation state for restore:
        s["current_turn_id"] = self.current_turn_id
        s["current_agent"] = self.current_agent
        return s

    def restore_from_redis(self, data: Dict[str, Any]) -> None:
        """Restore correlation context; (we do not reload raw samples)."""
        self.current_turn_id = data.get("current_turn_id", "")
        self.current_agent = data.get("current_agent", "")

    # ---- internals ---------------------------------------------------------------
    @staticmethod
    def _calibrate_overhead_ns(iterations: int = 50) -> int:
        """
        Estimate instrumentation overhead (ns) so we subtract it from timings.
        Done once per tracker; cheap.
        """
        try:
            start = time.perf_counter_ns()
            for _ in range(iterations):
                _ = time.perf_counter_ns()
            end = time.perf_counter_ns()
            avg = (end - start) / iterations
            return int(avg)
        except Exception:
            return 0


def _is_coroutine(fn: Callable[..., Any]) -> bool:
    return hasattr(fn, "__call__") and (getattr(fn, "__code__", None) is None or bool(getattr(fn, "__aiter__", None))) or \
           (hasattr(fn, "__annotations__") and "return" in fn.__annotations__ and "Awaitable" in str(fn.__annotations__["return"]))
