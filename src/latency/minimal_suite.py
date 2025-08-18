"""
Minimal Latency Suite - Production Voice Agent Analytics
========================================================

Ultra-lightweight latency tracking with <0.5ms overhead per measurement.
Features LATENCY_SUITE_V2 feature flag and ns-precision timing.

Key metrics:
- stt_capture_ms, stt_decode_ms, llm_queue_ms, llm_infer_ms
- tts_synthesize_ms, audio_out_ms, turn_e2e_ms  
- tool_{name}_ms for each tool execution
- Per-session stats: count, avg, min, max, p50, p90, p95, p99
"""

import os
import time
import statistics
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any

# Feature flag - disable all tracking when false
ENABLED = os.getenv("LATENCY_SUITE_V2", "true").lower() == "true"


@dataclass
class LatencyMeasurement:
    """Single ns-precision measurement."""
    component: str
    duration_ms: float
    turn_id: str = ""
    agent: str = ""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "component": self.component,
            "duration_ms": self.duration_ms,
            "turn_id": self.turn_id,
            "agent": self.agent
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'LatencyMeasurement':
        """Create from dictionary for JSON deserialization."""
        return cls(
            component=data.get("component", "unknown"),
            duration_ms=data.get("duration_ms", 0.0),
            turn_id=data.get("turn_id", ""),
            agent=data.get("agent", "")
        )


@dataclass  
class ComponentStats:
    """Statistics for a component with percentiles."""
    count: int = 0
    avg_ms: float = 0.0
    min_ms: float = 0.0
    max_ms: float = 0.0
    p50_ms: float = 0.0
    p90_ms: float = 0.0
    p95_ms: float = 0.0
    p99_ms: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "count": self.count,
            "avg_ms": self.avg_ms,
            "min_ms": self.min_ms,
            "max_ms": self.max_ms,
            "p50_ms": self.p50_ms,
            "p90_ms": self.p90_ms,
            "p95_ms": self.p95_ms,
            "p99_ms": self.p99_ms
        }


class MinimalLatencyTracker:
    """Minimal overhead latency tracker with ns precision."""
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.measurements: List[LatencyMeasurement] = []
        self.current_turn_id = ""
        self.current_agent = ""
    
    def start_turn(self, turn_id: str = "", agent: str = ""):
        """Start tracking a new turn."""
        if not ENABLED:
            return ""
        self.current_turn_id = turn_id or f"{self.session_id}_turn_{len(self.measurements)}"
        self.current_agent = agent
        return self.current_turn_id
    
    @asynccontextmanager
    async def track(self, component: str):
        """Track component latency with ns precision."""
        if not ENABLED:
            # When disabled, still provide a proper async context manager
            yield
            return
            
        start_ns = time.perf_counter_ns()
        try:
            yield
        finally:
            end_ns = time.perf_counter_ns()
            duration_ms = (end_ns - start_ns) / 1_000_000
            
            self.measurements.append(LatencyMeasurement(
                component=component,
                duration_ms=duration_ms,
                turn_id=self.current_turn_id,
                agent=self.current_agent
            ))
    
    def track_tool(self, tool_name: str, duration_ms: float):
        """Track tool execution time."""
        if not ENABLED:
            return
        self.measurements.append(LatencyMeasurement(
            component=f"tool_{tool_name}_ms",
            duration_ms=duration_ms,
            turn_id=self.current_turn_id,
            agent=self.current_agent
        ))
    
    def get_summary(self) -> Dict[str, Any]:
        """Get session summary with percentiles."""
        if not ENABLED or not self.measurements:
            return {"session_id": self.session_id, "total_measurements": 0}
        
        # Group by component
        by_component: Dict[str, List[float]] = {}
        by_agent: Dict[str, List[float]] = {}
        turns = set()
        
        for m in self.measurements:
            if m.component not in by_component:
                by_component[m.component] = []
            by_component[m.component].append(m.duration_ms)
            
            if m.agent and m.agent not in by_agent:
                by_agent[m.agent] = []
            if m.agent:
                by_agent[m.agent].append(m.duration_ms)
            
            if m.turn_id:
                turns.add(m.turn_id)
        
        # Calculate stats
        def calc_stats(values: List[float]) -> ComponentStats:
            if not values:
                return ComponentStats()
            values.sort()
            return ComponentStats(
                count=len(values),
                avg_ms=round(statistics.mean(values), 2),
                min_ms=round(min(values), 2),
                max_ms=round(max(values), 2),
                p50_ms=round(statistics.median(values), 2),
                p90_ms=round(_percentile(values, 90), 2),
                p95_ms=round(_percentile(values, 95), 2),
                p99_ms=round(_percentile(values, 99), 2),
            )
        
        return {
            "session_id": self.session_id,
            "total_turns": len(turns),
            "total_measurements": len(self.measurements),
            "components": {k: calc_stats(v).to_dict() for k, v in by_component.items()},
            "agents": {k: calc_stats(v).to_dict() for k, v in by_agent.items()},
            # Include raw data for persistence
            "current_turn_id": self.current_turn_id,
            "current_agent": self.current_agent,
            "measurements": [m.to_dict() for m in self.measurements]
        }
    
    def get_summary_for_display(self) -> Dict[str, Any]:
        """Get summary without raw measurements for display purposes."""
        summary = self.get_summary()
        # Remove raw measurements for cleaner display
        if "measurements" in summary:
            del summary["measurements"]
        return summary
    
    def get_stats_objects(self) -> Dict[str, Any]:
        """Get summary with ComponentStats objects (for internal use)."""
        if not ENABLED or not self.measurements:
            return {"session_id": self.session_id, "total_measurements": 0}
        
        # Group by component
        by_component: Dict[str, List[float]] = {}
        by_agent: Dict[str, List[float]] = {}
        turns = set()
        
        for m in self.measurements:
            if m.component not in by_component:
                by_component[m.component] = []
            by_component[m.component].append(m.duration_ms)
            
            if m.agent and m.agent not in by_agent:
                by_agent[m.agent] = []
            if m.agent:
                by_agent[m.agent].append(m.duration_ms)
            
            if m.turn_id:
                turns.add(m.turn_id)
        
        # Calculate stats
        def calc_stats(values: List[float]) -> ComponentStats:
            if not values:
                return ComponentStats()
            values.sort()
            return ComponentStats(
                count=len(values),
                avg_ms=round(statistics.mean(values), 2),
                min_ms=round(min(values), 2),
                max_ms=round(max(values), 2),
                p50_ms=round(statistics.median(values), 2),
                p90_ms=round(_percentile(values, 90), 2),
                p95_ms=round(_percentile(values, 95), 2),
                p99_ms=round(_percentile(values, 99), 2),
            )
        
        return {
            "session_id": self.session_id,
            "total_turns": len(turns),
            "total_measurements": len(self.measurements),
            "components": {k: calc_stats(v) for k, v in by_component.items()},
            "agents": {k: calc_stats(v) for k, v in by_agent.items()}
        }


def _percentile(data: List[float], p: int) -> float:
    """Fast percentile calculation."""
    if not data:
        return 0.0
    n = len(data)
    index = (p / 100) * (n - 1)
    if index.is_integer():
        return data[int(index)]
    lower = data[int(index)]
    upper = data[int(index) + 1] if int(index) + 1 < n else lower
    return lower + (upper - lower) * (index - int(index))
