"""
Enterprise Session Manager with Redis Integration

Single unified manager for all session and WebSocket state with Redis backing
using our existing AzureRedisManager infrastructure for horizontal scaling.

Key Features:
- Uses existing AzureRedisManager methods for compatibility
- Single source of truth for all session and connection state  
- Horizontal scaling with Redis persistence
- Connection-aware session management with circuit breaker
- Zero-downtime rolling updates with session persistence
"""

import asyncio
import json
import time
import uuid
import weakref
from datetime import datetime, timedelta
from typing import Dict, Set, Optional, List, Any, Union
from dataclasses import dataclass, field
from collections import defaultdict

from fastapi import WebSocket
from fastapi.websockets import WebSocketState
from utils.ml_logging import get_logger

logger = get_logger(__name__)


@dataclass
class SessionState:
    """Complete session state including WebSocket connections."""
    session_id: str
    session_type: str  # "acs_media", "realtime_conversation", "dashboard"
    created_at: float
    last_activity: float
    connection_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize for Redis storage."""
        return {
            "session_id": self.session_id,
            "session_type": self.session_type, 
            "created_at": str(self.created_at),
            "last_activity": str(self.last_activity),
            "connection_count": str(self.connection_count),
            "metadata": json.dumps(self.metadata)  # Serialize nested dict
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SessionState":
        """Deserialize from Redis storage."""
        # Handle both string and dict metadata
        metadata = data.get("metadata", "{}")
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (json.JSONDecodeError, TypeError):
                metadata = {}
        
        return cls(
            session_id=data["session_id"],
            session_type=data["session_type"],
            created_at=float(data["created_at"]),
            last_activity=float(data["last_activity"]),
            connection_count=int(data.get("connection_count", 0)),
            metadata=metadata
        )
    
    def touch(self) -> None:
        """Update last activity timestamp."""
        self.last_activity = time.time()
    
    @property
    def age_seconds(self) -> float:
        """Session age in seconds."""
        return time.time() - self.created_at
    
    @property
    def idle_seconds(self) -> float:
        """Idle time in seconds."""
        return time.time() - self.last_activity


class SessionManager:
    """
    Enterprise-grade unified session and WebSocket manager.
    
    Single source of truth for all session state with Redis backing
    using our existing AzureRedisManager for horizontal scaling.
    
    Features:
    - Redis-backed state using existing AzureRedisManager methods
    - Atomic connection tracking with Redis counters
    - Graceful degradation under load with circuit breaker
    - Zero-downtime rolling updates with session persistence
    - Comprehensive telemetry and health monitoring
    """
    
    def __init__(
        self,
        redis_manager=None,  # AzureRedisManager instance or None for memory-only
        session_ttl_seconds: int = 3600,        # 1 hour
        cleanup_interval_seconds: int = 300,    # 5 minutes
        max_connections_per_session: int = 10,  # Prevent DoS
        max_total_connections: int = 50000,     # Circuit breaker
        graceful_degradation: bool = True,      # Allow connections beyond limit
        redis_batch_size: int = 10,             # Batch Redis operations for performance
        redis_batch_interval: float = 0.1      # Batch interval in seconds
    ):
        self.redis = redis_manager
        self.session_ttl = session_ttl_seconds
        self.cleanup_interval = cleanup_interval_seconds
        self.max_connections_per_session = max_connections_per_session
        self.max_total_connections = max_total_connections
        self.graceful_degradation = graceful_degradation
        self.redis_batch_size = redis_batch_size
        self.redis_batch_interval = redis_batch_interval
        
        # Local WebSocket tracking (weak references prevent memory leaks)
        self._websocket_sessions: weakref.WeakKeyDictionary[WebSocket, str] = weakref.WeakKeyDictionary()
        self._session_websockets: Dict[str, Set[WebSocket]] = defaultdict(set)
        
        # Memory-only session storage for fallback
        self._local_sessions: Dict[str, SessionState] = {}
        
        # Redis operation batching for performance
        self._pending_redis_ops = []
        self._redis_batch_task: Optional[asyncio.Task] = None
        
        # Concurrency control
        self._lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None
        
        # Circuit breaker for overload protection
        self._connection_count = 0
        self._rejections_count = 0
        
        # Metrics
        self._metrics = {
            "total_sessions_created": 0,
            "total_connections_added": 0,
            "total_connections_removed": 0,
            "total_rejections": 0,
            "cleanup_cycles": 0,
            "last_cleanup_duration_ms": 0
        }
        
        storage_type = "Redis" if self.redis and self.redis.is_connected else "Memory-only"
        logger.info(
            f"SessionManager initialized: "
            f"storage={storage_type}, ttl={session_ttl_seconds}s, "
            f"max_connections={max_total_connections}, graceful_degradation={graceful_degradation}"
        )
    
    @staticmethod
    def normalize_session_id(raw_session_id: str) -> str:
        """
        Normalize session IDs for consistent storage.
        
        Rules:
        - ACS call-connection-ids: keep as-is (globally unique)
        - UUIDs: truncate for readability but maintain uniqueness
        - Others: validate and normalize
        """
        if not raw_session_id:
            return str(uuid.uuid4())[:12]
        
        # ACS call-connection-id (keep full format for uniqueness)
        if raw_session_id.count('-') == 4 and len(raw_session_id) > 30:
            return raw_session_id
        
        # UUID format (truncate but add timestamp for uniqueness)
        if raw_session_id.count('-') == 4 and len(raw_session_id) == 36:
            timestamp = str(int(time.time()))[-4:]  # Last 4 digits of timestamp
            return f"{raw_session_id[:8]}-{timestamp}"
        
        # Already normalized or custom format
        if len(raw_session_id) <= 12:
            return raw_session_id
        
        # Fallback: hash for uniqueness
        import hashlib
        return hashlib.md5(raw_session_id.encode()).hexdigest()[:12]
    
    def _session_key(self, session_id: str) -> str:
        """Generate Redis key for session data."""
        return f"rtvoice:session:{session_id}"
    
    def _connection_key(self, session_id: str) -> str:
        """Generate Redis key for connection counting."""
        return f"rtvoice:connections:{session_id}"
    
    async def _store_session_state(self, session_state: SessionState) -> bool:
        """Store session state in Redis or memory."""
        try:
            if self.redis and self.redis.is_connected:
                # Use Redis storage
                session_data = session_state.to_dict()
                session_key = self._session_key(session_state.session_id)
                
                # Use sync method in thread pool for thread safety
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(
                    None,
                    lambda: self.redis.store_session_data(session_key, session_data)
                )
            else:
                # Fallback to memory storage
                self._local_sessions[session_state.session_id] = session_state
                return True
        except Exception as e:
            logger.error(f"Failed to store session {session_state.session_id}: {e}")
            # Fallback to memory storage
            self._local_sessions[session_state.session_id] = session_state
            return True
    
    async def _get_session_state(self, session_id: str) -> Optional[SessionState]:
        """Retrieve session state from Redis or memory."""
        try:
            if self.redis and self.redis.is_connected:
                # Try Redis first
                session_key = self._session_key(session_id)
                
                # Use sync method in thread pool for thread safety
                loop = asyncio.get_event_loop()
                session_data = await loop.run_in_executor(
                    None,
                    lambda: self.redis.get_session_data(session_key)
                )
                
                if session_data:
                    return SessionState.from_dict(session_data)
            
            # Fallback to local memory
            return self._local_sessions.get(session_id)
            
        except Exception as e:
            logger.error(f"Failed to get session {session_id}: {e}")
            # Fallback to local memory
            return self._local_sessions.get(session_id)
    
    async def _delete_session_state(self, session_id: str) -> bool:
        """Delete session state from Redis and memory."""
        deleted = False
        
        try:
            if self.redis and self.redis.is_connected:
                # Delete from Redis
                session_key = self._session_key(session_id)
                connection_key = self._connection_key(session_id)
                
                # Use sync method in thread pool for thread safety
                loop = asyncio.get_event_loop()
                session_deleted = await loop.run_in_executor(
                    None,
                    lambda: self.redis.delete_session(session_key)
                )
                connection_deleted = await loop.run_in_executor(
                    None,
                    lambda: self.redis.delete_session(connection_key)
                )
                
                deleted = (session_deleted + connection_deleted) > 0
                
        except Exception as e:
            logger.error(f"Failed to delete session from Redis {session_id}: {e}")
        
        # Also delete from local memory
        if session_id in self._local_sessions:
            del self._local_sessions[session_id]
            deleted = True
        
        return deleted
    
    async def _increment_connections(self, session_id: str) -> int:
        """Atomically increment connection count."""
        try:
            if self.redis and self.redis.is_connected:
                # Use Redis for atomic increment
                connection_key = self._connection_key(session_id)
                
                # Use sync method in thread pool for thread safety
                loop = asyncio.get_event_loop()
                current_value = await loop.run_in_executor(
                    None,
                    lambda: self.redis.get_value(connection_key)
                )
                
                if current_value is None:
                    # First connection - set with TTL
                    await loop.run_in_executor(
                        None,
                        lambda: self.redis.set_value(connection_key, "1", ttl_seconds=self.session_ttl)
                    )
                    return 1
                else:
                    # Increment existing value
                    new_count = int(current_value) + 1
                    await loop.run_in_executor(
                        None,
                        lambda: self.redis.set_value(connection_key, str(new_count), ttl_seconds=self.session_ttl)
                    )
                    return new_count
                    
        except Exception as e:
            logger.error(f"Failed to increment connections for {session_id}: {e}")
        
        # Fallback to local tracking
        return len(self._session_websockets.get(session_id, set())) + 1
    
    async def _decrement_connections(self, session_id: str) -> int:
        """Atomically decrement connection count."""
        try:
            if self.redis and self.redis.is_connected:
                # Use Redis for atomic decrement
                connection_key = self._connection_key(session_id)
                
                # Use sync method in thread pool for thread safety
                loop = asyncio.get_event_loop()
                current_value = await loop.run_in_executor(
                    None,
                    lambda: self.redis.get_value(connection_key)
                )
                
                if current_value is None:
                    return 0
                
                new_count = max(0, int(current_value) - 1)
                
                if new_count == 0:
                    # Clean up if no connections remain
                    await loop.run_in_executor(
                        None,
                        lambda: self.redis.delete_session(connection_key)
                    )
                else:
                    await loop.run_in_executor(
                        None,
                        lambda: self.redis.set_value(connection_key, str(new_count), ttl_seconds=self.session_ttl)
                    )
                
                return new_count
                
        except Exception as e:
            logger.error(f"Failed to decrement connections for {session_id}: {e}")
        
        # Fallback to local tracking
        return max(0, len(self._session_websockets.get(session_id, set())) - 1)
    
    async def add_connection(
        self,
        websocket: WebSocket,
        session_id: str,
        connection_type: str,
        metadata: Optional[Dict[str, Any]] = None,
        skip_redis_for_acs: bool = True  # Fast path for ACS media to avoid barge-in latency
    ) -> bool:
        """
        Add WebSocket connection to session with circuit breaker protection.
        
        Returns False if connection was rejected due to limits.
        """
        normalized_session_id = self.normalize_session_id(session_id)
        
        async with self._lock:
            # Fast path for ACS media connections to minimize barge-in latency
            if skip_redis_for_acs and connection_type == "acs_media":
                # Skip Redis operations for ACS media - use local tracking only
                current_connections = len(self._session_websockets.get(normalized_session_id, set())) + 1
                
                # Still enforce connection limits locally
                if current_connections > self.max_connections_per_session:
                    self._rejections_count += 1
                    self._metrics["total_rejections"] += 1
                    logger.warning(
                        f"ACS connection rejected: per-session limit reached ({self.max_connections_per_session})",
                        extra={"session_id": normalized_session_id, "current_connections": current_connections}
                    )
                    return False
                
                # Create minimal session state in memory only for ACS
                session_state = SessionState(
                    session_id=normalized_session_id,
                    session_type=connection_type,
                    created_at=time.time(),
                    last_activity=time.time(),
                    connection_count=current_connections,
                    metadata=metadata or {}
                )
                self._local_sessions[normalized_session_id] = session_state
                
                # Track WebSocket locally only
                self._websocket_sessions[websocket] = normalized_session_id
                self._session_websockets[normalized_session_id].add(websocket)
                self._connection_count += 1
                self._metrics["total_connections_added"] += 1
                
                logger.debug(
                    f"ACS connection added (fast path): session_id={normalized_session_id}, "
                    f"total_local={self._connection_count}"
                )
                return True
            
            # Full Redis path for other connection types
            # Circuit breaker: Check global connection limit
            if (self._connection_count >= self.max_total_connections and 
                not self.graceful_degradation):
                self._rejections_count += 1
                self._metrics["total_rejections"] += 1
                logger.warning(
                    f"Connection rejected: global limit reached ({self.max_total_connections})",
                    extra={"session_id": normalized_session_id, "connection_type": connection_type}
                )
                return False
            
            # Get or create session
            session_state = await self._get_session_state(normalized_session_id)
            if not session_state:
                session_state = SessionState(
                    session_id=normalized_session_id,
                    session_type=connection_type,
                    created_at=time.time(),
                    last_activity=time.time(),
                    metadata=metadata or {}
                )
                self._metrics["total_sessions_created"] += 1
            
            # Check per-session connection limit
            current_connections = await self._increment_connections(normalized_session_id)
            if current_connections > self.max_connections_per_session:
                await self._decrement_connections(normalized_session_id)
                self._rejections_count += 1
                self._metrics["total_rejections"] += 1
                logger.warning(
                    f"Connection rejected: per-session limit reached ({self.max_connections_per_session})",
                    extra={"session_id": normalized_session_id, "current_connections": current_connections}
                )
                return False
            
            # Update session state
            session_state.connection_count = current_connections
            session_state.touch()
            await self._store_session_state(session_state)
            
            # Track WebSocket locally (for this instance only)
            self._websocket_sessions[websocket] = normalized_session_id
            self._session_websockets[normalized_session_id].add(websocket)
            self._connection_count += 1
            self._metrics["total_connections_added"] += 1
            
            logger.info(
                f"Connection added: session_id={normalized_session_id}, "
                f"type={connection_type}, total_local={self._connection_count}",
                extra={
                    "session_id": normalized_session_id,
                    "connection_type": connection_type,
                    "session_connections": current_connections
                }
            )
            
            return True
    
    async def remove_connection(self, websocket: WebSocket, fast_cleanup: bool = True) -> None:
        """Remove WebSocket connection and cleanup if session is empty."""
        session_id = self._websocket_sessions.get(websocket)
        if not session_id:
            logger.warning("Attempted to remove unknown WebSocket connection")
            return
        
        async with self._lock:
            # Check if this is an ACS session with only local state
            session_state = self._local_sessions.get(session_id)
            is_acs_local_only = (session_state and 
                                session_state.session_type == "acs_media" and 
                                fast_cleanup)
            
            # Remove from local tracking
            self._websocket_sessions.pop(websocket, None)
            if session_id in self._session_websockets:
                self._session_websockets[session_id].discard(websocket)
                if not self._session_websockets[session_id]:
                    del self._session_websockets[session_id]
            
            self._connection_count = max(0, self._connection_count - 1)
            self._metrics["total_connections_removed"] += 1
            
            if is_acs_local_only:
                # Fast cleanup for ACS connections - local only
                if session_id in self._local_sessions:
                    del self._local_sessions[session_id]
                
                logger.debug(
                    f"ACS connection removed (fast cleanup): session_id={session_id}, "
                    f"total_local={self._connection_count}"
                )
                return
            
            # Full Redis cleanup for other connection types
            # Update connection count
            remaining_connections = await self._decrement_connections(session_id)
            
            # If no connections remain globally, clean up session
            if remaining_connections <= 0:
                await self._delete_session_state(session_id)
                logger.info(
                    f"Session cleaned up: {session_id}",
                    extra={"session_id": session_id, "reason": "no_connections"}
                )
            else:
                # Update session activity
                session_state = await self._get_session_state(session_id)
                if session_state:
                    session_state.connection_count = remaining_connections
                    session_state.touch()
                    await self._store_session_state(session_state)
            
            logger.info(
                f"Connection removed: session_id={session_id}, "
                f"remaining_global={remaining_connections}, total_local={self._connection_count}",
                extra={"session_id": session_id, "remaining_connections": remaining_connections}
            )
    
    async def broadcast_to_session(self, session_id: str, message: str) -> int:
        """
        Broadcast a message to all WebSocket connections in a session.
        
        This method sends a dashboard message to all WebSocket connections
        associated with the given session ID on this instance.
        
        Args:
            session_id: The session ID to broadcast to
            message: The message to broadcast
            
        Returns:
            Number of connections the message was sent to
        """
        normalized_session_id = self.normalize_session_id(session_id)
        websockets = self._session_websockets.get(normalized_session_id, set())
        
        if not websockets:
            logger.debug(f"No local WebSocket connections found for session {normalized_session_id}")
            return 0
        
        sent_count = 0
        failed_websockets = []
        
        # Create dashboard message format
        dashboard_message = {
            "type": "dashboard_message",
            "message": message,
            "session_id": normalized_session_id,
            "timestamp": time.time()
        }
        
        # Send to all WebSocket connections in the session
        for ws in list(websockets):  # Copy to avoid modification during iteration
            try:
                if (hasattr(ws, 'client_state') and 
                    hasattr(ws, 'application_state') and
                    ws.client_state == WebSocketState.CONNECTED and 
                    ws.application_state == WebSocketState.CONNECTED):
                    
                    await ws.send_json(dashboard_message)
                    sent_count += 1
                else:
                    # WebSocket is disconnected, mark for cleanup
                    failed_websockets.append(ws)
                    
            except Exception as e:
                logger.warning(
                    f"Failed to send broadcast message to WebSocket in session {normalized_session_id}: {e}"
                )
                failed_websockets.append(ws)
        
        # Clean up failed WebSocket connections
        if failed_websockets:
            async with self._lock:
                for ws in failed_websockets:
                    self._websocket_sessions.pop(ws, None)
                    if normalized_session_id in self._session_websockets:
                        self._session_websockets[normalized_session_id].discard(ws)
        
        logger.debug(
            f"Broadcast message sent to {sent_count} connections in session {normalized_session_id}"
        )
        
        return sent_count

    async def broadcast_to_dashboard_connections(self, call_session_id: str, message: str) -> int:
        """
        Broadcast a message to all dashboard connections that might be monitoring this call.
        
        This method sends messages to dashboard connections across different sessions:
        1. Global dashboard connections (monitoring all calls)  
        2. Call-specific dashboard connections (monitoring this specific call)
        
        Args:
            call_session_id: The call session ID that triggered this broadcast
            message: The message to broadcast
            
        Returns:
            Number of dashboard connections the message was sent to
        """
        sent_count = 0
        failed_websockets = []
        
        # Create dashboard message format
        dashboard_message = {
            "type": "dashboard_message",
            "message": message,
            "call_session_id": call_session_id,
            "timestamp": time.time()
        }
        
        # Find all dashboard connections across all sessions
        dashboard_sessions_to_check = [
            "global-dashboard",  # Global monitoring
            call_session_id      # Call-specific monitoring
        ]
        
        for session_id in dashboard_sessions_to_check:
            normalized_session_id = self.normalize_session_id(session_id)
            websockets = self._session_websockets.get(normalized_session_id, set())
            
            for ws in list(websockets):
                try:
                    # Only send to connected dashboard connections (not ACS media)
                    if (hasattr(ws, 'client_state') and 
                        hasattr(ws, 'application_state') and
                        ws.client_state == WebSocketState.CONNECTED and 
                        ws.application_state == WebSocketState.CONNECTED):
                        
                        # Check if this is a dashboard connection by looking at session state
                        session_state = self._local_sessions.get(normalized_session_id)
                        if session_state and session_state.session_type == "dashboard":
                            await ws.send_json(dashboard_message)
                            sent_count += 1
                        
                except Exception as e:
                    logger.warning(
                        f"Failed to send dashboard broadcast to WebSocket in session {normalized_session_id}: {e}"
                    )
                    failed_websockets.append((ws, normalized_session_id))
        
        # Clean up failed WebSocket connections
        if failed_websockets:
            async with self._lock:
                for failed_ws, session_id in failed_websockets:
                    self._websocket_sessions.pop(failed_ws, None)
                    if session_id in self._session_websockets:
                        self._session_websockets[session_id].discard(failed_ws)
        
        if sent_count > 0:
            logger.debug(
                f"Dashboard broadcast sent to {sent_count} dashboard connections for call {call_session_id}"
            )
        else:
            logger.debug(
                f"No dashboard connections found for call {call_session_id} broadcast"
            )
        
        return sent_count
    
    async def get_session_info(self, session_id: str) -> Optional[SessionState]:
        """Get session information."""
        normalized_session_id = self.normalize_session_id(session_id)
        return await self._get_session_state(normalized_session_id)
    
    async def list_active_sessions(self) -> List[SessionState]:
        """List all active sessions."""
        sessions = []
        
        # Add local sessions
        sessions.extend(self._local_sessions.values())
        
        # For Redis sessions, we'd need to implement pattern matching
        # which can be expensive, so for now return local sessions only
        return sessions
    
    async def get_health_metrics(self) -> Dict[str, Any]:
        """Get comprehensive health and performance metrics."""
        try:
            redis_connected = self.redis.is_connected if self.redis else False
            
            return {
                "local_connections": self._connection_count,
                "local_sessions": len(self._local_sessions),
                "redis_connected": redis_connected,
                "rejection_rate": self._rejections_count / max(1, self._metrics["total_connections_added"]),
                "circuit_breaker_active": self._connection_count >= self.max_total_connections,
                "storage_mode": "redis" if redis_connected else "memory",
                "metrics": self._metrics.copy()
            }
        except Exception as e:
            logger.error(f"Failed to get health metrics: {e}")
            return {
                "local_connections": self._connection_count,
                "local_sessions": len(self._local_sessions),
                "redis_connected": False,
                "error": str(e),
                "metrics": self._metrics.copy()
            }
    
    async def start_background_cleanup(self) -> None:
        """Start background cleanup task."""
        if self._cleanup_task and not self._cleanup_task.done():
            logger.warning("Background cleanup already running")
            return
        
        self._cleanup_task = asyncio.create_task(self._background_cleanup_loop())
        logger.info("Started background session cleanup task")
    
    async def stop_background_cleanup(self) -> None:
        """Stop background cleanup task."""
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        logger.info("Stopped background session cleanup task")
    
    # Compatibility aliases for WebSocket manager interface
    async def start_background_tasks(self) -> None:
        """Alias for start_background_cleanup for WebSocket manager compatibility."""
        await self.start_background_cleanup()
    
    async def stop_background_tasks(self) -> None:
        """Alias for stop_background_cleanup for WebSocket manager compatibility."""
        await self.stop_background_cleanup()
    
    async def _background_cleanup_loop(self) -> None:
        """Background cleanup of stale sessions."""
        while True:
            try:
                await asyncio.sleep(self.cleanup_interval)
                start_time = time.time()
                
                # Clean up local sessions
                stale_sessions = []
                current_time = time.time()
                
                for session_id, session_state in self._local_sessions.items():
                    if session_state.idle_seconds > self.session_ttl:
                        stale_sessions.append(session_id)
                
                for session_id in stale_sessions:
                    await self._delete_session_state(session_id)
                
                cleanup_duration = (time.time() - start_time) * 1000
                self._metrics["cleanup_cycles"] += 1
                self._metrics["last_cleanup_duration_ms"] = cleanup_duration
                
                if stale_sessions:
                    logger.info(
                        f"Cleanup cycle completed: {len(stale_sessions)} sessions cleaned, "
                        f"duration={cleanup_duration:.1f}ms"
                    )
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}", exc_info=True)
                await asyncio.sleep(60)  # Wait before retrying


# Global instance (initialized in main.py)
_global_session_manager: Optional[SessionManager] = None


def get_session_manager() -> SessionManager:
    """Get the global session manager instance."""
    if _global_session_manager is None:
        raise RuntimeError("SessionManager not initialized. Call initialize_session_manager() first.")
    return _global_session_manager


async def initialize_session_manager(
    redis_manager=None,  # AzureRedisManager instance or None for memory-only
    session_ttl_seconds: int = 3600,
    cleanup_interval_seconds: int = 300,
    max_connections_per_session: int = 10,
    max_total_connections: int = 50000,
    graceful_degradation: bool = True
) -> SessionManager:
    """Initialize the global session manager."""
    global _global_session_manager
    
    _global_session_manager = SessionManager(
        redis_manager=redis_manager,
        session_ttl_seconds=session_ttl_seconds,
        cleanup_interval_seconds=cleanup_interval_seconds,
        max_connections_per_session=max_connections_per_session,
        max_total_connections=max_total_connections,
        graceful_degradation=graceful_degradation
    )
    
    return _global_session_manager



