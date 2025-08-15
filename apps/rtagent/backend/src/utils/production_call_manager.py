"""
Production-Ready Call Isolation Manager
======================================

Handles 1000+ concurrent calls with complete isolation and no shared state.
Each call gets its own isolated context with zero cross-contamination.

Key Features:
- Zero shared state between calls
- Redis namespace isolation per call
- Automatic resource cleanup
- Thread-safe operations
- Memory leak prevention
- Graceful degradation under load
"""

import asyncio
import threading
import time
import weakref
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Set, AsyncContextManager
from uuid import uuid4

from fastapi import WebSocket
from src.stateful.state_managment import MemoManager
from src.redis.manager import AzureRedisManager
from utils.ml_logging import get_logger
from .production_redis_pool import get_production_redis_pool

logger = get_logger("call_isolation_manager")


class ProductionMemoManager(MemoManager):
    """Production memory manager with isolated Redis operations."""
    
    def __init__(self, session_id: str, redis_pool, namespace: str):
        super().__init__(session_id=session_id)
        self.redis_pool = redis_pool
        self.namespace = namespace
    
    async def persist_to_redis_async(self, redis_mgr=None, ttl_seconds: Optional[int] = None):
        """Persist to Redis using isolated namespace."""
        try:
            data = self.to_redis_dict()
            await self.redis_pool.isolated_hset(
                self.namespace, 
                f"session:{self.session_id}", 
                data, 
                ttl_seconds
            )
            logger.debug(f"Persisted session {self.session_id} to isolated namespace: {self.namespace}")
        except Exception as e:
            logger.error(f"Error persisting session {self.session_id}: {e}")
    
    async def refresh_from_redis_async(self, redis_mgr=None):
        """Refresh from Redis using isolated namespace."""
        try:
            data = await self.redis_pool.isolated_hgetall(
                self.namespace,
                f"session:{self.session_id}"
            )
            
            if data and self._CORE_KEY in data:
                self.corememory.from_json(data[self._CORE_KEY])
            if data and self._HISTORY_KEY in data:
                self.chatHistory.from_json(data[self._HISTORY_KEY])
            
            logger.debug(f"Refreshed session {self.session_id} from isolated namespace: {self.namespace}")
            return True
        except Exception as e:
            logger.error(f"Error refreshing session {self.session_id}: {e}")
            return False


@dataclass
class IsolatedCallContext:
    """Completely isolated context for a single call."""
    call_connection_id: str
    session_id: str
    namespace: str
    websocket_id: str
    start_time: float
    thread_id: int
    
    # Isolated resources
    memory_manager: Optional[MemoManager] = None
    redis_namespace: Optional[str] = None
    handler: Optional[Any] = None
    
    # Resource tracking
    _cleanup_callbacks: list = field(default_factory=list)
    _is_cleaned_up: bool = False
    
    def add_cleanup_callback(self, callback):
        """Add callback for resource cleanup."""
        if not self._is_cleaned_up:
            self._cleanup_callbacks.append(callback)
    
    async def cleanup(self):
        """Clean up all resources for this call."""
        if self._is_cleaned_up:
            return
            
        self._is_cleaned_up = True
        errors = []
        
        for callback in reversed(self._cleanup_callbacks):
            try:
                if asyncio.iscoroutinefunction(callback):
                    await callback()
                else:
                    callback()
            except Exception as e:
                errors.append(f"Cleanup error: {e}")
                logger.error(f"Cleanup callback failed for call {self.call_connection_id}: {e}")
        
        if errors:
            logger.warning(f"Call {self.call_connection_id} cleanup completed with {len(errors)} errors")
        else:
            logger.info(f"Call {self.call_connection_id} cleanup completed successfully")


class ProductionCallManager:
    """Production-ready call manager with complete isolation."""
    
    def __init__(self, max_concurrent_calls: int = 2000):
        self.max_concurrent_calls = max_concurrent_calls
        self._active_contexts: Dict[str, IsolatedCallContext] = {}
        self._context_lock = threading.RLock()
        self._cleanup_tasks: Set[asyncio.Task] = set()
        
        # Production Redis pool manager
        self._redis_pool_manager = None
        
        # Metrics
        self._total_calls = 0
        self._failed_calls = 0
        self._rejected_calls = 0
        
        # Weak references to prevent memory leaks
        self._websocket_refs: weakref.WeakSet = weakref.WeakSet()
    
    def _ensure_redis_pool(self, redis_mgr: AzureRedisManager):
        """Ensure Redis pool manager is initialized."""
        if self._redis_pool_manager is None:
            self._redis_pool_manager = get_production_redis_pool(redis_mgr)
        return self._redis_pool_manager
        
    async def create_isolated_call_context(
        self, 
        call_connection_id: str, 
        websocket: WebSocket,
        redis_mgr: AzureRedisManager
    ) -> AsyncContextManager[IsolatedCallContext]:
        """Create completely isolated context for a call."""
        
        @asynccontextmanager
        async def _isolated_context():
            context = None
            try:
                # Check capacity
                with self._context_lock:
                    if len(self._active_contexts) >= self.max_concurrent_calls:
                        self._rejected_calls += 1
                        logger.error(
                            f"🚨 CALL REJECTED: Max capacity ({self.max_concurrent_calls}) reached. "
                            f"Active calls: {len(self._active_contexts)}"
                        )
                        raise Exception("System at maximum capacity")
                    
                    # Check for duplicate calls
                    if call_connection_id in self._active_contexts:
                        existing = self._active_contexts[call_connection_id]
                        logger.error(
                            f"🚨 DUPLICATE CALL REJECTED: {call_connection_id} already active "
                            f"since {existing.start_time} (thread: {existing.thread_id})"
                        )
                        raise Exception(f"Call {call_connection_id} already active")
                
                # Create isolated context
                context = IsolatedCallContext(
                    call_connection_id=call_connection_id,
                    session_id=f"{call_connection_id}_{uuid4().hex[:8]}",
                    namespace=f"call:{call_connection_id}:{int(time.time() * 1000)}",
                    websocket_id=f"ws:{id(websocket)}",
                    start_time=time.time(),
                    thread_id=threading.get_ident()
                )
                
                # Create isolated Redis namespace
                context.redis_namespace = f"isolated:call:{call_connection_id}:{context.session_id}"
                
                # Get production Redis pool
                redis_pool = self._ensure_redis_pool(redis_mgr)
                
                # Create isolated memory manager with production Redis pool
                context.memory_manager = ProductionMemoManager(
                    session_id=context.session_id,
                    redis_pool=redis_pool,
                    namespace=context.redis_namespace
                )
                
                # Add cleanup for Redis namespace using production pool
                context.add_cleanup_callback(
                    lambda: self._cleanup_redis_namespace_production(context.redis_namespace)
                )
                
                # Track weak reference to websocket
                self._websocket_refs.add(websocket)
                
                # Register context
                with self._context_lock:
                    self._active_contexts[call_connection_id] = context
                    self._total_calls += 1
                
                logger.info(
                    f"📞 CALL ISOLATED: {call_connection_id} "
                    f"(namespace: {context.namespace}, session: {context.session_id})"
                )
                
                yield context
                
            except Exception as e:
                self._failed_calls += 1
                logger.error(f"Failed to create isolated context for {call_connection_id}: {e}")
                raise
            finally:
                # Always cleanup, even on exceptions
                if context:
                    await self._cleanup_call_context(context)
        
        return _isolated_context()
    
    async def _cleanup_call_context(self, context: IsolatedCallContext):
        """Clean up isolated call context."""
        try:
            # Remove from active contexts first
            with self._context_lock:
                if context.call_connection_id in self._active_contexts:
                    del self._active_contexts[context.call_connection_id]
            
            # Schedule cleanup task to avoid blocking
            cleanup_task = asyncio.create_task(context.cleanup())
            self._cleanup_tasks.add(cleanup_task)
            cleanup_task.add_done_callback(self._cleanup_tasks.discard)
            
            duration = time.time() - context.start_time
            logger.info(
                f"📞 CALL COMPLETED: {context.call_connection_id} "
                f"(duration: {duration:.2f}s, namespace: {context.namespace})"
            )
            
        except Exception as e:
            logger.error(f"Error cleaning up context for {context.call_connection_id}: {e}")
    
    async def _cleanup_redis_namespace_production(self, namespace: str):
        """Clean up Redis namespace using production pool."""
        try:
            if self._redis_pool_manager:
                await self._redis_pool_manager.cleanup_namespace(namespace)
                logger.debug(f"Cleaned up production Redis namespace: {namespace}")
        except Exception as e:
            logger.error(f"Error cleaning up production Redis namespace {namespace}: {e}")
    
    async def _cleanup_redis_namespace(self, redis_mgr: AzureRedisManager, namespace: str):
        """Clean up Redis keys for isolated namespace."""
        try:
            # Use simple pattern matching and deletion
            # This is a simplified version - in production you'd want Redis SCAN
            loop = asyncio.get_event_loop()
            
            # Get keys matching pattern (using thread executor for sync Redis operations)
            pattern = f"{namespace}:*"
            delete_count = await loop.run_in_executor(
                None, 
                lambda: self._sync_delete_pattern(redis_mgr, pattern)
            )
            
            logger.debug(f"Cleaned up {delete_count} Redis keys for namespace: {namespace}")
            
        except Exception as e:
            logger.error(f"Error cleaning up Redis namespace {namespace}: {e}")
    
    def _sync_delete_pattern(self, redis_mgr: AzureRedisManager, pattern: str) -> int:
        """Synchronously delete keys matching pattern."""
        try:
            # Use Redis client scan_iter to avoid loading all keys into memory
            keys_to_delete = []
            for key in redis_mgr.redis_client.scan_iter(match=pattern, count=100):
                keys_to_delete.append(key)
                # Delete in batches to avoid large operations
                if len(keys_to_delete) >= 1000:
                    redis_mgr.redis_client.delete(*keys_to_delete)
                    deleted_count = len(keys_to_delete)
                    keys_to_delete = []
                    
            # Delete remaining keys
            if keys_to_delete:
                redis_mgr.redis_client.delete(*keys_to_delete)
                return len(keys_to_delete)
            return 0
        except Exception as e:
            logger.error(f"Error in sync delete pattern: {e}")
            return 0
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get call manager metrics."""
        with self._context_lock:
            active_count = len(self._active_contexts)
            
        return {
            "active_calls": active_count,
            "total_calls": self._total_calls,
            "failed_calls": self._failed_calls,
            "rejected_calls": self._rejected_calls,
            "success_rate": (self._total_calls - self._failed_calls) / max(self._total_calls, 1),
            "capacity_utilization": active_count / self.max_concurrent_calls,
            "cleanup_tasks_pending": len(self._cleanup_tasks),
            "websockets_tracked": len(self._websocket_refs)
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check."""
        metrics = self.get_metrics()
        
        # Check for stuck contexts (older than 10 minutes)
        stuck_contexts = []
        current_time = time.time()
        
        with self._context_lock:
            for call_id, context in self._active_contexts.items():
                if current_time - context.start_time > 600:  # 10 minutes
                    stuck_contexts.append({
                        "call_id": call_id,
                        "age_seconds": current_time - context.start_time,
                        "namespace": context.namespace
                    })
        
        health_status = "healthy"
        if metrics["capacity_utilization"] > 0.9:
            health_status = "warning"
        if metrics["capacity_utilization"] > 0.95 or stuck_contexts:
            health_status = "critical"
        
        return {
            "status": health_status,
            "metrics": metrics,
            "stuck_contexts": stuck_contexts,
            "recommendations": self._get_health_recommendations(metrics, stuck_contexts)
        }
    
    def _get_health_recommendations(self, metrics: Dict, stuck_contexts: list) -> list:
        """Get health recommendations."""
        recommendations = []
        
        if metrics["capacity_utilization"] > 0.8:
            recommendations.append("Consider increasing max_concurrent_calls or scaling horizontally")
        
        if metrics["success_rate"] < 0.95:
            recommendations.append("High failure rate detected - investigate error patterns")
        
        if stuck_contexts:
            recommendations.append(f"Found {len(stuck_contexts)} stuck contexts - investigate call lifecycle")
        
        if metrics["cleanup_tasks_pending"] > 100:
            recommendations.append("High cleanup backlog - may indicate resource contention")
        
        return recommendations


# Global production-ready manager
production_call_manager = ProductionCallManager(max_concurrent_calls=2000)
