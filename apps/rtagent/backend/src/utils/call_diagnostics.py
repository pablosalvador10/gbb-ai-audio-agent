"""
Concurrent Call Diagnostics
==========================

Utility functions to diagnose and debug concurrent call issues in ACS.
"""

import threading
from typing import Dict, Any, Optional
from utils.ml_logging import get_logger

logger = get_logger("concurrent_call_diagnostics")

class CallDiagnostics:
    """Diagnose concurrent call issues."""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._active_calls = {}
                    cls._instance._call_lock = threading.RLock()
        return cls._instance
    
    def register_call_start(self, call_connection_id: str, session_id: str, metadata: Dict[str, Any] = None):
        """Register when a call starts processing."""
        with self._call_lock:
            if call_connection_id in self._active_calls:
                existing = self._active_calls[call_connection_id]
                logger.warning(
                    f"🚨 CONCURRENT CALL DETECTED: {call_connection_id} "
                    f"already active since {existing.get('start_time')}. "
                    f"Previous session: {existing.get('session_id')}, "
                    f"New session: {session_id}"
                )
            
            import time
            self._active_calls[call_connection_id] = {
                'session_id': session_id,
                'start_time': time.time(),
                'metadata': metadata or {},
                'thread_id': threading.get_ident()
            }
            
            logger.info(
                f"📞 Call registered: {call_connection_id} (session: {session_id}, "
                f"thread: {threading.get_ident()})"
            )
    
    def register_call_end(self, call_connection_id: str):
        """Register when a call ends processing."""
        with self._call_lock:
            if call_connection_id in self._active_calls:
                call_info = self._active_calls.pop(call_connection_id)
                import time
                duration = time.time() - call_info['start_time']
                logger.info(
                    f"📞 Call completed: {call_connection_id} "
                    f"(duration: {duration:.2f}s, session: {call_info['session_id']})"
                )
            else:
                logger.warning(f"⚠️ Call end without start: {call_connection_id}")
    
    def get_active_calls(self) -> Dict[str, Dict[str, Any]]:
        """Get all currently active calls."""
        with self._call_lock:
            return self._active_calls.copy()
    
    def check_for_conflicts(self, call_connection_id: str) -> Optional[Dict[str, Any]]:
        """Check if there's already an active call with this ID."""
        with self._call_lock:
            return self._active_calls.get(call_connection_id)

# Global instance
diagnostics = CallDiagnostics()
