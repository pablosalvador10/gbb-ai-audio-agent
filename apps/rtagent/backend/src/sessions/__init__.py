"""
Enterprise session management module.

This module provides unified session management components optimized for
high-concurrency horizontal scaling with Redis backing.
"""

from .session_manager import (
    SessionManager, 
    SessionState,
    initialize_session_manager,
    get_session_manager
)
from .session_statistics import SessionStatisticsManager

__all__ = [
    "SessionManager",
    "SessionState",
    "initialize_session_manager", 
    "get_session_manager",
    "SessionStatisticsManager",
]
