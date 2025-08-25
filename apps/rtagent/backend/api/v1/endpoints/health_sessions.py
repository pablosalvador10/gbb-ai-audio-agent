"""
Enhanced health checks for session and WebSocket management.

Provides comprehensive health metrics for monitoring and alerting
in production environments.
"""

from fastapi import APIRouter, Depends, Request
from typing import Dict, Any
import time

router = APIRouter()


async def get_session_manager(request: Request):
    """Dependency to get the session manager."""
    return getattr(request.app.state, 'session_manager', None)


@router.get("/health/sessions")
async def session_health(request: Request):
    """
    Get detailed session health metrics.
    
    Returns comprehensive metrics for monitoring session management
    performance and identifying potential issues.
    """
    # Get websocket manager from app state (unified session manager)
    websocket_mgr = getattr(request.app.state, "websocket_manager", None)
    
    if not websocket_mgr:
        # Fallback to session manager if websocket manager not available
        session_manager = getattr(request.app.state, 'session_manager', None)
        if not session_manager:
            return {
                "status": "error",
                "message": "Session manager not initialized",
                "timestamp": time.time(),
                "metrics": {}
            }
        
        try:
            # Get basic health from session manager
            metrics = await session_manager.get_health_metrics()
            return {
                "status": "healthy",
                "message": "Session manager active (legacy mode)",
                "timestamp": time.time(),
                "metrics": metrics
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Legacy session manager failed: {str(e)}",
                "timestamp": time.time(),
                "metrics": {}
            }
    
    try:
        # Get health metrics from the unified session manager
        metrics = await websocket_mgr.get_health_metrics()
        
        # Calculate rejection rate
        total_attempted = metrics.get("total_connections_added", 0)
        total_rejected = metrics.get("total_connections_rejected", 0)
        rejection_rate = (total_rejected / total_attempted) if total_attempted > 0 else 0.0
        
        # Determine health status
        active_connections = metrics.get("active_connections", 0)
        max_connections = metrics.get("max_connections", 50000)
        connection_utilization = (active_connections / max_connections) if max_connections > 0 else 0.0
        circuit_breaker_active = metrics.get('circuit_breaker_active', False)
        
        if circuit_breaker_active or connection_utilization > 0.9:
            status = "degraded"
            message = "Circuit breaker active or connection limit nearly reached"
        elif rejection_rate > 0.1 or connection_utilization > 0.8:  # 10% rejection rate threshold
            status = "warning"
            message = f"High rejection rate: {rejection_rate:.2%} or high connection utilization: {connection_utilization:.1%}"
        else:
            status = "healthy"
            message = "All systems operational"
        
        return {
            "status": status,
            "message": message,
            "timestamp": time.time(),
            "metrics": metrics
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Health check failed: {str(e)}",
            "timestamp": time.time(),
            "metrics": {}
        }


@router.get("/health/sessions/detailed")
async def detailed_session_health(session_manager=Depends(get_session_manager)):
    """
    Get detailed session information for debugging.
    
    Warning: This endpoint can be expensive for high session counts.
    Use with caution in production.
    """
    if not session_manager:
        return {"error": "Session manager not initialized"}
    
    try:
        sessions = await session_manager.list_active_sessions()
        return {
            "total_sessions": len(sessions),
            "sessions": [
                {
                    "session_id": s.session_id,
                    "session_type": s.session_type,
                    "age_seconds": s.age_seconds,
                    "idle_seconds": s.idle_seconds,
                    "connection_count": s.connection_count
                }
                for s in sessions[:100]  # Limit to 100 for performance
            ],
            "truncated": len(sessions) > 100
        }
            
    except Exception as e:
        return {"error": f"Failed to get session details: {str(e)}"}
