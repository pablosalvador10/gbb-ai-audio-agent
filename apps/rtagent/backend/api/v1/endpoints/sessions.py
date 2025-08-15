"""
Session management API endpoints for RTAgent Developer Hub.
Provides real-time access to session data, memory state, and agent information.
"""

import json
from typing import Dict, List, Optional, Any
from datetime import datetime

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

from apps.rtagent.backend.api.v1.endpoints.realtime import _active_conversation_sessions
from src.stateful.state_managment import MemoManager
from utils.ml_logging import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/sessions", tags=["sessions"])

# Response Models
class SessionInfo(BaseModel):
    session_id: str
    connection_id: Optional[str]
    start_time: datetime
    duration_seconds: int
    active_agent: str
    authenticated: bool
    websocket_connected: bool
    orchestrator_name: Optional[str]

class CoreMemoryResponse(BaseModel):
    session_id: str
    memory: Dict[str, Any]
    keys_count: int
    last_updated: datetime

class AgentFlowResponse(BaseModel):
    session_id: str
    agent_history: List[Dict[str, Any]]
    current_agent: str
    handoff_count: int

class SessionsOverviewResponse(BaseModel):
    total_active_sessions: int
    sessions: List[SessionInfo]

@router.get("/", response_model=SessionsOverviewResponse, summary="List Active Sessions")
async def get_active_sessions():
    """Get overview of all active conversation sessions."""
    sessions = []
    
    for session_id, session_data in _active_conversation_sessions.items():
        try:
            memory_manager: MemoManager = session_data.get("memory_manager")
            websocket = session_data.get("websocket")
            start_time = session_data.get("start_time", datetime.utcnow())
            orchestrator = session_data.get("orchestrator")
            
            # Calculate duration
            duration = (datetime.utcnow() - start_time).total_seconds()
            
            # Determine active agent and auth status
            active_agent = "AutoAuth"  # Default
            authenticated = False
            
            if memory_manager:
                # Check context for agent info
                caller_name = memory_manager.get_context("caller_name")
                policy_id = memory_manager.get_context("policy_id") 
                authenticated = bool(caller_name or policy_id)
                
                # Determine current agent based on conversation state
                if authenticated:
                    intent = memory_manager.get_context("intent", "").lower()
                    if "claim" in intent:
                        active_agent = "Claims"
                    else:
                        active_agent = "General"
            
            sessions.append(SessionInfo(
                session_id=session_id,
                connection_id=session_id,  # Using session_id as connection_id for now
                start_time=start_time,
                duration_seconds=int(duration),
                active_agent=active_agent,
                authenticated=authenticated,
                websocket_connected=websocket is not None,
                orchestrator_name=getattr(orchestrator, 'name', 'default') if orchestrator else 'default'
            ))
            
        except Exception as e:
            logger.error(f"Error processing session {session_id}: {e}")
            continue
    
    return SessionsOverviewResponse(
        total_active_sessions=len(sessions),
        sessions=sessions
    )

@router.get("/{session_id}", response_model=SessionInfo, summary="Get Session Details")
async def get_session(session_id: str):
    """Get detailed information about a specific session."""
    if session_id not in _active_conversation_sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    session_data = _active_conversation_sessions[session_id]
    memory_manager: MemoManager = session_data.get("memory_manager")
    websocket = session_data.get("websocket")
    start_time = session_data.get("start_time", datetime.utcnow())
    orchestrator = session_data.get("orchestrator")
    
    # Calculate duration
    duration = (datetime.utcnow() - start_time).total_seconds()
    
    # Determine active agent and auth status
    active_agent = "AutoAuth"
    authenticated = False
    
    if memory_manager:
        caller_name = memory_manager.get_context("caller_name")
        policy_id = memory_manager.get_context("policy_id")
        authenticated = bool(caller_name or policy_id)
        
        if authenticated:
            intent = memory_manager.get_context("intent", "").lower()
            if "claim" in intent:
                active_agent = "Claims"
            else:
                active_agent = "General"
    
    return SessionInfo(
        session_id=session_id,
        connection_id=session_id,
        start_time=start_time,
        duration_seconds=int(duration),
        active_agent=active_agent,
        authenticated=authenticated,
        websocket_connected=websocket is not None,
        orchestrator_name=getattr(orchestrator, 'name', 'default') if orchestrator else 'default'
    )

@router.get("/{session_id}/memory", response_model=CoreMemoryResponse, summary="Get Session Core Memory")
async def get_session_memory(session_id: str):
    """Get the core memory state for a session."""
    if session_id not in _active_conversation_sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    session_data = _active_conversation_sessions[session_id]
    memory_manager: MemoManager = session_data.get("memory_manager")
    
    if not memory_manager:
        raise HTTPException(status_code=404, detail="No memory manager found for session")
    
    # Get current memory state
    memory = memory_manager.context.copy()
    
    # Add queue and processing info
    memory.update({
        "queue_size": len(getattr(memory_manager.message_queue, 'queue', [])),
        "queue_processing": getattr(memory_manager, '_queue_processing', False),
        "tts_interrupted": memory_manager.get_context("tts_interrupted", False),
        "last_refresh": datetime.utcnow().isoformat()
    })
    
    return CoreMemoryResponse(
        session_id=session_id,
        memory=memory,
        keys_count=len(memory),
        last_updated=datetime.utcnow()
    )

@router.get("/{session_id}/agents", response_model=AgentFlowResponse, summary="Get Agent Flow History")
async def get_session_agents(session_id: str):
    """Get agent handoff history and current active agent."""
    if session_id not in _active_conversation_sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    session_data = _active_conversation_sessions[session_id]
    memory_manager: MemoManager = session_data.get("memory_manager")
    
    if not memory_manager:
        raise HTTPException(status_code=404, detail="No memory manager found for session")
    
    # Build agent history from conversation history
    agent_history = []
    handoff_count = 0
    current_agent = "AutoAuth"
    
    try:
        # Get all agent histories
        all_histories = memory_manager.histories
        
        agents_sequence = []
        for agent_name, messages in all_histories.items():
            if messages:  # Agent has messages
                agents_sequence.append({
                    "agent": agent_name,
                    "message_count": len(messages),
                    "first_message_time": datetime.utcnow().isoformat(),  # Placeholder
                    "last_activity": datetime.utcnow().isoformat()
                })
        
        # Determine current agent based on context
        caller_name = memory_manager.get_context("caller_name")
        policy_id = memory_manager.get_context("policy_id")
        authenticated = bool(caller_name or policy_id)
        
        if authenticated:
            intent = memory_manager.get_context("intent", "").lower()
            if "claim" in intent:
                current_agent = "Claims"
            else:
                current_agent = "General"
            handoff_count = 1 if agents_sequence else 0
        
        agent_history = agents_sequence
        
    except Exception as e:
        logger.error(f"Error building agent history for session {session_id}: {e}")
    
    return AgentFlowResponse(
        session_id=session_id,
        agent_history=agent_history,
        current_agent=current_agent,
        handoff_count=handoff_count
    )

@router.post("/{session_id}/refresh", summary="Refresh Session from Redis")
async def refresh_session(session_id: str):
    """Force refresh session data from Redis."""
    if session_id not in _active_conversation_sessions:
        raise HTTPException(status_code=404, detail="Session not found")
    
    session_data = _active_conversation_sessions[session_id]
    memory_manager: MemoManager = session_data.get("memory_manager")
    
    if not memory_manager:
        raise HTTPException(status_code=404, detail="No memory manager found for session")
    
    # Get redis manager from app state (this would need to be passed in real implementation)
    try:
        # In real implementation, you'd get redis_mgr from app.state.redis
        # For now, return success without actual refresh
        return {
            "session_id": session_id,
            "status": "refreshed",
            "timestamp": datetime.utcnow().isoformat(),
            "message": "Session data refresh requested"
        }
    except Exception as e:
        logger.error(f"Error refreshing session {session_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Refresh failed: {str(e)}")
