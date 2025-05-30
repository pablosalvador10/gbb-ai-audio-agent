"""
Call Event Handler for ACS Call Lifecycle Events
=================================================
Handles call connected/disconnected events and manages call state.
"""

from __future__ import annotations

from typing import Any, Dict
from datetime import datetime, timezone

from azure.core.messaging import CloudEvent
from fastapi import Request

from .base_handler import BaseEventHandler
from rtagents.RTAgent.backend.orchestration.conversation_state import ConversationManager


class CallEventHandler(BaseEventHandler):
    """
    Handler for ACS call lifecycle events
    
    Supported events:
    - Microsoft.Communication.CallConnected
    - Microsoft.Communication.CallDisconnected
    - Microsoft.Communication.CallTransferAccepted
    - Microsoft.Communication.CallTransferFailed
    """
    
    def __init__(self):
        super().__init__("call_lifecycle")
        self._active_calls = {}  # Track active call sessions
    
    async def _validate_specific_event(self, event: CloudEvent) -> None:
        """
        Validate call-specific event data
        
        Args:
            event: CloudEvent to validate
            
        Raises:
            ValueError: If call event data is invalid
        """
        valid_call_events = {
            "Microsoft.Communication.CallConnected",
            "Microsoft.Communication.CallDisconnected", 
            "Microsoft.Communication.CallTransferAccepted",
            "Microsoft.Communication.CallTransferFailed",
        }
        
        if event.type not in valid_call_events:
            raise ValueError(f"Unsupported call event type: {event.type}")
    
    async def _process_event(
        self, 
        event: CloudEvent, 
        request: Request,
        correlation_id: str
    ) -> Dict[str, Any]:
        """
        Process call lifecycle events
        
        Args:
            event: CloudEvent to process
            request: FastAPI request object
            correlation_id: Correlation ID for tracking
            
        Returns:
            Dict containing processing results
        """
        event_type = event.type
        call_connection_id = event.data.get("callConnectionId")
        
        if event_type == "Microsoft.Communication.CallConnected":
            return await self._handle_call_connected(event, request, correlation_id)
        elif event_type == "Microsoft.Communication.CallDisconnected":
            return await self._handle_call_disconnected(event, request, correlation_id)
        elif event_type == "Microsoft.Communication.CallTransferAccepted":
            return await self._handle_call_transfer_accepted(event, request, correlation_id)
        elif event_type == "Microsoft.Communication.CallTransferFailed":
            return await self._handle_call_transfer_failed(event, request, correlation_id)
        else:
            raise ValueError(f"Unsupported event type: {event_type}")
    
    async def _handle_call_connected(
        self, 
        event: CloudEvent, 
        request: Request,
        correlation_id: str
    ) -> Dict[str, Any]:
        """
        Handle call connected event
        
        Args:
            event: CallConnected CloudEvent
            request: FastAPI request object
            correlation_id: Correlation ID for tracking
            
        Returns:
            Dict containing processing results
        """
        call_connection_id = event.data.get("callConnectionId")
        server_call_id = event.data.get("serverCallId")
        
        self.logger.info(
            "📞 Call connected - initializing session",
            extra={
                "call_connection_id": call_connection_id,
                "server_call_id": server_call_id,
                "correlation_id": correlation_id,
            }
        )
        
        # Initialize call session tracking
        call_session = {
            "call_connection_id": call_connection_id,
            "server_call_id": server_call_id,
            "correlation_id": correlation_id,
            "connected_at": datetime.now(timezone.utc),
            "status": "connected",
            "recording_started": False,
            "media_streaming_active": False,
        }
        
        self._active_calls[call_connection_id] = call_session
        
        # Initialize conversation state for this call
        try:
            conversation_manager = request.app.state.conversation_manager
            await conversation_manager.initialize_conversation(
                call_connection_id, 
                correlation_id
            )
            
            # Check if we should auto-greet (avoid double greetings)
            if hasattr(request.app.state, 'greeted_call_ids'):
                if call_connection_id not in request.app.state.greeted_call_ids:
                    # Trigger greeting logic here if needed
                    request.app.state.greeted_call_ids.add(call_connection_id)
                    self.logger.info(
                        "👋 Call marked for greeting",
                        extra={
                            "call_connection_id": call_connection_id,
                            "correlation_id": correlation_id,
                        }
                    )
            
        except Exception as e:
            self.logger.error(
                "❌ Failed to initialize conversation state",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                    "error": str(e),
                },
                exc_info=True
            )
            # Don't fail the entire call for this
        
        # Broadcast to connected clients (dashboard, etc.)
        try:
            if hasattr(request.app.state, 'clients'):
                from rtagents.RTAgent.backend.shared_ws import broadcast_message
                await broadcast_message(
                    request.app.state.clients, 
                    f"📞 Call Connected: {call_connection_id}"
                )
        except Exception as e:
            self.logger.warning(
                "⚠️ Failed to broadcast call connected message",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                    "error": str(e),
                }
            )
        
        return {
            "action": "call_connected",
            "call_connection_id": call_connection_id,
            "server_call_id": server_call_id,
            "session_initialized": True,
        }
    
    async def _handle_call_disconnected(
        self, 
        event: CloudEvent, 
        request: Request,
        correlation_id: str
    ) -> Dict[str, Any]:
        """
        Handle call disconnected event
        
        Args:
            event: CallDisconnected CloudEvent
            request: FastAPI request object
            correlation_id: Correlation ID for tracking
            
        Returns:
            Dict containing processing results
        """
        call_connection_id = event.data.get("callConnectionId")
        
        self.logger.info(
            "❌ Call disconnected - cleaning up session",
            extra={
                "call_connection_id": call_connection_id,
                "correlation_id": correlation_id,
            }
        )
        
        # Get call session info before cleanup
        call_session = self._active_calls.get(call_connection_id, {})
        
        # Cleanup call session
        if call_connection_id in self._active_calls:
            call_session = self._active_calls[call_connection_id]
            call_session["status"] = "disconnected"
            call_session["disconnected_at"] = datetime.now(timezone.utc)
            
            # Calculate call duration
            if "connected_at" in call_session:
                duration = (call_session["disconnected_at"] - call_session["connected_at"]).total_seconds()
                call_session["duration_seconds"] = duration
                
                self.logger.info(
                    "⏱️ Call duration calculated",
                    extra={
                        "call_connection_id": call_connection_id,
                        "duration_seconds": duration,
                        "correlation_id": correlation_id,
                    }
                )
        
        # Cleanup conversation state
        try:
            conversation_manager = request.app.state.conversation_manager
            await conversation_manager.cleanup_conversation(call_connection_id)
        except Exception as e:
            self.logger.error(
                "❌ Failed to cleanup conversation state",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                    "error": str(e),
                },
                exc_info=True
            )
        
        # Remove from greeted calls if present
        if hasattr(request.app.state, 'greeted_call_ids'):
            request.app.state.greeted_call_ids.discard(call_connection_id)
        
        # Trigger post-call processing
        try:
            from rtagents.RTAgent.backend.postcall.push import build_and_flush
            await build_and_flush(call_connection_id, request.app.state)
        except Exception as e:
            self.logger.error(
                "❌ Failed to trigger post-call processing",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                    "error": str(e),
                },
                exc_info=True
            )
        
        # Broadcast to connected clients
        try:
            if hasattr(request.app.state, 'clients'):
                from rtagents.RTAgent.backend.shared_ws import broadcast_message
                await broadcast_message(
                    request.app.state.clients, 
                    f"❌ Call Disconnected: {call_connection_id}"
                )
        except Exception as e:
            self.logger.warning(
                "⚠️ Failed to broadcast call disconnected message",
                extra={
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                    "error": str(e),
                }
            )
        
        # Remove from active calls (keep for a while for debugging)
        # TODO: Implement proper cleanup after some time
        
        return {
            "action": "call_disconnected",
            "call_connection_id": call_connection_id,
            "session_cleaned": True,
            "call_duration_seconds": call_session.get("duration_seconds"),
        }
    
    async def _handle_call_transfer_accepted(
        self, 
        event: CloudEvent, 
        request: Request,
        correlation_id: str
    ) -> Dict[str, Any]:
        """
        Handle call transfer accepted event
        
        Args:
            event: CallTransferAccepted CloudEvent
            request: FastAPI request object
            correlation_id: Correlation ID for tracking
            
        Returns:
            Dict containing processing results
        """
        call_connection_id = event.data.get("callConnectionId")
        
        self.logger.info(
            "🔄 Call transfer accepted",
            extra={
                "call_connection_id": call_connection_id,
                "correlation_id": correlation_id,
            }
        )
        
        # Update call session status
        if call_connection_id in self._active_calls:
            self._active_calls[call_connection_id]["status"] = "transferred"
        
        return {
            "action": "call_transfer_accepted",
            "call_connection_id": call_connection_id,
        }
    
    async def _handle_call_transfer_failed(
        self, 
        event: CloudEvent, 
        request: Request,
        correlation_id: str
    ) -> Dict[str, Any]:
        """
        Handle call transfer failed event
        
        Args:
            event: CallTransferFailed CloudEvent
            request: FastAPI request object
            correlation_id: Correlation ID for tracking
            
        Returns:
            Dict containing processing results
        """
        call_connection_id = event.data.get("callConnectionId")
        
        self.logger.warning(
            "⚠️ Call transfer failed",
            extra={
                "call_connection_id": call_connection_id,
                "correlation_id": correlation_id,
            }
        )
        
        return {
            "action": "call_transfer_failed", 
            "call_connection_id": call_connection_id,
        }
    
    async def _cleanup_on_error(
        self, 
        event: CloudEvent, 
        request: Request,
        correlation_id: str
    ) -> None:
        """
        Cleanup resources on error for call events
        
        Args:
            event: CloudEvent that failed
            request: FastAPI request object
            correlation_id: Correlation ID for tracking
        """
        call_connection_id = event.data.get("callConnectionId")
        
        if call_connection_id and call_connection_id in self._active_calls:
            try:
                # Mark call as failed
                self._active_calls[call_connection_id]["status"] = "failed"
                self._active_calls[call_connection_id]["failed_at"] = datetime.now(timezone.utc)
                
                # Cleanup conversation state
                conversation_manager = request.app.state.conversation_manager
                await conversation_manager.cleanup_conversation(call_connection_id)
                
                self.logger.info(
                    "🧹 Cleaned up failed call session",
                    extra={
                        "call_connection_id": call_connection_id,
                        "correlation_id": correlation_id,
                    }
                )
            except Exception as e:
                self.logger.error(
                    "💥 Failed to cleanup call session on error",
                    extra={
                        "call_connection_id": call_connection_id,
                        "correlation_id": correlation_id,
                        "cleanup_error": str(e),
                    }
                )
    
    def get_active_calls(self) -> Dict[str, Dict[str, Any]]:
        """
        Get currently active call sessions
        
        Returns:
            Dict of call connection IDs to session info
        """
        return self._active_calls.copy()
    
    def get_call_session(self, call_connection_id: str) -> Dict[str, Any]:
        """
        Get specific call session info
        
        Args:
            call_connection_id: Call connection ID
            
        Returns:
            Dict containing call session info
        """
        return self._active_calls.get(call_connection_id, {})
