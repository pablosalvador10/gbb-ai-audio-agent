"""
ACS Event Handlers Package
==========================
Azure Communication Services event handlers for call lifecycle management.

This package provides structured event handling for ACS call events:
- Centralized event management with ACSEventManager
- Call connected/disconnected events
- Media streaming events  
- Recording management with automatic start/stop
- Session tracking and state management
- Error handling and resilience

Usage:
    from services.acs.events import ACSEventManager, RecordingConfig
    
    # Create event manager with recording config
    recording_config = RecordingConfig(auto_start_on_connect=True)
    event_manager = ACSEventManager(recording_config)
    
    # Process callback events in ACS router
    result = await event_manager.process_callback_events(request, events)
"""

from .acs_event_manager import (
    ACSEventManager,
    ACSEventType,
    RecordingConfig,
)

from .base_handler import BaseEventHandler
from .call_event_handler import CallEventHandler
from .media_event_handler import MediaEventHandler

# Note: RecordingEventHandler temporarily disabled due to import issues
# from .recording_event_handler import RecordingEventHandler

__all__ = [
    "ACSEventManager",
    "ACSEventType", 
    "RecordingConfig",
    "BaseEventHandler",
    "CallEventHandler",
    "MediaEventHandler",
    # "RecordingEventHandler",  # Commented out until Azure SDK imports are resolved
]
