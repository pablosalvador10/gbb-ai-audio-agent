"""
Example: ACS Event Handler with Automatic Recording
==================================================
This example demonstrates how the new ACS Event Handler system automatically
configures and starts recording when a call is connected.

Key Features Demonstrated:
1. Automatic recording start on call connection
2. Session state management
3. Recording configuration
4. Error handling and logging
5. Integration with existing FastAPI router
"""

import asyncio
from datetime import datetime, timezone
from typing import Dict, Any

# Example event data that would come from ACS
SAMPLE_CALL_CONNECTED_EVENT = {
    "id": "event-123",
    "source": "calling/callConnections/call-abc-123",
    "type": "Microsoft.Communication.CallConnected",
    "time": "2025-05-28T10:30:00Z",
    "specVersion": "1.0",
    "datacontenttype": "application/json",
    "data": {
        "callConnectionId": "call-abc-123",
        "serverCallId": "server-456-789",
        "participant": {
            "rawId": "participant-raw-id-xyz",
            "communicationUserId": "user-123"
        },
        "correlationId": "correlation-uuid-456"
    }
}

SAMPLE_CALL_DISCONNECTED_EVENT = {
    "id": "event-124", 
    "source": "calling/callConnections/call-abc-123",
    "type": "Microsoft.Communication.CallDisconnected",
    "time": "2025-05-28T10:35:30Z",
    "specVersion": "1.0",
    "datacontenttype": "application/json",
    "data": {
        "callConnectionId": "call-abc-123",
        "serverCallId": "server-456-789",
        "correlationId": "correlation-uuid-789"
    }
}

async def demonstrate_event_processing():
    """
    Demonstrate the event processing workflow with recording.
    """
    from services.acs.events import ACSEventManager, RecordingConfig
    
    print("🎬 ACS Event Handler Demo - Automatic Recording Configuration")
    print("=" * 70)
    
    # 1. Configure recording settings
    recording_config = RecordingConfig(
        auto_start_on_connect=True,        # 🎙️ Start recording on call connect
        auto_stop_on_disconnect=True,      # 🛑 Stop recording on call disconnect  
        recording_format="wav",            # 🎵 WAV audio format
        recording_channel="unmixed",       # 🎧 Separate audio channels
        storage_container="demo-recordings", # 💾 Storage location
        enable_transcription=False         # 📝 Transcription disabled for demo
    )
    
    print(f"📋 Recording Configuration:")
    print(f"   Auto-start: {recording_config.auto_start_on_connect}")
    print(f"   Auto-stop: {recording_config.auto_stop_on_disconnect}")
    print(f"   Format: {recording_config.recording_format}")
    print(f"   Channel: {recording_config.recording_channel}")
    print()
    
    # 2. Initialize event manager
    event_manager = ACSEventManager(recording_config)
    print("✅ Event Manager initialized with recording configuration")
    print()
    
    # 3. Mock FastAPI request object
    class MockAppState:
        def __init__(self):
            self.acs_caller = "mock-acs-caller"  # Placeholder
            
    class MockRequest:
        def __init__(self):
            self.app = MockAppState()
            
    mock_request = MockRequest()
    
    # 4. Process Call Connected Event
    print("📞 Processing Call Connected Event...")
    print(f"   Call ID: {SAMPLE_CALL_CONNECTED_EVENT['data']['callConnectionId']}")
    print(f"   Server Call ID: {SAMPLE_CALL_CONNECTED_EVENT['data']['serverCallId']}")
    print(f"   Participant: {SAMPLE_CALL_CONNECTED_EVENT['data']['participant']['rawId']}")
    
    connected_result = await event_manager.process_callback_events(
        request=mock_request,
        events=[SAMPLE_CALL_CONNECTED_EVENT]
    )
    
    print("📋 Call Connected Processing Result:")
    for event in connected_result.get("processed_events", []):
        print(f"   ✅ Event: {event['event_type']}")
        print(f"   📞 Call ID: {event['call_connection_id']}")
        if event['result'].get('recording_started'):
            print(f"   🎙️ Recording Started: {event['result']['recording_id']}")
        print()
    
    # 5. Show session information
    call_id = SAMPLE_CALL_CONNECTED_EVENT['data']['callConnectionId']
    session = event_manager.get_session_info(call_id)
    
    if session:
        print("📊 Active Session Information:")
        print(f"   Call Connection ID: {session.call_connection_id}")
        print(f"   Server Call ID: {session.server_call_id}")
        print(f"   Participant Raw ID: {session.participant_raw_id}")
        print(f"   Recording ID: {session.recording_id}")
        print(f"   Recording State: {session.recording_state}")
        print(f"   Connected At: {session.connected_at}")
        print(f"   Media Streaming: {session.media_streaming_active}")
        print()
    
    # 6. List all active sessions
    active_sessions = event_manager.list_active_sessions()
    print(f"📈 Total Active Sessions: {len(active_sessions)}")
    print()
    
    # 7. Simulate some delay (call duration)
    print("⏱️  Simulating call in progress (2 seconds)...")
    await asyncio.sleep(2)
    
    # 8. Process Call Disconnected Event
    print("❌ Processing Call Disconnected Event...")
    
    disconnected_result = await event_manager.process_callback_events(
        request=mock_request,
        events=[SAMPLE_CALL_DISCONNECTED_EVENT]
    )
    
    print("📋 Call Disconnected Processing Result:")
    for event in disconnected_result.get("processed_events", []):
        print(f"   ✅ Event: {event['event_type']}")
        print(f"   📞 Call ID: {event['call_connection_id']}")
        result = event['result']
        if result.get('duration_seconds'):
            print(f"   ⏱️  Call Duration: {result['duration_seconds']:.1f} seconds")
        if result.get('recording_stopped'):
            print(f"   🛑 Recording Stopped: Yes")
        if result.get('recording_url'):
            print(f"   🔗 Recording URL: {result['recording_url']}")
        print()
    
    # 9. Show final session state
    final_session = event_manager.get_session_info(call_id)
    if final_session:
        print("📊 Final Session State:")
        print(f"   Call Connection ID: {final_session.call_connection_id}")
        print(f"   Recording State: {final_session.recording_state}")
        print(f"   Connected At: {final_session.connected_at}")
        print(f"   Disconnected At: {final_session.disconnected_at}")
        if final_session.connected_at and final_session.disconnected_at:
            duration = (final_session.disconnected_at - final_session.connected_at).total_seconds()
            print(f"   Total Duration: {duration:.1f} seconds")
        print(f"   Recording URL: {final_session.recording_url}")
        print()
    
    print("🎉 Demo completed successfully!")
    print("=" * 70)


def demonstrate_recording_configuration():
    """
    Show different recording configuration options.
    """
    from services.acs.events import RecordingConfig
    
    print("🎛️  Recording Configuration Examples")
    print("=" * 50)
    
    # Basic configuration
    basic_config = RecordingConfig()
    print("📋 Basic Configuration (defaults):")
    print(f"   Auto-start: {basic_config.auto_start_on_connect}")
    print(f"   Auto-stop: {basic_config.auto_stop_on_disconnect}")
    print(f"   Format: {basic_config.recording_format}")
    print(f"   Channel: {basic_config.recording_channel}")
    print()
    
    # Medical/Healthcare configuration
    medical_config = RecordingConfig(
        auto_start_on_connect=True,
        auto_stop_on_disconnect=True,
        recording_format="wav",
        recording_channel="unmixed",
        storage_container="medical-calls",
        enable_transcription=True  # For medical notes
    )
    print("🏥 Medical/Healthcare Configuration:")
    print(f"   Auto-start: {medical_config.auto_start_on_connect}")
    print(f"   Storage: {medical_config.storage_container}")
    print(f"   Transcription: {medical_config.enable_transcription}")
    print()
    
    # Customer Service configuration
    service_config = RecordingConfig(
        auto_start_on_connect=True,
        auto_stop_on_disconnect=True,
        recording_format="mp4",  # Video calls
        recording_channel="mixed",  # Single audio track
        storage_container="customer-service",
        enable_transcription=True  # For quality monitoring
    )
    print("📞 Customer Service Configuration:")
    print(f"   Format: {service_config.recording_format}")
    print(f"   Channel: {service_config.recording_channel}")
    print(f"   Storage: {service_config.storage_container}")
    print()


if __name__ == "__main__":
    # Run the configuration demo first
    demonstrate_recording_configuration()
    print()
    
    # Run the main event processing demo
    asyncio.run(demonstrate_event_processing())
