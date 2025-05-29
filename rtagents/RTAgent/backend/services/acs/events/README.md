# ACS Event Handler System

## Overview

The ACS Event Handler System provides comprehensive event management for Azure Communication Services (ACS) call operations with automatic recording capabilities. This system replaces the basic event logging in the original ACS router with a sophisticated event-driven architecture that follows Azure best practices.

## Features

### 🎯 Core Capabilities
- **Centralized Event Management**: Single entry point for all ACS callback events
- **Automatic Recording**: Start/stop recording based on call lifecycle events
- **Session Tracking**: Maintain call state across multiple events
- **Error Resilience**: Comprehensive error handling with retry logic
- **Performance Monitoring**: Track call duration, recording status, and event processing
- **Azure Best Practices**: Follows WAF principles with structured logging and correlation IDs

### 📞 Supported Event Types
- `Microsoft.Communication.CallConnected` - Call establishment with automatic recording start
- `Microsoft.Communication.CallDisconnected` - Call termination with recording cleanup
- `Microsoft.Communication.MediaStreamingStarted` - Media flow initiation
- `Microsoft.Communication.MediaStreamingStopped` - Media flow termination
- `Microsoft.Communication.RecordingStateChanged` - Recording status updates
- `Microsoft.Communication.PlayCompleted` - Audio playback completion
- `Microsoft.Communication.PlayFailed` - Audio playback failures
- `Microsoft.Communication.PlayCanceled` - Audio playback cancellation

## Architecture

### Directory Structure
```
rtagents/RTAgent/backend/services/acs/events/
├── __init__.py                    # Package exports
├── acs_event_manager.py          # Main event manager (NEW)
├── base_handler.py               # Base event handler class
├── call_event_handler.py         # Call lifecycle events  
├── media_event_handler.py        # Media streaming events
└── recording_event_handler.py    # Recording management (WIP)
```

### Key Components

#### 1. ACSEventManager
The central coordinator that:
- Routes events to appropriate handlers
- Manages call session state
- Handles recording lifecycle
- Provides session information APIs

#### 2. RecordingConfig
Configuration for automatic recording:
```python
RecordingConfig(
    auto_start_on_connect=True,      # Start recording when call connects
    auto_stop_on_disconnect=True,    # Stop recording when call disconnects
    recording_format="wav",          # Audio format (wav, mp4)
    recording_channel="unmixed",     # Channel type (mixed, unmixed)
    storage_container="call-recordings",  # Blob storage container
    enable_transcription=False       # Enable call transcription
)
```

#### 3. CallSession
Tracks the complete call lifecycle:
```python
CallSession(
    call_connection_id: str,         # ACS call connection ID
    server_call_id: str,            # ACS server call ID
    participant_raw_id: str,        # Participant identifier
    recording_id: str,              # Recording session ID
    recording_state: str,           # Current recording state
    connected_at: datetime,         # Call start time
    disconnected_at: datetime,      # Call end time
    media_streaming_active: bool,   # Media streaming status
    recording_url: str              # Download URL for recording
)
```

## Integration with Existing Router

### Updated Callback Handler
The `/call/callbacks` endpoint now uses the event manager:

```python
@router.post(ACS_CALLBACK_PATH)
async def callbacks(request: Request):
    """Enhanced ACS callback handler with recording management."""
    events = await request.json()
    
    # Process through centralized event manager
    result = await acs_event_manager.process_callback_events(request, events)
    
    return {
        "status": "callback processed",
        "correlation_id": result.get("correlation_id"),
        "processed_events": len(result.get("processed_events", [])),
        "errors": result.get("errors", []),
    }
```

### New Management Endpoints

#### Get Active Sessions
```http
GET /call/sessions
```
Returns all active call sessions with recording status.

#### Get Specific Session
```http  
GET /call/{call_connection_id}/session
```
Returns detailed information for a specific call session.

#### Cleanup Old Sessions
```http
POST /call/sessions/cleanup?max_age_hours=24
```
Removes inactive sessions older than specified hours.

## Recording Workflow

### Automatic Recording Start
1. **Call Connected Event** received
2. Extract call and participant information
3. Create `CallSession` object
4. Start recording if `auto_start_on_connect=True`
5. Track recording ID and state
6. Log success/failure with correlation ID

### Recording State Management
1. **Recording State Changed Event** received
2. Update session with new recording state
3. Handle specific states:
   - `active`: Recording successfully started
   - `stopped`/`inactive`: Recording completed, get download URL
   - `failed`: Log error and alert

### Automatic Recording Stop
1. **Call Disconnected Event** received
2. Stop active recording if `auto_stop_on_disconnect=True`
3. Calculate call duration
4. Generate secure download URL
5. Clean up session state

## Error Handling & Resilience

### Retry Logic
- **Exponential Backoff**: 2^attempt seconds for transient failures
- **Circuit Breaker**: Prevent cascade failures
- **Graceful Degradation**: Continue operation even if recording fails

### Comprehensive Logging
```python
logger.info(
    "Recording started successfully",
    extra={
        "call_connection_id": call_connection_id,
        "recording_id": recording_session.recording_id,
        "correlation_id": correlation_id,
        "format": self.config.format.value,
        "channel": self.config.channel.value,
    }
)
```

### Error Categories
- **Transient Errors**: Rate limiting, temporary service unavailability
- **Configuration Errors**: Invalid recording settings
- **Resource Errors**: Storage unavailable, insufficient permissions
- **Network Errors**: Connection timeouts, DNS resolution failures

## Performance Monitoring

### Key Metrics Tracked
- **Call Duration**: From connect to disconnect
- **Recording Duration**: Active recording time
- **Event Processing Time**: Callback handling latency
- **Error Rates**: Failed operations per time period
- **Session Lifecycle**: Create, update, cleanup operations

### Azure Monitor Integration
All events include structured logging with:
- Correlation IDs for request tracing
- Performance counters for monitoring
- Error details for alerting
- Custom dimensions for filtering

## Usage Examples

### Basic Usage
```python
from services.acs.events import ACSEventManager, RecordingConfig

# Create event manager with recording
recording_config = RecordingConfig(auto_start_on_connect=True)
event_manager = ACSEventManager(recording_config)

# Process events (typically in FastAPI route)
result = await event_manager.process_callback_events(request, events)
```

### Custom Recording Configuration
```python
# Advanced recording setup
recording_config = RecordingConfig(
    auto_start_on_connect=True,
    auto_stop_on_disconnect=True,
    recording_format="wav",
    recording_channel="unmixed",
    storage_container="medical-calls",
    enable_transcription=True
)
```

### Session Management
```python
# Get active session info
session = event_manager.get_session_info(call_connection_id)

# List all active sessions  
sessions = event_manager.list_active_sessions()

# Cleanup old sessions
removed = await event_manager.cleanup_old_sessions(max_age_hours=24)
```

## Configuration

### Environment Variables
```bash
# Recording Configuration
ACS_RECORDING_AUTO_START=true
ACS_RECORDING_FORMAT=wav
ACS_RECORDING_CHANNEL=unmixed
ACS_STORAGE_CONTAINER=call-recordings

# Azure Storage for Recordings
AZURE_STORAGE_ACCOUNT_NAME=your_storage_account
AZURE_STORAGE_CONTAINER=recordings

# Monitoring
AZURE_APPLICATION_INSIGHTS_KEY=your_insights_key
LOG_LEVEL=INFO
```

### Recording Storage Options
1. **ACS Default Storage**: Recordings stored in ACS-managed storage
2. **Custom Azure Storage**: Direct storage to your Blob Storage account
3. **External Storage**: Integration with third-party storage providers

## Security Considerations

### Access Control
- **Managed Identity**: All Azure service access uses managed identity
- **RBAC**: Least privilege access to ACS and Storage resources
- **SAS Tokens**: Time-limited access for recording downloads
- **Network Security**: Private endpoints for internal communication

### Data Protection
- **Encryption at Rest**: All recordings encrypted in storage
- **Encryption in Transit**: TLS 1.2+ for all communications
- **Data Retention**: Automated cleanup based on retention policies
- **Audit Logging**: All access logged for compliance

## Testing

### Unit Tests
```bash
# Run event handler tests
pytest tests/services/acs/events/

# Run integration tests
pytest tests/integration/acs_events/
```

### Load Testing
```bash
# Test callback event processing
artillery run tests/load/acs_callbacks.yml

# Test concurrent call handling
artillery run tests/load/concurrent_calls.yml
```

## Future Enhancements

### Planned Features
1. **Real-time Transcription**: Live transcription during calls
2. **Sentiment Analysis**: Real-time mood detection
3. **Call Analytics**: Advanced metrics and insights
4. **Multi-language Support**: Automatic language detection
5. **Custom Event Handlers**: Plugin architecture for custom logic

### Integration Roadmap
1. **Azure Cognitive Services**: Speech-to-text and translation
2. **Power BI**: Real-time call analytics dashboards
3. **Microsoft Teams**: Integration with Teams calling
4. **Third-party CRM**: Automatic call logging to CRM systems

## Troubleshooting

### Common Issues

#### Recording Not Starting
1. Check `auto_start_on_connect` configuration
2. Verify ACS caller permissions
3. Review correlation ID in logs
4. Check Azure service health

#### Missing Session Information
1. Ensure callbacks endpoint is configured
2. Verify event routing to event manager
3. Check session cleanup intervals
4. Review error logs for processing failures

#### Download URL Generation Fails
1. Verify recording completed successfully
2. Check Azure Storage permissions
3. Ensure recording retention policy
4. Review SAS token configuration

### Debug Mode
Enable detailed logging:
```python
import logging
logging.getLogger("services.acs.events").setLevel(logging.DEBUG)
```

### Health Checks
Monitor event manager health:
```http
GET /health/acs-events
```

## Migration Guide

### From Basic Event Logging
1. **Update Imports**: Replace basic event handling with `ACSEventManager`
2. **Configure Recording**: Set up `RecordingConfig` based on requirements
3. **Update Routes**: Replace callback handler with event manager integration
4. **Test Events**: Verify all event types are properly handled
5. **Monitor Performance**: Set up alerts for error rates and latency

### Breaking Changes
- Event processing now returns structured responses
- Session state is maintained across events  
- Recording configuration is centralized
- Error handling is more comprehensive

## Support

### Documentation
- [Azure Communication Services Documentation](https://docs.microsoft.com/azure/communication-services/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Azure Monitor Documentation](https://docs.microsoft.com/azure/azure-monitor/)

### Monitoring
- Application Insights for telemetry
- Azure Monitor for infrastructure metrics
- Custom dashboards for call analytics
- Alerting for error conditions

### Contact
For questions or issues with the ACS Event Handler System:
1. Check existing logs and correlation IDs
2. Review configuration and permissions
3. Create issue with detailed error information
4. Include correlation ID and timestamps
