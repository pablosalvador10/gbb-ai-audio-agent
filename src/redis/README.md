# Redis Implementation Documentation

## Overview

The RTAgent Voice AI Backend uses a sophisticated Redis-based session management system that provides hierarchical key organization, automatic TTL management, and seamless data persistence for Azure Communication Services (ACS) calls and conversation sessions.

## Architecture Components

### 1. RedisKeyManager
The central component responsible for hierarchical key structure and TTL management.

### 2. AsyncAzureRedisManager
Provides high-level async operations for conversation and call management.

### 3. ConversationManager
Handles conversation state with automatic migration between legacy and new key formats.

## Key Structure

The system uses a hierarchical key format:
```
{app_prefix}:{environment}:{data_type}:{identifier}:{component}
```

### Examples:
- `rtvoice:prod:call:call-connection-id-1234:session` (ACS call using call_connection_id)
- `rtvoice:prod:conversation:session-id-5678:context` (conversation using session_id)
- `rtvoice:dev:worker:worker-abc123:affinity` (worker using worker_id)

## Component Flow Diagrams

### 1. Redis Key Manager Flow

```mermaid
flowchart TD
    A@{ shape: rounded, label: "Application Request" } --> B@{ shape: diamond, label: "Data Type?" }
    B -->|Call| C@{ shape: rect, label: "call_key method" }
    B -->|Conversation| D@{ shape: rect, label: "conversation_key method" }
    B -->|Worker| E@{ shape: rect, label: "worker_key method" }
    
    C --> F@{ shape: rect, label: "DataType.CALL" }
    D --> G@{ shape: rect, label: "DataType.CONVERSATION" }
    E --> H@{ shape: rect, label: "DataType.WORKER" }
    
    F --> I@{ shape: rect, label: "build_key" }
    G --> I
    H --> I
    
    I --> J@{ shape: rect, label: "Hierarchical Key" }
    J --> K@{ shape: rect, label: "Apply TTL Policy" }
    K --> L@{ shape: cyl, label: "Redis Storage" }
    
    style A fill:#e1f5fe
    style L fill:#c8e6c9
    style J fill:#fff3e0
```

### 2. ACS Call Session Management

```mermaid
flowchart LR
    subgraph "ACS Event Processing"
        A@{ shape: rounded, label: "ACS Event" } --> B@{ shape: rect, label: "Extract call_connection_id" }
        B --> C@{ shape: rect, label: "Event Handler" }
    end
    
    subgraph "Redis Operations"
        C --> D@{ shape: rect, label: "AsyncAzureRedisManager" }
        D --> E@{ shape: rect, label: "RedisKeyManager" }
        E --> F@{ shape: diamond, label: "Key Type" }
        
        F -->|Session| G@{ shape: rect, label: "call:id:session" }
        F -->|Recording| H@{ shape: rect, label: "call:id:recording" }
        F -->|Participants| I@{ shape: rect, label: "call:id:participants" }
        F -->|Media Stream| J@{ shape: rect, label: "call:id:media_stream" }
    end
    
    subgraph "Data Storage"
        G --> K@{ shape: cyl, label: "Session Data" }
        H --> L@{ shape: cyl, label: "Recording State" }
        I --> M@{ shape: cyl, label: "Participant List" }
        J --> N@{ shape: cyl, label: "Stream Status" }
    end
    
    style A fill:#ffcdd2
    style K fill:#c8e6c9
    style L fill:#c8e6c9
    style M fill:#c8e6c9
    style N fill:#c8e6c9
```

### 3. Conversation State Management

```mermaid
flowchart TD
    subgraph "Session Creation"
        A@{ shape: rounded, label: "New Session Request" } --> B@{ shape: diamond, label: "Session ID Provided?" }
        B -->|Yes| C@{ shape: rect, label: "Use Provided ID" }
        B -->|No| D@{ shape: rect, label: "Generate UUID" }
        C --> E@{ shape: rect, label: "ConversationManager" }
        D --> E
    end
    
    subgraph "Data Operations"
        E --> F@{ shape: rect, label: "AsyncAzureRedisManager" }
        F --> G@{ shape: diamond, label: "Check New Format" }
        G -->|Found| H@{ shape: rect, label: "Load from New Keys" }
        G -->|Not Found| I@{ shape: diamond, label: "Check Legacy Format" }
        I -->|Found| J@{ shape: rect, label: "Load from Legacy" }
        I -->|Not Found| K@{ shape: rect, label: "Create New Session" }
        
        J --> L@{ shape: rect, label: "Migrate to New Format" }
        H --> M@{ shape: rect, label: "Session Ready" }
        K --> M
        L --> M
    end
    
    subgraph "Key Structure"
        M --> N@{ shape: rect, label: "conversation:id:context" }
        M --> O@{ shape: rect, label: "conversation:id:history" }
        M --> P@{ shape: rect, label: "conversation:id:transcript" }
    end
    
    style A fill:#e3f2fd
    style M fill:#fff3e0
    style N fill:#c8e6c9
    style O fill:#c8e6c9
    style P fill:#c8e6c9
```

### 4. Legacy Migration Process

```mermaid
flowchart LR
    A@{ shape: rect, label: "Legacy Key Format" } --> B@{ shape: rect, label: "migrate_legacy_key" }
    
    subgraph "Migration Logic"
        B --> C@{ shape: diamond, label: "Key Pattern" }
        C -->|session:id| D@{ shape: rect, label: "conversation:id:context" }
        C -->|call:id:*| E@{ shape: rect, label: "call:id:component" }
        C -->|cid:hist| F@{ shape: rect, label: "conversation:cid:history" }
        C -->|Other| G@{ shape: rect, label: "No Migration" }
    end
    
    subgraph "Data Transfer"
        D --> H@{ shape: rect, label: "Copy Context Data" }
        E --> I@{ shape: rect, label: "Copy Call Data" }
        F --> J@{ shape: rect, label: "Copy History Data" }
        
        H --> K@{ shape: rect, label: "Delete Legacy Key" }
        I --> K
        J --> K
    end
    
    K --> L@{ shape: rect, label: "Migration Complete" }
    
    style A fill:#ffcdd2
    style L fill:#c8e6c9
    style K fill:#fff3e0
```

### 5. TTL Management System

```mermaid
flowchart TD
    A@{ shape: rounded, label: "Data Storage Request" } --> B@{ shape: rect, label: "Determine Data Type" }
    
    subgraph "TTL Policies"
        B --> C@{ shape: diamond, label: "Data Type" }
        C -->|CALL| D@{ shape: rect, label: "30min - 4hrs" }
        C -->|CONVERSATION| E@{ shape: rect, label: "2hrs - 24hrs" }
        C -->|WORKER| F@{ shape: rect, label: "5min - 10min" }
        C -->|SYSTEM| G@{ shape: rect, label: "1hr - 24hrs" }
        C -->|CACHE| H@{ shape: rect, label: "5min - 30min" }
    end
    
    subgraph "TTL Application"
        D --> I@{ shape: rect, label: "get_ttl method" }
        E --> I
        F --> I
        G --> I
        H --> I
        
        I --> J@{ shape: rect, label: "Validate Custom TTL" }
        J --> K@{ shape: rect, label: "Apply to Redis Key" }
    end
    
    K --> L@{ shape: rect, label: "Auto Cleanup" }
    
    style A fill:#e1f5fe
    style L fill:#c8e6c9
    style I fill:#fff3e0
```

## Usage Patterns

### 1. ACS Call Session Setup

```python
# For ACS calls, always use the call_connection_id as the identifier
async def handle_call_connected(call_connection_id: str):
    # Store call session data
    session_key = redis_manager.key_manager.call_key(
        call_connection_id, 
        Component.SESSION
    )
    await redis_manager.store_call_session(call_connection_id, session_data)
    
    # Store participant information
    participants_key = redis_manager.key_manager.call_key(
        call_connection_id, 
        Component.PARTICIPANTS
    )
    await redis_manager.set_value(participants_key, participants_data)
```

### 2. Conversation Management

```python
# Create conversation manager with automatic key management
async def setup_conversation(session_id: str = None):
    cm = ConversationManager(
        session_id=session_id,  # Uses call_connection_id for ACS calls
        environment="prod"
    )
    
    # Load existing session or create new one
    cm = await ConversationManager.from_redis(session_id, redis_mgr)
    
    # Update conversation context
    await cm.update_context({"user_authenticated": True})
    
    # Add to conversation history
    await cm.append_to_history("user", "Hello")
```

### 3. Recording State Management

```python
# Store recording state for ACS calls
async def start_recording(call_connection_id: str, recording_id: str):
    recording_data = {
        "recording_id": recording_id,
        "state": "started",
        "storage_account": "recordings_storage"
    }
    
    await redis_manager.store_recording_state(
        call_connection_id, 
        recording_data
    )
```

### 4. Worker Affinity Management

```python
# Set worker affinity for call processing
async def assign_worker(call_connection_id: str, worker_id: str):
    await redis_manager.set_worker_affinity(
        call_connection_id, 
        worker_id
    )
    
    # Get assigned worker
    assigned_worker = await redis_manager.get_worker_affinity(
        call_connection_id
    )
```

## Data Flow Integration

### ACS Call Lifecycle with ConversationManager Integration

```mermaid
sequenceDiagram
    participant ACS as Azure Communication Services
    participant Handler as ACS Event Handler
    participant CM as ConversationManager
    participant Redis as Redis Manager
    participant AI as AI Service
    participant Analytics as Analytics Engine
    
    Note over ACS,Analytics: Phase 1: Call Initialization
    ACS->>Handler: CallConnected Event (call_connection_id)
    Handler->>CM: new ConversationManager(session_id=call_connection_id)
    CM->>Redis: Initialize hierarchical keys
    CM->>CM: set_context(call_state="connected", authenticated=False)
    CM->>CM: ensure_system_prompt() - authentication flow
    CM->>Redis: persist_to_redis(ttl=4hrs)
    CM->>Handler: Return initialized CM instance
    Handler->>ACS: Acknowledge call setup
    
    Note over ACS,Analytics: Phase 2: Authentication Flow
    loop Authentication Attempts (max 3)
        ACS->>Handler: User speech input
        Handler->>CM: from_redis(call_connection_id) - load state
        CM->>Redis: get_conversation_context() & get_conversation_history()
        CM->>CM: append_to_history("user", input)
        CM->>CM: update_context("authentication_attempts", count++)
        
        alt Authentication Success
            CM->>CM: set_context({authenticated: True, patient_info: data})
            CM->>CM: upsert_system_prompt() - switch to authenticated flow
            CM->>AI: Generate welcome message with patient context
            AI->>CM: Personalized welcome response
            CM->>CM: append_to_history("assistant", response)
            CM->>Redis: persist_to_redis() - save authenticated state
        else Authentication Failed
            CM->>CM: append_to_history("assistant", "Please try again")
            CM->>Redis: persist_to_redis()
            Note over CM: Continue loop or terminate if max attempts reached
        end
    end
    
    Note over ACS,Analytics: Phase 3: Active Conversation
    loop Conversation Turns
        ACS->>Handler: User message
        Handler->>CM: from_redis(call_connection_id)
        CM->>Redis: Load current conversation state
        CM->>CM: append_to_history("user", message)
        CM->>CM: update_context("last_message_time", timestamp)
        CM->>AI: Generate response(history=cm.hist, context=cm.context)
        AI->>CM: Contextual AI response
        CM->>CM: append_to_history("assistant", response)
        CM->>CM: update_context("message_count", count++)
        CM->>Redis: persist_to_redis()
        CM->>Handler: Return response
        Handler->>ACS: Send audio response to caller
    end
    
    Note over ACS,Analytics: Phase 4: Call Transfer (Optional)
    opt Transfer Required
        Handler->>CM: from_redis(call_connection_id)
        CM->>CM: update_context("transfer_reason", reason)
        CM->>AI: generate_transfer_summary(cm.hist, cm.context)
        AI->>CM: Transfer summary for human agent
        CM->>CM: set_context({call_state: "transferring", transfer_summary: summary})
        CM->>CM: append_to_history("system", "Transfer initiated")
        CM->>Redis: persist_to_redis(ttl=8hrs) - extend for human access
        Handler->>ACS: Execute call transfer
    end
    
    Note over ACS,Analytics: Phase 5: Recording Events
    ACS->>Handler: RecordingStarted Event
    Handler->>CM: from_redis(call_connection_id)
    CM->>CM: set_context({recording_active: True, recording_id: id})
    CM->>CM: append_to_history("system", "Recording started")
    CM->>Redis: persist_to_redis()
    
    ACS->>Handler: RecordingStopped Event
    Handler->>CM: from_redis(call_connection_id)
    CM->>CM: update_context("recording_url", download_url)
    CM->>CM: append_to_history("system", "Recording completed")
    CM->>Redis: persist_to_redis()
    
    Note over ACS,Analytics: Phase 6: Call Completion
    ACS->>Handler: CallDisconnected Event
    Handler->>CM: from_redis(call_connection_id)
    CM->>CM: calculate_call_metrics(start_time, end_time)
    CM->>CM: set_context({call_state: "completed", metrics: data})
    CM->>AI: generate_call_summary(cm.hist, cm.context)
    AI->>CM: Call summary and analytics
    CM->>CM: append_to_history("system", "Call completed")
    CM->>Redis: persist_to_redis(ttl=7days) - analytics retention
    Handler->>Analytics: queue_post_call_processing(call_connection_id)
    Analytics->>Redis: Access final conversation state for analysis
```

## Best Practices

### 1. Key Naming
- **Always use actual identifiers**: For ACS calls, use the `call_connection_id` provided by Azure Communication Services
- **Avoid generating UUIDs**: When external identifiers exist (like call_connection_id), use them directly
- **Consistent environment naming**: Use standard environment names: `dev`, `test`, `staging`, `prod`

### 2. TTL Management
- **Let the system manage TTLs**: Use the built-in TTL policies unless you have specific requirements
- **Custom TTLs for special cases**: Only override TTLs when you need longer persistence for specific use cases
- **Monitor expiration**: Implement monitoring for critical data that shouldn't expire unexpectedly

### 3. Migration Handling
- **Gradual migration**: The system supports both legacy and new key formats simultaneously
- **Automatic migration**: Use the built-in migration methods rather than manual key conversion
- **Verify migration**: Always verify that data is accessible after migration

### 4. Error Handling
- **Graceful degradation**: Handle Redis connection failures gracefully
- **Retry logic**: Implement exponential backoff for transient failures
- **Fallback strategies**: Have fallback mechanisms when Redis is unavailable

### 5. Performance Optimization
- **Batch operations**: Use batch operations for multiple related keys
- **Connection pooling**: Leverage the built-in connection pooling
- **Async operations**: Always use async methods for non-blocking operations

## Environment Configuration

### Development
```yaml
ENVIRONMENT: dev
REDIS_HOST: localhost
REDIS_PORT: 6379
TTL_MULTIPLIER: 0.5  # Shorter TTLs for testing
```

### Production
```yaml
ENVIRONMENT: prod
REDIS_HOST: redis-cluster.region.cache.windows.net
REDIS_PORT: 6380
REDIS_SSL: true
TTL_MULTIPLIER: 2.0  # Longer TTLs for production
```

## Monitoring and Debugging

### Key Metrics to Monitor
- **Key creation rate**: Monitor new session creation
- **TTL distribution**: Ensure keys are expiring as expected
- **Migration success rate**: Track legacy key migrations
- **Memory usage**: Monitor Redis memory consumption
- **Operation latency**: Track Redis operation performance

### Debugging Tools
```python
# List all keys for a specific call
pattern = redis_manager.key_manager.get_pattern(DataType.CALL, call_connection_id)
keys = await redis_manager.scan_keys(pattern)

# Check TTL for a specific key
key = redis_manager.key_manager.call_key(call_connection_id, Component.SESSION)
ttl = await redis_manager.get_ttl(key)

# Migrate legacy keys manually if needed
legacy_key = "session:old-format-id"
new_key = redis_manager.key_manager.migrate_legacy_key(legacy_key)
```

## Security Considerations

### 1. Data Privacy
- **Sensitive data encryption**: Encrypt sensitive conversation data before storage
- **Access control**: Implement proper access controls for Redis instances
- **Network security**: Use TLS/SSL for Redis connections in production

### 2. Key Security
- **Predictable patterns**: Avoid exposing internal system details in key names
- **Access logging**: Log access to sensitive conversation data
- **Key rotation**: Implement key rotation for long-lived sessions

### 3. Compliance
- **Data retention**: Respect data retention policies with appropriate TTLs
- **Audit trails**: Maintain audit trails for data access and modifications
- **Cleanup procedures**: Implement proper cleanup for terminated sessions

This Redis implementation provides a robust, scalable foundation for the RTAgent Voice AI Backend, ensuring efficient session management while maintaining data integrity and performance.

## End-to-End ConversationManager Usage

The ConversationManager is the primary interface for handling conversation state throughout the entire ACS call lifecycle. This section demonstrates proper integration patterns from call initiation to completion, with comprehensive examples showing how to leverage the ConversationManager as the central state management component for Azure Communication Services calls.

### Overview of ConversationManager Integration

The ConversationManager serves as the stateful backbone of every ACS call, providing:

- **Session State Management**: Persistent storage of conversation context and history
- **Authentication Flow**: Seamless handling of user authentication with state persistence
- **Context-Aware Responses**: AI-powered responses using full conversation context
- **Event Integration**: Integration with ACS events (recording, transfers, disconnections)
- **Analytics Support**: Call metrics and summary generation for post-call analysis
- **Legacy Migration**: Automatic migration from legacy key formats

### Complete Call Lifecycle with ConversationManager

### ConversationManager Architecture Patterns

#### Key Design Principles

1. **Session ID as Call Connection ID**: Always use the ACS `call_connection_id` as the `session_id` for ConversationManager instances. This ensures:
   - Direct correlation between ACS events and conversation state
   - Simplified debugging and tracing across ACS and Redis logs
   - Automatic cleanup when calls terminate

2. **Hierarchical State Management**: ConversationManager leverages Redis hierarchical keys:
   ```
   rtvoice:prod:conversation:call-connection-id-1234:context
   rtvoice:prod:conversation:call-connection-id-1234:history
   ```

3. **Event-Driven State Updates**: Every ACS event should trigger a ConversationManager state update:
   - Call events → context updates
   - User messages → history appends
   - System events → system message logs

4. **Context-Aware AI Integration**: Use full conversation context for AI responses:
   - Patient authentication status
   - Call duration and message count
   - Recording state and previous interactions
   - Transfer history and escalation context

#### Integration Patterns

The ConversationManager follows specific patterns for different call phases:

| Phase | Pattern | Key Operations |
|-------|---------|----------------|
| **Initialization** | Factory + Setup | `new ConversationManager()` → `ensure_system_prompt()` → `persist_to_redis()` |
| **Authentication** | Load + Validate + Update | `from_redis()` → validate credentials → `update_context()` → `upsert_system_prompt()` |
| **Conversation** | Load + Process + Store | `from_redis()` → `append_to_history()` → AI processing → `persist_to_redis()` |
| **Events** | Load + Log + Persist | `from_redis()` → `append_to_history("system")` → `update_context()` → `persist_to_redis()` |
| **Completion** | Load + Summarize + Archive | `from_redis()` → generate summary → final `persist_to_redis(extended_ttl)` |



### Best Practices for ConversationManager Usage

#### 1. Session ID Management
- **Always use `call_connection_id`** as the session_id for ACS calls
- **Never generate random UUIDs** when a meaningful identifier exists
- **Maintain session continuity** across call events and state changes

#### 2. Context Management
- **Set comprehensive initial context** during call connection
- **Update context incrementally** rather than replacing entire context
- **Use consistent key naming** for context attributes
- **Store temporal data** (timestamps, durations, counts) for analytics

#### 3. History Management
- **Add system messages** for important call events (recording, transfers)
- **Maintain conversation chronology** with proper role assignments
- **Limit history size** for very long conversations to manage memory
- **Include metadata** in system messages for debugging

#### 4. Error Handling and Recovery
- **Gracefully handle Redis failures** with fallback mechanisms
- **Implement retry logic** for transient failures
- **Validate conversation state** after loading from Redis
- **Log state transitions** for debugging and monitoring

#### 5. Performance Optimization
- **Use appropriate TTL values** based on call lifecycle phase
- **Batch Redis operations** when updating multiple context keys
- **Implement conversation state caching** for frequently accessed sessions
- **Monitor memory usage** for large conversation histories

This comprehensive approach ensures that the ConversationManager serves as a robust, stateful backbone for the entire call experience, maintaining context and conversation flow while integrating seamlessly with ACS events and Redis storage.
