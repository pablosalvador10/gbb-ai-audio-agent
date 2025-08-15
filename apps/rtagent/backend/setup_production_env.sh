#!/bin/bash

# Production Environment Configuration
# Sets up environment variables for production-ready concurrent call handling

echo "🔧 Setting up production environment configuration..."

# Azure Monitor / Telemetry Configuration
export AZURE_MONITOR_STORAGE_MAX_SIZE=209715200  # 200MB (increased from default ~50MB)
export AZURE_MONITOR_MAINTENANCE_PERIOD=60       # 1 minute cleanup cycle for high volume
export AZURE_MONITOR_STORAGE_DIR="/tmp/azure_monitor"
export AZURE_MONITOR_DISABLE_LIVE_METRICS=false  # Enable live metrics in production

# Production Call Manager Configuration  
export PRODUCTION_MAX_CONCURRENT_CALLS=2000      # Maximum concurrent calls
export REDIS_MAX_CONNECTIONS=2000                # Redis connection pool size
export REDIS_HEALTH_CHECK_INTERVAL=30            # Redis health check frequency
export REDIS_CONNECTION_TIMEOUT=5                # Redis connection timeout
export REDIS_SOCKET_TIMEOUT=5                    # Redis socket timeout
export REDIS_SOCKET_KEEPALIVE=true               # Enable Redis keepalive

# Circuit Breaker Configuration
export REDIS_CIRCUIT_BREAKER_FAILURE_THRESHOLD=5 # Failures before opening circuit
export REDIS_CIRCUIT_BREAKER_TIMEOUT=60          # Circuit breaker timeout in seconds

# Performance Optimization
export ORCHESTRATOR_PRESET=production            # Use production orchestrator preset
export ACS_STREAMING_MODE=media                  # Use media streaming mode
export VAD_SEMANTIC_SEGMENTATION=false          # Disable for better performance
export SILENCE_DURATION_MS=800                   # Faster speech detection

# Logging Configuration
export LOG_LEVEL=INFO                             # Production log level
export ENABLE_TRACE_LOGGING=false               # Disable verbose tracing in production

echo "✅ Production environment configured!"
echo ""
echo "📊 Key Production Settings:"
echo "  • Max Concurrent Calls: $PRODUCTION_MAX_CONCURRENT_CALLS"
echo "  • Redis Max Connections: $REDIS_MAX_CONNECTIONS"
echo "  • Telemetry Storage Limit: $AZURE_MONITOR_STORAGE_MAX_SIZE bytes ($(($AZURE_MONITOR_STORAGE_MAX_SIZE / 1024 / 1024))MB)"
echo "  • Circuit Breaker Timeout: $REDIS_CIRCUIT_BREAKER_TIMEOUT seconds"
echo ""
echo "🚀 Ready for production deployment with 1000+ concurrent calls!"

# Optional: Write to .env file for persistence
if [ "$1" = "--save" ]; then
    echo "💾 Saving configuration to .env file..."
    {
        echo ""
        echo "# Production Configuration - Generated $(date)"
        echo "AZURE_MONITOR_STORAGE_MAX_SIZE=$AZURE_MONITOR_STORAGE_MAX_SIZE"
        echo "AZURE_MONITOR_MAINTENANCE_PERIOD=$AZURE_MONITOR_MAINTENANCE_PERIOD"
        echo "AZURE_MONITOR_STORAGE_DIR=$AZURE_MONITOR_STORAGE_DIR"
        echo "AZURE_MONITOR_DISABLE_LIVE_METRICS=$AZURE_MONITOR_DISABLE_LIVE_METRICS"
        echo "PRODUCTION_MAX_CONCURRENT_CALLS=$PRODUCTION_MAX_CONCURRENT_CALLS"
        echo "REDIS_MAX_CONNECTIONS=$REDIS_MAX_CONNECTIONS"
        echo "REDIS_HEALTH_CHECK_INTERVAL=$REDIS_HEALTH_CHECK_INTERVAL"
        echo "REDIS_CONNECTION_TIMEOUT=$REDIS_CONNECTION_TIMEOUT"
        echo "REDIS_SOCKET_TIMEOUT=$REDIS_SOCKET_TIMEOUT"
        echo "REDIS_SOCKET_KEEPALIVE=$REDIS_SOCKET_KEEPALIVE"
        echo "REDIS_CIRCUIT_BREAKER_FAILURE_THRESHOLD=$REDIS_CIRCUIT_BREAKER_FAILURE_THRESHOLD"
        echo "REDIS_CIRCUIT_BREAKER_TIMEOUT=$REDIS_CIRCUIT_BREAKER_TIMEOUT"
        echo "ORCHESTRATOR_PRESET=$ORCHESTRATOR_PRESET"
        echo "ACS_STREAMING_MODE=$ACS_STREAMING_MODE"
        echo "VAD_SEMANTIC_SEGMENTATION=$VAD_SEMANTIC_SEGMENTATION"
        echo "SILENCE_DURATION_MS=$SILENCE_DURATION_MS"
        echo "LOG_LEVEL=$LOG_LEVEL"
        echo "ENABLE_TRACE_LOGGING=$ENABLE_TRACE_LOGGING"
    } >> .env
    echo "✅ Configuration saved to .env file"
fi
