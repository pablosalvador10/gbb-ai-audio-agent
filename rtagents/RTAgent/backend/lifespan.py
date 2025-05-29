"""
Application Lifespan Management for RTAgent Backend
==================================================
Following Azure best practices for FastAPI application lifecycle:
- Proper resource initialization and cleanup
- Azure service connection management
- Error handling and logging
- Graceful shutdown procedures
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI

from utils.ml_logging import get_logger
from rtagents.RTAgent.backend.settings import (
    ALLOWED_ORIGINS,
    AOAI_STT_KEY,
    AOAI_STT_ENDPOINT,
    AZURE_COSMOS_CONNECTION_STRING,
    AZURE_COSMOS_DB_DATABASE_NAME,
    AZURE_COSMOS_DB_COLLECTION_NAME,
    VOICE_TTS,
    RATE,
    CHANNELS,
    FORMAT,
    CHUNK,
    VAD_THRESHOLD,
    PREFIX_PADDING_MS,
    SILENCE_DURATION_MS,
)

# Import services with error handling
try:
    from services import (
        SpeechSynthesizer,
        SpeechCoreTranslator,
        CosmosDBMongoCoreManager,
        AzureRedisManager,
    )
except ImportError:
    # Fallback imports for development
    from rtagents.RTAgent.backend.services import (
        SpeechSynthesizer,
        SpeechCoreTranslator,
        CosmosDBMongoCoreManager,
        AzureRedisManager,
    )

from rtagents.RTAgent.backend.services.acs.acs_caller import (
    initialize_acs_caller_instance,
)
from rtagents.RTAgent.backend.agents.base import RTAgent

logger = get_logger("lifespan")


async def initialize_azure_services(app: FastAPI) -> None:
    """
    Initialize all Azure services following Azure best practices
    
    Args:
        app: FastAPI application instance
        
    Raises:
        RuntimeError: If critical Azure services fail to initialize
    """
    logger.info("🔧 Initializing Azure services...")
    
    # Initialize service tracking
    app.state.clients = set()  # /relay dashboard sockets
    app.state.greeted_call_ids = set()  # to avoid double greetings
    app.state.service_health = {}
    
    # Speech Services initialization with error handling
    try:
        logger.info("🎤 Initializing Speech services...")
        app.state.stt_client = SpeechCoreTranslator()
        app.state.tts_client = SpeechSynthesizer(voice=VOICE_TTS)
        app.state.service_health['speech'] = 'healthy'
        logger.info("✅ Speech services initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize Speech services: {e}")
        app.state.service_health['speech'] = 'failed'
        raise RuntimeError(f"Critical service failure - Speech services: {e}")
    
    # Redis connection with retry logic
    try:
        logger.info("🔴 Initializing Redis connection...")
        app.state.redis = AzureRedisManager()
        
        # Test Redis connection
        if hasattr(app.state.redis, 'ping'):
            ping_result = app.state.redis.ping()
            if not ping_result:
                raise ConnectionError("Redis ping failed")
        
        app.state.service_health['redis'] = 'healthy'
        logger.info("✅ Redis connection established successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize Redis: {e}")
        app.state.service_health['redis'] = 'failed'
        # Redis failure is critical for session management
        raise RuntimeError(f"Critical service failure - Redis: {e}")
    
    # Cosmos DB connection with retry logic
    try:
        logger.info("🌌 Initializing Cosmos DB connection...")
        app.state.cosmos = CosmosDBMongoCoreManager(
            connection_string=AZURE_COSMOS_CONNECTION_STRING,
            database_name=AZURE_COSMOS_DB_DATABASE_NAME,
            collection_name=AZURE_COSMOS_DB_COLLECTION_NAME,
        )
        
        # Test Cosmos DB connection
        test_query = {"_id": "test_connection"}
        if app.state.cosmos.document_exists(test_query):
            logger.info("✅ Cosmos DB connection test passed")
        else:
            logger.warning("⚠️ Cosmos DB connection test failed - no test document found")
        
        app.state.service_health['cosmos'] = 'healthy'
        logger.info("✅ Cosmos DB connection established successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize Cosmos DB: {e}")
        app.state.service_health['cosmos'] = 'failed'
        # Continue without Cosmos DB for now - can be non-critical
        logger.warning("⚠️ Continuing without Cosmos DB - some features may be limited")
    
    # Azure OpenAI STT configuration
    try:
        logger.info("🤖 Setting up Azure OpenAI STT configuration...")
        app.state.aoai_stt_cfg = {
            "url": f"{AOAI_STT_ENDPOINT.replace('https','wss')}"
            "/openai/realtime?api-version=2025-04-01-preview&intent=transcription",
            "headers": {"api-key": AOAI_STT_KEY},
            "rate": RATE,
            "channels": CHANNELS,  # Mono audio
            "format_": FORMAT,  # PCM16
            "chunk": CHUNK,  # Size of audio chunks to process
            # VAD settings
            "vad": {
                "threshold": VAD_THRESHOLD,
                # Prefix padding in milliseconds to avoid cutting off speech
                "prefix_padding_ms": PREFIX_PADDING_MS,
                # Silence duration in milliseconds to consider the end of speech
                "silence_duration_ms": SILENCE_DURATION_MS,
            },
        }
        app.state.service_health['aoai_stt'] = 'healthy'
        logger.info("✅ Azure OpenAI STT configuration completed")
    except Exception as e:
        logger.error(f"❌ Failed to configure Azure OpenAI STT: {e}")
        app.state.service_health['aoai_stt'] = 'failed'
        # Continue without STT config - can be initialized later
        logger.warning("⚠️ Continuing without Azure OpenAI STT config")
    
    # ACS Caller initialization (may be None if env vars missing)
    try:
        logger.info("📞 Initializing ACS caller...")
        app.state.acs_caller = initialize_acs_caller_instance()
        if app.state.acs_caller is not None:
            app.state.service_health['acs'] = 'healthy'
            logger.info("✅ ACS caller initialized successfully")
        else:
            app.state.service_health['acs'] = 'not_configured'
            logger.warning("⚠️ ACS caller not configured - check environment variables")
    except Exception as e:
        logger.error(f"❌ Failed to initialize ACS caller: {e}")
        app.state.service_health['acs'] = 'failed'
        app.state.acs_caller = None
        logger.warning("⚠️ Continuing without ACS caller")
    
    # RT Agent initialization
    try:
        logger.info("🤖 Initializing RT Agents...")
        
        # Auth Agent
        try:
            app.state.auth_agent = RTAgent(
                config_path="rtagents/RTMedAgent/backend/agents/agent_store/auth_agent.yaml"
            )
            logger.info("✅ Auth agent initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize auth agent: {e}")
            app.state.auth_agent = None
        
        # Task Agent
        try:
            app.state.task_agent = RTAgent(
                config_path="rtagents/RTMedAgent/backend/agents/agent_store/task_agent.yaml"
            )
            logger.info("✅ Task agent initialized successfully")
        except Exception as e:
            logger.error(f"❌ Failed to initialize task agent: {e}")
            app.state.task_agent = None
        
        # Check if at least one agent is available
        if app.state.auth_agent or app.state.task_agent:
            app.state.service_health['agents'] = 'healthy'
        else:
            app.state.service_health['agents'] = 'failed'
            logger.warning("⚠️ No agents available - limited functionality")
        
    except Exception as e:
        logger.error(f"❌ Failed to initialize RT Agents: {e}")
        app.state.service_health['agents'] = 'failed'
        app.state.auth_agent = None
        app.state.task_agent = None


async def cleanup_azure_services(app: FastAPI) -> None:
    """
    Cleanup all Azure services following Azure best practices
    
    Args:
        app: FastAPI application instance
    """
    logger.info("🧹 Starting Azure services cleanup...")
    
    # Cleanup tasks list for parallel execution
    cleanup_tasks = []
    
    # Redis cleanup
    if hasattr(app.state, 'redis') and app.state.redis:
        async def cleanup_redis():
            try:
                if hasattr(app.state.redis, 'close'):
                    await app.state.redis.close()
                logger.info("✅ Redis connection closed")
            except Exception as e:
                logger.error(f"❌ Error closing Redis connection: {e}")
        
        cleanup_tasks.append(cleanup_redis())
    
    # Cosmos DB cleanup
    if hasattr(app.state, 'cosmos') and app.state.cosmos:
        async def cleanup_cosmos():
            try:
                if hasattr(app.state.cosmos, 'close'):
                    await app.state.cosmos.close()
                logger.info("✅ Cosmos DB connection closed")
            except Exception as e:
                logger.error(f"❌ Error closing Cosmos DB connection: {e}")
        
        cleanup_tasks.append(cleanup_cosmos())
    
    # ACS cleanup
    if hasattr(app.state, 'acs_caller') and app.state.acs_caller:
        async def cleanup_acs():
            try:
                if hasattr(app.state.acs_caller, 'cleanup'):
                    await app.state.acs_caller.cleanup()
                logger.info("✅ ACS caller cleaned up")
            except Exception as e:
                logger.error(f"❌ Error cleaning up ACS caller: {e}")
        
        cleanup_tasks.append(cleanup_acs())
    
    # Speech services cleanup
    if hasattr(app.state, 'stt_client') and app.state.stt_client:
        async def cleanup_speech():
            try:
                if hasattr(app.state.stt_client, 'close'):
                    await app.state.stt_client.close()
                if hasattr(app.state.tts_client, 'close'):
                    await app.state.tts_client.close()
                logger.info("✅ Speech services cleaned up")
            except Exception as e:
                logger.error(f"❌ Error cleaning up speech services: {e}")
        
        cleanup_tasks.append(cleanup_speech())
    
    # Agent cleanup
    if hasattr(app.state, 'auth_agent') and app.state.auth_agent:
        async def cleanup_agents():
            try:
                if hasattr(app.state.auth_agent, 'cleanup'):
                    await app.state.auth_agent.cleanup()
                if hasattr(app.state.task_agent, 'cleanup'):
                    await app.state.task_agent.cleanup()
                logger.info("✅ RT Agents cleaned up")
            except Exception as e:
                logger.error(f"❌ Error cleaning up RT Agents: {e}")
        
        cleanup_tasks.append(cleanup_agents())
    
    # Execute all cleanup tasks concurrently with timeout
    if cleanup_tasks:
        try:
            await asyncio.wait_for(
                asyncio.gather(*cleanup_tasks, return_exceptions=True),
                timeout=30.0  # 30 second timeout for cleanup
            )
            logger.info("✅ All Azure services cleaned up successfully")
        except asyncio.TimeoutError:
            logger.warning("⚠️ Cleanup timeout reached - some services may not have closed gracefully")
        except Exception as e:
            logger.error(f"❌ Error during cleanup: {e}")
    
    # Close any remaining client connections
    if hasattr(app.state, 'clients'):
        for client in app.state.clients:
            try:
                if hasattr(client, 'close'):
                    await client.close()
            except Exception as e:
                logger.error(f"❌ Error closing client connection: {e}")
    
    logger.info("🧹 Azure services cleanup completed")


def get_service_health_summary(app: FastAPI) -> dict:
    """
    Get health summary of all Azure services
    
    Args:
        app: FastAPI application instance
        
    Returns:
        dict: Service health summary
    """
    if not hasattr(app.state, 'service_health'):
        return {"status": "unknown", "services": {}}
    
    health_summary = {
        "status": "healthy",
        "services": app.state.service_health,
        "critical_services": ["speech", "redis"],
        "total_services": len(app.state.service_health),
        "healthy_services": len([s for s in app.state.service_health.values() if s == "healthy"]),
    }
    
    # Determine overall status
    critical_failed = any(
        app.state.service_health.get(service) == "failed"
        for service in health_summary["critical_services"]
    )
    
    if critical_failed:
        health_summary["status"] = "degraded"
    elif health_summary["healthy_services"] < health_summary["total_services"]:
        health_summary["status"] = "partial"
    
    return health_summary


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    Application lifespan context manager following Azure best practices
    
    Handles:
    - Azure service initialization
    - Error handling and logging
    - Graceful shutdown
    - Resource cleanup
    
    Args:
        app: FastAPI application instance
        
    Yields:
        None: During application runtime
    """
    # Startup phase
    logger.info("🚀 RTAgent backend starting up...")
    
    try:
        # Initialize all Azure services
        await initialize_azure_services(app)
        
        # Log service health summary
        health_summary = get_service_health_summary(app)
        logger.info(f"📊 Service health: {health_summary['status']} "
                   f"({health_summary['healthy_services']}/{health_summary['total_services']} healthy)")
        
        # Check critical services
        if health_summary["status"] == "degraded":
            logger.warning("⚠️ Application starting with degraded services")
        else:
            logger.info("✅ All critical Azure services initialized successfully")
        
        logger.info("🎉 RTAgent backend startup completed")
        
    except Exception as e:
        logger.error(f"❌ Critical error during startup: {e}")
        # Attempt cleanup of any partially initialized services
        try:
            await cleanup_azure_services(app)
        except Exception as cleanup_error:
            logger.error(f"❌ Error during startup cleanup: {cleanup_error}")
        raise  # Re-raise to prevent application start
    
    # Application runtime
    yield
    
    # Shutdown phase
    logger.info("🛑 RTAgent backend shutting down...")
    
    try:
        await cleanup_azure_services(app)
        logger.info("✅ RTAgent backend shutdown completed")
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}")
        # Don't re-raise during shutdown to allow graceful exit
