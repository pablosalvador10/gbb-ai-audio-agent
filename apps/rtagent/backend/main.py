"""
voice_agent.main
================
Entrypoint that stitches everything together:

• config / CORS
• shared objects on `app.state`  (Speech pools, Redis, ACS, dashboard-clients)
• route registration (routers package)
"""

from __future__ import annotations

import sys
import os

# Add parent directories to sys.path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, os.path.dirname(__file__))

from utils.telemetry_config import setup_azure_monitor

# ---------------- Monitoring ------------------------------------------------
setup_azure_monitor(logger_name="rtagent")

from utils.ml_logging import get_logger

logger = get_logger("main")

import time
import asyncio
from datetime import datetime
from typing import Awaitable, Callable, TypeVar
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from opentelemetry import trace
from src.pools.async_pool import AsyncPool

from apps.rtagent.backend.settings import (
    AGENT_AUTH_CONFIG,
    AGENT_CLAIM_INTAKE_CONFIG,
    AGENT_GENERAL_INFO_CONFIG,
    ALLOWED_ORIGINS,
    AUDIO_FORMAT,
    AZURE_COSMOS_COLLECTION_NAME,
    AZURE_COSMOS_CONNECTION_STRING,
    AZURE_COSMOS_DATABASE_NAME,
    RECOGNIZED_LANGUAGE,
    SILENCE_DURATION_MS,
    VAD_SEMANTIC_SEGMENTATION,
    GREETING_VOICE_TTS,
    ENTRA_EXEMPT_PATHS,
    ENABLE_AUTH_VALIDATION,
    # Documentation settings
    ENABLE_DOCS,
    DOCS_URL,
    REDOC_URL,
    OPENAPI_URL,
    SECURE_DOCS_URL,
    ENVIRONMENT,
    DEBUG_MODE,
)

from apps.rtagent.backend.src.agents.base import ARTAgent
from apps.rtagent.backend.src.utils.auth import validate_entraid_token
from apps.rtagent.backend.src.agents.prompt_store.prompt_manager import PromptManager

# from apps.rtagent.backend.src.routers import router as api_router
from apps.rtagent.backend.api.v1.router import v1_router
from apps.rtagent.backend.src.services import (
    AzureRedisManager,
    CosmosDBMongoCoreManager,
    SpeechSynthesizer,
    StreamingSpeechRecognizerFromBytes,
)
from apps.rtagent.backend.src.services.acs.acs_caller import (
    initialize_acs_caller_instance,
)
from apps.rtagent.backend.src.services.openai_services import (
    client as azure_openai_client,
)
from apps.rtagent.backend.api.v1.events.registration import register_default_handlers
from apps.rtagent.backend.src.sessions.session_manager import SessionManager



# --------------------------------------------------------------------------- #
#  Lifecycle Management
# --------------------------------------------------------------------------- #
async def lifespan(app: FastAPI):
    """
    Manage complete application lifecycle including startup and shutdown events.

    This function handles the initialization and cleanup of all application components
    including speech pools, Redis connections, Cosmos DB, Azure OpenAI clients, and
    ACS agents. It provides comprehensive resource management with proper tracing and
    error handling for production deployment.

    :param app: The FastAPI application instance requiring lifecycle management.
    :return: AsyncGenerator yielding control to the application runtime.
    :raises RuntimeError: If critical startup components fail to initialize.
    """
    tracer = trace.get_tracer(__name__)

    # ---- Startup ----
    with tracer.start_as_current_span("startup-lifespan") as span:
        logger.info("🚀 startup…")
        start_time = time.perf_counter()

        span.set_attributes(
            {
                "service.name": "rtagent-api",
                "service.version": "1.0.0",
                "startup.stage": "initialization",
            }
        )

        # ------------------------ Process-wide shared state -------------------
        # Enterprise session management with Redis backing
        span.set_attribute("startup.stage", "session_management")

        # Import unified session manager
        from apps.rtagent.backend.src.sessions import (
            initialize_session_manager
        )

        # Initialize Redis-backed session manager for horizontal scaling
        redis_manager = getattr(app.state, 'redis', None)
        if redis_manager and redis_manager.is_connected:
            # Redis-backed session manager for horizontal scaling
            app.state.session_manager = await initialize_session_manager(
                redis_manager=redis_manager,
                session_ttl_seconds=int(os.getenv("SESSION_TTL", "3600")),
                cleanup_interval_seconds=int(os.getenv("SESSION_CLEANUP_INTERVAL", "300")),
                max_connections_per_session=int(os.getenv("MAX_CONNECTIONS_PER_SESSION", "10")),
                max_total_connections=int(os.getenv("MAX_WEBSOCKET_CONNECTIONS", "50000")),
                graceful_degradation=True  # Allow connections beyond limit with degraded performance
            )
            await app.state.session_manager.start_background_cleanup()
            
            # Unified manager handles both sessions and websockets
            app.state.websocket_manager = app.state.session_manager
            
            logger.info("Initialized Redis-backed session manager for horizontal scaling")
        else:
            logger.warning("Redis not available, falling back to memory-only session management")
            # Fallback to memory-only session manager (single instance only)
            app.state.session_manager = await initialize_session_manager(
                redis_manager=None,  # No Redis - memory only
                session_ttl_seconds=int(os.getenv("SESSION_TTL", "3600")),
                cleanup_interval_seconds=int(os.getenv("SESSION_CLEANUP_INTERVAL", "300")),
                max_connections_per_session=int(os.getenv("MAX_CONNECTIONS_PER_SESSION", "10")),
                max_total_connections=int(os.getenv("MAX_WEBSOCKET_CONNECTIONS", "10000")),  # Reduced for memory-only
                graceful_degradation=True
            )
            await app.state.session_manager.start_background_cleanup()
            app.state.websocket_manager = app.state.session_manager

        # Production tracking
        app.state.greeted_call_ids = set()  # avoid double greetings

        # ------------------------ Speech Pools (TTS / STT) -------------------
        span.set_attribute("startup.stage", "speech_pools")

        # Pool sizes via env; tune to expected per-worker concurrency
        POOL_SIZE_TTS = int(os.getenv("POOL_SIZE_TTS", "8"))
        POOL_SIZE_STT = int(os.getenv("POOL_SIZE_STT", "8"))

        async def make_tts() -> SpeechSynthesizer:
            """
            Create and configure a new Text-to-Speech synthesizer instance.

            This factory function creates a properly configured SpeechSynthesizer
            with the appropriate voice settings and playback configuration for
            real-time audio generation in the voice agent system.

            :param: None (uses configuration from environment variables).
            :return: Configured SpeechSynthesizer instance ready for audio generation.
            :raises SpeechServiceError: If TTS service initialization fails.
            """
            # If SDK benefits from a warm-up, you can synth a short phrase here.
            return SpeechSynthesizer(voice=GREETING_VOICE_TTS, playback="always")

        async def make_stt() -> StreamingSpeechRecognizerFromBytes:
            """
            Create and configure a new Speech-to-Text recognizer instance.

            This factory function creates a properly configured streaming speech
            recognizer with semantic segmentation, VAD settings, and language
            configuration for real-time audio processing and transcription.

            :param: None (uses configuration from environment variables).
            :return: Configured StreamingSpeechRecognizerFromBytes instance ready for transcription.
            :raises SpeechServiceError: If STT service initialization fails.
            """
            return StreamingSpeechRecognizerFromBytes(
                use_semantic_segmentation=VAD_SEMANTIC_SEGMENTATION,
                vad_silence_timeout_ms=SILENCE_DURATION_MS,
                candidate_languages=RECOGNIZED_LANGUAGE,
                audio_format=AUDIO_FORMAT,
            )

        app.state.tts_pool = AsyncPool(make_tts, POOL_SIZE_TTS)
        app.state.stt_pool = AsyncPool(make_stt, POOL_SIZE_STT)

        # Warm both pools concurrently
        await asyncio.gather(
            app.state.tts_pool.prepare(),
            app.state.stt_pool.prepare(),
        )

        # ------------------------ Other singletons ---------------------------
        span.set_attribute("startup.stage", "redis")
        app.state.redis = AzureRedisManager()

        span.set_attribute("startup.stage", "cosmos_db")
        app.state.cosmos = CosmosDBMongoCoreManager(
            connection_string=AZURE_COSMOS_CONNECTION_STRING,
            database_name=AZURE_COSMOS_DATABASE_NAME,
            collection_name=AZURE_COSMOS_COLLECTION_NAME,
        )

        span.set_attribute("startup.stage", "session_statistics")
        from apps.rtagent.backend.src.sessions.session_statistics import SessionStatisticsManager
        app.state.session_statistics = SessionStatisticsManager(cosmos_manager=app.state.cosmos)
        await app.state.session_statistics.initialize()

        # Start background cleanup tasks (already done above)
        # Background cleanup was started when managers were initialized

        span.set_attribute("startup.stage", "openai_clients")
        app.state.azureopenai_client = azure_openai_client
        app.state.promptsclient = PromptManager()

        span.set_attribute("startup.stage", "acs_agents")
        app.state.acs_caller = initialize_acs_caller_instance()
        app.state.auth_agent = ARTAgent(config_path=AGENT_AUTH_CONFIG)
        app.state.claim_intake_agent = ARTAgent(config_path=AGENT_CLAIM_INTAKE_CONFIG)
        app.state.general_info_agent = ARTAgent(config_path=AGENT_GENERAL_INFO_CONFIG)

        # ------------------------ Events / Orchestrator -----------------------
        span.set_attribute("startup.stage", "v1_event_handlers")
        register_default_handlers()
        logger.info("✅ V1 event handlers registered at startup")

        span.set_attribute("startup.stage", "orchestrator")
        orchestrator_preset = os.getenv("ORCHESTRATOR_PRESET", "production")
        logger.info(f"Initializing orchestrator with preset: {orchestrator_preset}")

        elapsed = time.perf_counter() - start_time
        logger.info(f"startup complete in {elapsed:.2f}s")
        span.set_attributes(
            {
                "startup.duration_sec": elapsed,
                "startup.stage": "complete",
                "startup.success": True,
            }
        )

    # ---- Run app ----
    yield

    # ---- Shutdown ----
    with tracer.start_as_current_span("shutdown-lifespan") as span:
        logger.info("🛑 shutdown…")
        span.set_attributes(
            {"service.name": "rtagent-api", "shutdown.stage": "cleanup"}
        )
        
        # Stop production managers background cleanup
        try:
            # Stop session management
            if hasattr(app.state, 'session_manager'):
                await app.state.session_manager.stop_background_cleanup()
            logger.info("✅ Production session and WebSocket cleanup stopped")
        except Exception as e:
            logger.error(f"❌ Error stopping background cleanup: {e}")
        
        span.set_attribute("shutdown.success", True)


# --------------------------------------------------------------------------- #
#  App factory with Dynamic Documentation
# --------------------------------------------------------------------------- #
def create_app() -> FastAPI:
    """Create FastAPI app with configurable documentation."""

    # Conditionally get documentation based on settings
    if ENABLE_DOCS:
        from apps.rtagent.backend.api.swagger_docs import get_tags, get_description
        tags = get_tags()
        description = get_description()
        logger.info(f"📚 API documentation enabled for environment: {ENVIRONMENT}")
    else:
        tags = None
        description = "Real-Time Voice Agent API"
        logger.info(f"📚 API documentation disabled for environment: {ENVIRONMENT}")

    app = FastAPI(
        title="Real-Time Voice Agent API",
        description=description,
        version="1.0.0",
        contact={"name": "Real-Time Voice Agent Team", "email": "support@example.com"},
        license_info={
            "name": "MIT License",
            "url": "https://opensource.org/licenses/MIT",
        },
        openapi_tags=tags,
        lifespan=lifespan,
        docs_url=DOCS_URL,
        redoc_url=REDOC_URL,
        openapi_url=OPENAPI_URL,
    )

    # Add secure docs endpoint if configured and docs are enabled
    if SECURE_DOCS_URL and ENABLE_DOCS:
        from fastapi.openapi.docs import get_swagger_ui_html
        from fastapi.responses import HTMLResponse
        
        @app.get(SECURE_DOCS_URL, include_in_schema=False)
        async def secure_docs():
            """Secure documentation endpoint."""
            return get_swagger_ui_html(
                openapi_url=OPENAPI_URL or "/openapi.json",
                title=f"{app.title} - Secure Docs"
            )
        
        logger.info(f"🔒 Secure docs endpoint available at: {SECURE_DOCS_URL}")

    return app


# --------------------------------------------------------------------------- #
#  App Initialization with Dynamic Documentation
# --------------------------------------------------------------------------- #
def setup_app_middleware_and_routes(app: FastAPI):
    """
    Configure comprehensive middleware stack and route registration for the application.

    This function sets up CORS middleware for cross-origin requests, implements
    authentication middleware for Entra ID validation, and registers all API
    routers including v1 endpoints for health, calls, media, and real-time features.

    :param app: The FastAPI application instance to configure with middleware and routes.
    :return: None (modifies the application instance in place).
    :raises HTTPException: If authentication validation fails during middleware setup.
    """
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
        max_age=86400,
    )

    if ENABLE_AUTH_VALIDATION:

        @app.middleware("http")
        async def entraid_auth_middleware(request: Request, call_next):
            """
            Validate Entra ID authentication tokens for protected API endpoints.

            This middleware function checks incoming requests for valid authentication
            tokens, exempts specified paths from validation, and ensures proper
            security enforcement across the API surface area.

            :param request: The incoming HTTP request requiring authentication validation.
            :param call_next: The next middleware or endpoint handler in the chain.
            :return: HTTP response from the next handler or authentication error response.
            :raises HTTPException: If authentication token validation fails.
            """
            path = request.url.path
            if any(path.startswith(p) for p in ENTRA_EXEMPT_PATHS):
                return await call_next(request)
            try:
                await validate_entraid_token(request)
            except HTTPException as e:
                return JSONResponse(
                    content={"error": e.detail}, status_code=e.status_code
                )
            return await call_next(request)

    # app.include_router(api_router)  # legacy, if needed
    app.include_router(v1_router)

    # Health endpoints are now included in v1_router at /api/v1/health

    # Add environment and docs status info endpoint
    @app.get("/api/info", tags=["System"], include_in_schema=ENABLE_DOCS)
    async def get_system_info():
        """Get system environment and documentation status."""
        return {
            "environment": ENVIRONMENT,
            "debug_mode": DEBUG_MODE,
            "docs_enabled": ENABLE_DOCS,
            "docs_url": DOCS_URL,
            "redoc_url": REDOC_URL,
            "openapi_url": OPENAPI_URL,
            "secure_docs_url": SECURE_DOCS_URL,
        }


# Create the app
app = None


def initialize_app():
    """Initialize app with configurable documentation."""
    global app
    app = create_app()
    setup_app_middleware_and_routes(app)
    
    # Log documentation status
    if ENABLE_DOCS:
        logger.info(f"📚 Swagger docs available at: {DOCS_URL}")
        logger.info(f"📚 ReDoc available at: {REDOC_URL}")
        if SECURE_DOCS_URL:
            logger.info(f"🔒 Secure docs available at: {SECURE_DOCS_URL}")
    else:
        logger.info("📚 API documentation is disabled for this environment")
    
    return app


# Initialize the app
app = initialize_app()

# --------------------------------------------------------------------------- #
#  Main entry point for uv run
# --------------------------------------------------------------------------- #
def main():
    """Entry point for uv run rtagent-server."""
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(
        app,                  # Use app object directly
        host="0.0.0.0",      # nosec: B104
        port=port,
        reload=False,         # Don't use reload in production
    )

# --------------------------------------------------------------------------- #
#  CLI entry-point
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8010))
    # For development with reload, use the import string instead of app object
    uvicorn.run(
        "main:app",  # Use import string for reload to work
        host="0.0.0.0",  # nosec: B104
        port=port,
        reload=True,
    )
