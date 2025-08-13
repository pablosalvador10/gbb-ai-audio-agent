"""
voice_agent.main
================
Entrypoint that stitches everything together:

• config / CORS
• shared objects on `app.state`  (Speech, Redis, ACS, TTS, dashboard-clients)
• route registration (routers package)
"""

from __future__ import annotations

import sys
import os
# Add parent directories to sys.path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', '..'))
sys.path.insert(0, os.path.dirname(__file__))

from utils.telemetry_config import setup_azure_monitor

# ---------------- Monitoring ------------------------------------------------
setup_azure_monitor(logger_name="rtagent")


from utils.ml_logging import get_logger

logger = get_logger("main")

import os
import time
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from opentelemetry import trace

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
    ENABLE_AUTH_VALIDATION
)
from apps.rtagent.backend.src.agents.base import RTAgent
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

# --------------------------------------------------------------------------- #
#  Lifecycle Management
# --------------------------------------------------------------------------- #
async def lifespan(app: FastAPI):
    """Manage application lifecycle: startup and shutdown events."""

    tracer = trace.get_tracer(__name__)

    # Startup
    with tracer.start_as_current_span("startup-lifespan") as span:
        logger.info("🚀 startup…")
        start_time = time.perf_counter()

        # Set span attributes for better correlation
        span.set_attributes(
            {
                "service.name": "rtagent-api",
                "service.version": "1.0.0",
                "startup.stage": "initialization",
            }
        )

        # Initialize app state
        app.state.clients = set()  # /relay dashboard sockets
        app.state.greeted_call_ids = set()  # to avoid double greetings

        # Speech SDK
        span.set_attribute("startup.stage", "speech_sdk")
        # Speech SDK
        app.state.tts_client = SpeechSynthesizer(voice=GREETING_VOICE_TTS, playback="always")
        app.state.stt_client = StreamingSpeechRecognizerFromBytes(
            use_semantic_segmentation=VAD_SEMANTIC_SEGMENTATION,
            vad_silence_timeout_ms=SILENCE_DURATION_MS,
            candidate_languages=RECOGNIZED_LANGUAGE,
            audio_format=AUDIO_FORMAT,
        )

        # Redis connection
        span.set_attribute("startup.stage", "redis")
        app.state.redis = AzureRedisManager()

        # Cosmos DB connection
        span.set_attribute("startup.stage", "cosmos_db")
        app.state.cosmos = CosmosDBMongoCoreManager(
            connection_string=AZURE_COSMOS_CONNECTION_STRING,
            database_name=AZURE_COSMOS_DATABASE_NAME,
            collection_name=AZURE_COSMOS_COLLECTION_NAME,
        )

        span.set_attribute("startup.stage", "openai_clients")
        app.state.azureopenai_client = azure_openai_client
        app.state.promptsclient = PromptManager()

        # Outbound ACS caller (may be None if env vars missing)
        span.set_attribute("startup.stage", "acs_agents")
        app.state.acs_caller = initialize_acs_caller_instance()
        app.state.auth_agent = RTAgent(config_path=AGENT_AUTH_CONFIG)
        app.state.claim_intake_agent = RTAgent(config_path=AGENT_CLAIM_INTAKE_CONFIG)
        app.state.general_info_agent = RTAgent(config_path=AGENT_GENERAL_INFO_CONFIG)

        # Legacy event registry
        # span.set_attribute("startup.stage", "event_system")
        # initialize_call_event_registry()
        # initialize_media_event_registry()
        
        # Initialize V1 event handlers during startup
        span.set_attribute("startup.stage", "v1_event_handlers")
        register_default_handlers()
        logger.info("✅ V1 event handlers registered at startup")

        # Initialize enterprise orchestrator
        span.set_attribute("startup.stage", "orchestrator")
        # Use environment variable to determine orchestrator preset, default to production
        orchestrator_preset = os.getenv("ORCHESTRATOR_PRESET", "production")
        logger.info(f"Initializing orchestrator with preset: {orchestrator_preset}")
        
        elapsed = time.perf_counter() - start_time
        logger.info(f"startup complete in {elapsed:.2f}s")

        # Set final span attributes
        span.set_attributes(
            {
                "startup.duration_sec": elapsed,
                "startup.stage": "complete",
                "startup.success": True,
            }
        )

    # Yield control to the application
    yield

    # Shutdown
    with tracer.start_as_current_span("shutdown-lifespan") as span:
        logger.info("🛑 shutdown…")
        span.set_attributes(
            {"service.name": "rtagent-api", "shutdown.stage": "cleanup"}
        )
        
        span.set_attribute("shutdown.success", True)


# --------------------------------------------------------------------------- #
#  App factory with Dynamic Documentation
# --------------------------------------------------------------------------- #

def create_app() -> FastAPI:
    """Create FastAPI app with static documentation."""
    
    # Get documentation
    from apps.rtagent.backend.api.swagger_docs import (
        get_tags, 
        get_description
    )
    
    tags = get_tags()
    description = get_description()
    
    app = FastAPI(
        title="Real-Time Voice Agent API",
        description=description,
        version="1.0.0",
        contact={
            "name": "Real-Time Voice Agent Team",
            "email": "support@example.com",
        },
        license_info={
            "name": "MIT License",
            "url": "https://opensource.org/licenses/MIT",
        },
        openapi_tags=tags,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json"
    )
    
    return app


# --------------------------------------------------------------------------- #
#  App Initialization with Dynamic Documentation
# --------------------------------------------------------------------------- #

def setup_app_middleware_and_routes(app: FastAPI):
    """Set up middleware and routes for the app."""
    # Add middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
        max_age=86400,
    )

    # If auth validation is enabled, add auth middleware with
    # excluded paths for ACS-specific connections and health check
    if ENABLE_AUTH_VALIDATION:
        @app.middleware("http")
        async def entraid_auth_middleware(request: Request, call_next):
            path = request.url.path
            if any(path.startswith(p) for p in ENTRA_EXEMPT_PATHS):
                return await call_next(request)

            try:
                await validate_entraid_token(request)
            except HTTPException as e:
                return JSONResponse(content={"error": e.detail}, status_code=e.status_code)

            return await call_next(request)

    # Include legacy routers for compatibility (maintain existing paths for backward compatibility)
    # app.include_router(api_router)

    # Include new V1 API
    app.include_router(v1_router)
    
    # Include health endpoints at root level for frontend compatibility
    from apps.rtagent.backend.api.v1.endpoints import health
    app.include_router(health.router, tags=["Health"])
    
    

# Create the app
app = None

def initialize_app():
    """Initialize app with static documentation."""
    global app
    app = create_app()
    setup_app_middleware_and_routes(app)
    return app

# Initialize the app
# Initialize the app
app = initialize_app()

# --------------------------------------------------------------------------- #
#  CLI entry-point
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import uvicorn
    
    port = int(os.environ.get("PORT", 8010))
    # For development with reload, use the import string instead of app object
    uvicorn.run(
        "main:app",  # Use import string for reload to work
        host="0.0.0.0",  # nosec: B104
        port=port,
        reload=True,
    )
