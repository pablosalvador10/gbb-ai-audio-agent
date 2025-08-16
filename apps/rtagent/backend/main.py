"""
voice_agent.main
================
Entrypoint that stitches everything together:

• config / CORS
• shared objects on `app.state`  (Speech, Redis, ACS, TTS, dashboard-clients, session registries)
• route registration (routers package)
"""

from __future__ import annotations

import sys
import os

# Add parent directories to sys.path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))
sys.path.insert(0, os.path.dirname(__file__))

# from utils.telemetry_config import setup_azure_monitor

# ---------------- Monitoring ------------------------------------------------
# setup_azure_monitor(logger_name="rtagent")  # Temporarily disabled for debugging

from utils.ml_logging import get_logger

logger = get_logger("main")

import time
import asyncio
from datetime import datetime
from collections import defaultdict

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
    ENABLE_AUTH_VALIDATION,
)
from apps.rtagent.backend.src.agents.base import RTAgent
from apps.rtagent.backend.src.utils.auth import validate_entraid_token
from apps.rtagent.backend.src.agents.prompt_store.prompt_manager import PromptManager

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


# ------------------------------------------------------------------------------
# Helper utilities (safe warmups / shutdown)
# ------------------------------------------------------------------------------

async def _maybe_call(obj, *method_names, timeout: float = 3.0):
    """Best-effort call to a method on obj (supports async or sync)."""
    for name in method_names:
        fn = getattr(obj, name, None)
        if not fn:
            continue
        try:
            if asyncio.iscoroutinefunction(fn):
                return await asyncio.wait_for(fn(), timeout=timeout)
            # If it returns awaitable anyway
            res = fn()
            if asyncio.iscoroutine(res):
                return await asyncio.wait_for(res, timeout=timeout)
            return res
        except Exception as e:
            logger.warning(f"Warmup/shutdown method '{name}' failed on {type(obj).__name__}: {e}")
    return None


async def _warmup_dependencies(app: FastAPI):
    """Best-effort warmup (don’t fail startup if something is slow)."""
    logger.info("🔥 warmup start")

    tasks = []

    # Redis ping
    if hasattr(app.state, "redis"):
        tasks.append(_maybe_call(app.state.redis, "warmup", "ping"))

    # Cosmos health_check or ping
    if hasattr(app.state, "cosmos"):
        tasks.append(_maybe_call(app.state.cosmos, "warmup", "health_check", "ping"))

    # Azure OpenAI: some SDKs provide a warmup; otherwise no-op
    if hasattr(app.state, "azureopenai_client"):
        tasks.append(_maybe_call(app.state.azureopenai_client, "warmup"))

    # TTS/STT: many SDKs don’t like being called at startup; only call if explicit warmup exists
    if hasattr(app.state, "tts_client"):
        tasks.append(_maybe_call(app.state.tts_client, "warmup", "prewarm"))
    if hasattr(app.state, "stt_client"):
        tasks.append(_maybe_call(app.state.stt_client, "warmup", "prewarm"))

    # Agents can expose warmups too
    for agent_name in ("auth_agent", "claim_intake_agent", "general_info_agent"):
        if hasattr(app.state, agent_name):
            tasks.append(_maybe_call(getattr(app.state, agent_name), "warmup"))

    # Run with global timeout; swallow errors
    try:
        await asyncio.wait_for(asyncio.gather(*tasks, return_exceptions=True), timeout=5.0)
    except asyncio.TimeoutError:
        logger.warning("Warmup timed out (continuing startup)")

    logger.info("🔥 warmup end")


async def _graceful_close(obj):
    """Try to close/dispose/aclose an object without raising."""
    await _maybe_call(obj, "aclose", "close", "dispose", timeout=2.0)


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

        span.set_attributes(
            {
                "service.name": "rtagent-api",
                "service.version": "1.0.0",
                "startup.stage": "initialization",
                "worker.pid": os.getpid(),
            }
        )

        # ------------------------ Process-wide shared state -------------------
        app.state.started_at = datetime.utcnow().isoformat()
        app.state.worker_pid = os.getpid()

        # 🧭 Dashboard clients (process-wide)
        app.state.clients = set()              # /relay dashboard sockets
        app.state.greeted_call_ids = set()     # avoid double greetings

        # 🔐 Session routing registries (process-wide, concurrency-safe)
        # - session_sockets: session_id -> set[WebSocket]
        # - call_session:    call_id    -> session_id  (ACS mapping)
        # - session_lock:    protects both structures
        app.state.session_lock = asyncio.Lock()
        app.state.session_sockets = defaultdict(set)
        app.state.call_session = {}

        # lightweight counters (optional)
        app.state.session_metrics = {
            "ws_connected": 0,
            "ws_disconnected": 0,
            "last_updated": datetime.utcnow().isoformat(),
        }
        # ---------------------------------------------------------------------

        # Speech SDK factories/config (shared)
        span.set_attribute("startup.stage", "speech_sdk")
        app.state.tts_client = SpeechSynthesizer(
            voice=GREETING_VOICE_TTS, playback="always"
        )
        app.state.stt_client = StreamingSpeechRecognizerFromBytes(
            use_semantic_segmentation=VAD_SEMANTIC_SEGMENTATION,
            vad_silence_timeout_ms=SILENCE_DURATION_MS,
            candidate_languages=RECOGNIZED_LANGUAGE,
            audio_format=AUDIO_FORMAT,
        )
        # NOTE: per-WS streams are created inside WS handlers using these factories.

        # Redis connection (single client with internal pool)
        span.set_attribute("startup.stage", "redis")
        app.state.redis = AzureRedisManager()

        # Cosmos DB connection
        span.set_attribute("startup.stage", "cosmos_db")
        app.state.cosmos = CosmosDBMongoCoreManager(
            connection_string=AZURE_COSMOS_CONNECTION_STRING,
            database_name=AZURE_COSMOS_DATABASE_NAME,
            collection_name=AZURE_COSMOS_COLLECTION_NAME,
        )

        # OpenAI / prompts
        span.set_attribute("startup.stage", "openai_clients")
        app.state.azureopenai_client = azure_openai_client
        app.state.promptsclient = PromptManager()

        # Outbound ACS caller (may be None if env vars missing)
        span.set_attribute("startup.stage", "acs_agents")
        app.state.acs_caller = initialize_acs_caller_instance()

        # Agents (stateless or read shared state)
        app.state.auth_agent = RTAgent(config_path=AGENT_AUTH_CONFIG)
        app.state.claim_intake_agent = RTAgent(config_path=AGENT_CLAIM_INTAKE_CONFIG)
        app.state.general_info_agent = RTAgent(config_path=AGENT_GENERAL_INFO_CONFIG)

        # Register v1 event handlers at startup (idempotent)
        span.set_attribute("startup.stage", "v1_event_handlers")
        register_default_handlers()
        logger.info("✅ V1 event handlers registered at startup")

        # Orchestrator preset (informational)
        span.set_attribute("startup.stage", "orchestrator")
        orchestrator_preset = os.getenv("ORCHESTRATOR_PRESET", "production")
        logger.info(f"Initializing orchestrator with preset: {orchestrator_preset}")

        # Best-effort warmups with small timeouts (don’t block startup)
        try:
            await _warmup_dependencies(app)
        except Exception as e:
            logger.warning(f"Warmup encountered issues (continuing): {e}")

        elapsed = time.perf_counter() - start_time
        logger.info(f"startup complete in {elapsed:.2f}s")

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

        # Graceful close of known resources (best-effort)
        for attr in (
            "redis",
            "cosmos",
            "tts_client",
            "stt_client",
            "acs_caller",
            "promptsclient",
            "azureopenai_client",
        ):
            obj = getattr(app.state, attr, None)
            if obj:
                try:
                    await _graceful_close(obj)
                except Exception as e:
                    logger.warning(f"Error during shutdown of {attr}: {e}")

        span.set_attribute("shutdown.success", True)


# --------------------------------------------------------------------------- #
#  App factory with Dynamic Documentation
# --------------------------------------------------------------------------- #
def create_app() -> FastAPI:
    """Create FastAPI app with static documentation."""

    # Get documentation
    from apps.rtagent.backend.api.swagger_docs import get_tags, get_description

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
        openapi_url="/openapi.json",
    )

    return app


# --------------------------------------------------------------------------- #
#  App Initialization with Dynamic Documentation
# --------------------------------------------------------------------------- #
def setup_app_middleware_and_routes(app: FastAPI):
    """Set up middleware and routes for the app."""
    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allow_headers=["*"],
        max_age=86400,
    )

    # Optional Entra ID validation for non-exempt paths
    if ENABLE_AUTH_VALIDATION:

        @app.middleware("http")
        async def entraid_auth_middleware(request: Request, call_next):
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

    # Include v1 API
    app.include_router(v1_router)

    # Health endpoints at root level
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


# Initialize the app at import time (for ASGI servers)
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
