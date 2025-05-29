"""
voice_agent.main
================
Entrypoint that stitches everything together following Azure best practices:

• Modern FastAPI lifespan management for Azure services
• CORS configuration 
• Route registration
• Azure service initialization and cleanup
• Proper error handling and logging
"""
from __future__ import annotations
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from utils.ml_logging import get_logger

# Set the current directory as the Python path
current_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(current_dir))

from rtagents.RTAgent.backend.settings import ALLOWED_ORIGINS
from rtagents.RTAgent.backend.routers import router as api_router
from lifespan import lifespan
logger = get_logger("main")


# --------------------------------------------------------------------------- #
#  App factory with modern lifespan management
# --------------------------------------------------------------------------- #
def create_app() -> FastAPI:
    """
    Create FastAPI application with proper Azure service lifecycle management
    
    Returns:
        FastAPI: Configured application instance
    """
    # Initialize FastAPI with lifespan context manager
    app = FastAPI(
        title="RTAgent Voice AI Backend",
        description="Voice AI agent with Azure Communication Services integration",
        version="1.0.0",
        lifespan=lifespan,  # Use the lifespan context manager
    )

    # ---------------- Middleware ------------------------------------------------
    app.add_middleware(
        CORSMiddleware,
        allow_origins=ALLOWED_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---------------- Routers ---------------------------------------------------
    app.include_router(api_router)
    
    logger.info("✅ FastAPI application created with Azure lifespan management")
    return app


# Create the application instance
app = create_app()


# --------------------------------------------------------------------------- #
#  Health check endpoint for Azure monitoring
# --------------------------------------------------------------------------- #
@app.get("/health")
async def health_check():
    """
    Health check endpoint for Azure Load Balancer and Application Gateway
    
    Returns:
        dict: Service health status and details
    """
    from lifespan import get_service_health_summary
    
    try:
        health_summary = get_service_health_summary(app)
        return {
            "status": health_summary["status"],
            "timestamp": None,  # Will be added by monitoring
            "services": health_summary["services"],
            "details": {
                "total_services": health_summary["total_services"],
                "healthy_services": health_summary["healthy_services"],
                "critical_services_healthy": all(
                    health_summary["services"].get(service) == "healthy"
                    for service in health_summary["critical_services"]
                ),
            }
        }
    except Exception as e:
        logger.error(f"❌ Health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": None,
        }


# --------------------------------------------------------------------------- #
#  CLI entry-point
# --------------------------------------------------------------------------- #
if __name__ == "__main__":
    import uvicorn


    logger.info("🚀 Starting RTAgent Voice AI Backend server...")
    
    # Production-ready uvicorn configuration for Azure deployment
    uvicorn.run(
        "main:app",  # Use import string to support reload
        host="0.0.0.0",
        port=8010,
        reload=True,  # Disable in production
        access_log=True,
        log_level="info",
        # Azure-friendly configuration
        timeout_keep_alive=30,
        limit_concurrency=1000,
        limit_max_requests=10000,
    )
