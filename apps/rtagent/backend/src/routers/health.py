import asyncio
import time
from typing import Dict, List, Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from settings import (
    ACS_CONNECTION_STRING,
    ACS_ENDPOINT,
    ACS_SOURCE_PHONE_NUMBER,
    AZURE_CLIENT_ID,
    AZURE_OPENAI_ENDPOINT,
    AZURE_TENANT_ID,
)

from src.stateful.state_managment import MemoManager
from utils.ml_logging import get_logger

logger = get_logger("health")

router = APIRouter()


def _validate_phone_number(phone_number: str) -> tuple[bool, str]:
    """
    Validate ACS phone number format.
    Returns (is_valid, error_message_if_invalid)
    """
    if not phone_number or phone_number == "null":
        return False, "Phone number not provided"

    if not phone_number.startswith("+"):
        return False, f"Phone number must start with '+': {phone_number}"

    if not phone_number[1:].isdigit():
        return False, f"Phone number must contain only digits after '+': {phone_number}"

    if len(phone_number) < 8 or len(phone_number) > 16:  # Basic length validation
        return (
            False,
            f"Phone number length invalid (8-15 digits expected): {phone_number}",
        )

    return True, ""


@router.get("/health")
async def health():
    """
    Basic health check endpoint - always returns 200 if server is running.
    Used by load balancers for basic liveness checks.
    """
    return {"status": "healthy", "message": "Server is running!"}


@router.get("/readiness")
async def readiness(request: Request):
    """
    Fast readiness probe: checks only that core dependencies are initialized and responsive within 1-5s.
    No deep or blocking checks. Returns degraded if any are not ready.
    """
    start_time = time.time()
    health_checks = []
    overall_status = "ready"
    timeout = 1.0  # seconds per check

    async def fast_ping(check_fn, *args, component=None):
        try:
            result = await asyncio.wait_for(check_fn(*args), timeout=timeout)
            return result
        except Exception as e:
            return {
                "component": component or check_fn.__name__,
                "status": "unhealthy",
                "error": str(e),
                "check_time_ms": round((time.time() - start_time) * 1000, 2),
            }

    # Only check if initialized and can respond to a ping/basic call
    redis_status = await fast_ping(
        _check_redis_fast, request.app.state.redis, component="redis"
    )
    health_checks.append(redis_status)

    openai_status = await fast_ping(
        _check_azure_openai_fast,
        request.app.state.azureopenai_client,
        component="azure_openai",
    )
    health_checks.append(openai_status)

    speech_status = await fast_ping(
        _check_speech_services_fast,
        request.app.state.tts_client,
        request.app.state.stt_client,
        component="speech_services",
    )
    health_checks.append(speech_status)

    acs_status = await fast_ping(
        _check_acs_caller_fast, request.app.state.acs_caller, component="acs_caller"
    )
    health_checks.append(acs_status)

    agent_status = await fast_ping(
        _check_rt_agents_fast,
        request.app.state.auth_agent,
        request.app.state.claim_intake_agent,
        component="rt_agents",
    )
    health_checks.append(agent_status)

    failed_checks = [check for check in health_checks if check["status"] != "healthy"]
    if failed_checks:
        overall_status = (
            "degraded" if len(failed_checks) < len(health_checks) else "unhealthy"
        )

    response_time = round((time.time() - start_time) * 1000, 2)
    response_data = {
        "status": overall_status,
        "timestamp": time.time(),
        "response_time_ms": response_time,
        "checks": health_checks,
    }
    # Always return quickly, never block
    return JSONResponse(
        content=response_data, status_code=200 if overall_status != "unhealthy" else 503
    )


async def _check_redis_fast(redis_manager) -> Dict:
    start = time.time()
    if not redis_manager:
        return {
            "component": "redis",
            "status": "unhealthy",
            "error": "not initialized",
            "check_time_ms": round((time.time() - start) * 1000, 2),
        }
    try:
        pong = await asyncio.wait_for(redis_manager.ping(), timeout=0.5)
        if pong:
            return {
                "component": "redis",
                "status": "healthy",
                "check_time_ms": round((time.time() - start) * 1000, 2),
            }
        else:
            return {
                "component": "redis",
                "status": "unhealthy",
                "error": "no pong",
                "check_time_ms": round((time.time() - start) * 1000, 2),
            }
    except Exception as e:
        return {
            "component": "redis",
            "status": "unhealthy",
            "error": str(e),
            "check_time_ms": round((time.time() - start) * 1000, 2),
        }


async def _check_azure_openai_fast(openai_client) -> Dict:
    start = time.time()
    if not openai_client:
        return {
            "component": "azure_openai",
            "status": "unhealthy",
            "error": "not initialized",
            "check_time_ms": round((time.time() - start) * 1000, 2),
        }
    return {
        "component": "azure_openai",
        "status": "healthy",
        "check_time_ms": round((time.time() - start) * 1000, 2),
    }


async def _check_speech_services_fast(tts_client, stt_client) -> Dict:
    start = time.time()
    if not tts_client or not stt_client:
        return {
            "component": "speech_services",
            "status": "unhealthy",
            "error": "not initialized",
            "check_time_ms": round((time.time() - start) * 1000, 2),
        }
    return {
        "component": "speech_services",
        "status": "healthy",
        "check_time_ms": round((time.time() - start) * 1000, 2),
    }


async def _check_acs_caller_fast(acs_caller) -> Dict:
    """Fast ACS caller check with comprehensive phone number and config validation."""
    start = time.time()

    # Check if ACS phone number is provided
    if not ACS_SOURCE_PHONE_NUMBER or ACS_SOURCE_PHONE_NUMBER == "null":
        return {
            "component": "acs_caller",
            "status": "unhealthy",
            "error": "ACS_SOURCE_PHONE_NUMBER not provided",
            "check_time_ms": round((time.time() - start) * 1000, 2),
        }

    # Validate phone number format
    is_valid, error_msg = _validate_phone_number(ACS_SOURCE_PHONE_NUMBER)
    if not is_valid:
        return {
            "component": "acs_caller",
            "status": "unhealthy",
            "error": f"ACS phone number validation failed: {error_msg}",
            "check_time_ms": round((time.time() - start) * 1000, 2),
        }

    # Check ACS connection string or endpoint id
    acs_conn_missing = not ACS_CONNECTION_STRING
    acs_endpoint_missing = not ACS_ENDPOINT
    if acs_conn_missing and acs_endpoint_missing:
        return {
            "component": "acs_caller",
            "status": "unhealthy",
            "error": "Neither ACS_CONNECTION_STRING nor ACS_ENDPOINT is configured",
            "check_time_ms": round((time.time() - start) * 1000, 2),
        }

    if not acs_caller:
        # Try to diagnose why ACS caller is not configured
        missing = []
        if not is_valid:
            missing.append(f"ACS_SOURCE_PHONE_NUMBER ({error_msg})")
        if not ACS_CONNECTION_STRING:
            missing.append("ACS_CONNECTION_STRING")
        if not ACS_ENDPOINT:
            missing.append("ACS_ENDPOINT")
        details = (
            f"ACS caller not configured. Missing: {', '.join(missing)}"
            if missing
            else "ACS caller not initialized for unknown reason"
        )
        return {
            "component": "acs_caller",
            "status": "unhealthy",
            "check_time_ms": round((time.time() - start) * 1000, 2),
            "details": details,
        }

    # Obfuscate phone number, show only last 4 digits
    obfuscated_phone = (
        "*" * (len(ACS_SOURCE_PHONE_NUMBER) - 4) + ACS_SOURCE_PHONE_NUMBER[-4:]
        if len(ACS_SOURCE_PHONE_NUMBER) > 4
        else ACS_SOURCE_PHONE_NUMBER
    )
    return {
        "component": "acs_caller",
        "status": "healthy",
        "check_time_ms": round((time.time() - start) * 1000, 2),
        "details": f"ACS caller configured with phone: {obfuscated_phone}",
    }


async def _check_rt_agents_fast(auth_agent, claim_intake_agent) -> Dict:
    start = time.time()
    if not auth_agent or not claim_intake_agent:
        return {
            "component": "rt_agents",
            "status": "unhealthy",
            "error": "not initialized",
            "check_time_ms": round((time.time() - start) * 1000, 2),
        }
    return {
        "component": "rt_agents",
        "status": "healthy",
        "check_time_ms": round((time.time() - start) * 1000, 2),
    }


@router.get("/agents")
async def get_agents_info(request: Request):
    """
    Get information about loaded RT agents including their configuration,
    model settings, and voice settings that can be modified.
    """
    start_time = time.time()
    agents_info = []
    
    try:
        # Get agents from app state
        auth_agent = getattr(request.app.state, 'auth_agent', None)
        claim_intake_agent = getattr(request.app.state, 'claim_intake_agent', None)
        general_info_agent = getattr(request.app.state, 'general_info_agent', None)
        
        # Helper function to extract agent info
        def extract_agent_info(agent, config_path: str = None):
            if not agent:
                return None
                
            try:
                # Get voice setting from agent configuration
                agent_voice = getattr(agent, 'voice_name', None)
                agent_voice_style = getattr(agent, 'voice_style', 'conversational')
                
                # Fallback to global GREETING_VOICE_TTS if agent doesn't have voice configured
                from settings import GREETING_VOICE_TTS
                current_voice = agent_voice or GREETING_VOICE_TTS
                
                agent_info = {
                    "name": getattr(agent, 'name', 'Unknown'),
                    "status": "loaded",
                    "creator": getattr(agent, 'creator', 'Unknown'),
                    "organization": getattr(agent, 'organization', 'Unknown'),
                    "description": getattr(agent, 'description', ''),
                    "model": {
                        "deployment_id": getattr(agent, 'model_id', 'Unknown'),
                        "temperature": getattr(agent, 'temperature', 0.7),
                        "top_p": getattr(agent, 'top_p', 1.0),
                        "max_tokens": getattr(agent, 'max_tokens', 4096)
                    },
                    "voice": {
                        "current_voice": current_voice,
                        "voice_style": agent_voice_style,
                        "voice_configurable": True,
                        "is_per_agent_voice": bool(agent_voice)  # True if agent has its own voice
                    },
                    "config_path": config_path,
                    "prompt_path": getattr(agent, 'prompt_path', 'Unknown'),
                    "tools": [tool.get('function', {}).get('name', 'Unknown') for tool in getattr(agent, 'tools', [])],
                    "modifiable_settings": {
                        "model_deployment": True,
                        "temperature": True,
                        "voice_name": True,
                        "voice_style": True,
                        "max_tokens": True
                    }
                }
                return agent_info
            except Exception as e:
                logger.warning(f"Error extracting agent info: {e}")
                return {
                    "name": getattr(agent, 'name', 'Unknown'),
                    "status": "error",
                    "error": str(e)
                }
        
        # Extract info for each agent
        if auth_agent:
            from settings import AGENT_AUTH_CONFIG
            agent_info = extract_agent_info(auth_agent, AGENT_AUTH_CONFIG)
            if agent_info:
                agents_info.append(agent_info)
                
        if claim_intake_agent:
            from settings import AGENT_CLAIM_INTAKE_CONFIG
            agent_info = extract_agent_info(claim_intake_agent, AGENT_CLAIM_INTAKE_CONFIG)
            if agent_info:
                agents_info.append(agent_info)
                
        if general_info_agent:
            from settings import AGENT_GENERAL_INFO_CONFIG
            agent_info = extract_agent_info(general_info_agent, AGENT_GENERAL_INFO_CONFIG)
            if agent_info:
                agents_info.append(agent_info)
        
        response_time = round((time.time() - start_time) * 1000, 2)
        
        return {
            "status": "success",
            "agents_count": len(agents_info),
            "agents": agents_info,
            "response_time_ms": response_time,
            "available_voices": {
                "turbo_voices": [
                    "en-US-AlloyTurboMultilingualNeural",
                    "en-US-EchoTurboMultilingualNeural", 
                    "en-US-FableTurboMultilingualNeural",
                    "en-US-OnyxTurboMultilingualNeural",
                    "en-US-NovaTurboMultilingualNeural",
                    "en-US-ShimmerTurboMultilingualNeural"
                ],
                "standard_voices": [
                    "en-US-AvaMultilingualNeural",
                    "en-US-AndrewMultilingualNeural",
                    "en-US-EmmaMultilingualNeural",
                    "en-US-BrianMultilingualNeural"
                ],
                "hd_voices": [
                    "en-US-Ava:DragonHDLatestNeural",
                    "en-US-Andrew:DragonHDLatestNeural",
                    "en-US-Brian:DragonHDLatestNeural",
                    "en-US-Emma:DragonHDLatestNeural"
                ]
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting agents info: {e}")
        return JSONResponse(
            content={
                "status": "error", 
                "error": str(e),
                "response_time_ms": round((time.time() - start_time) * 1000, 2)
            },
            status_code=500
        )


class AgentModelUpdate(BaseModel):
    deployment_id: Optional[str] = None
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    max_tokens: Optional[int] = None

class AgentVoiceUpdate(BaseModel):
    voice_name: Optional[str] = None
    voice_style: Optional[str] = None

class AgentConfigUpdate(BaseModel):
    model: Optional[AgentModelUpdate] = None
    voice: Optional[AgentVoiceUpdate] = None


@router.put("/agents/{agent_name}")
async def update_agent_config(agent_name: str, config: AgentConfigUpdate, request: Request):
    """
    Update configuration for a specific agent (model settings, voice, etc.).
    Changes are applied to the runtime instance but not persisted to YAML files.
    """
    start_time = time.time()
    
    try:
        # Get the agent instance from app state
        agent = None
        if agent_name.lower() in ['authagent', 'auth_agent', 'auth']:
            agent = getattr(request.app.state, 'auth_agent', None)
        elif agent_name.lower() in ['fnolintakeagent', 'claim_intake_agent', 'claim', 'fnol']:
            agent = getattr(request.app.state, 'claim_intake_agent', None)
        elif agent_name.lower() in ['generalinfoagent', 'general_info_agent', 'general']:
            agent = getattr(request.app.state, 'general_info_agent', None)
        
        if not agent:
            raise HTTPException(
                status_code=404, 
                detail=f"Agent '{agent_name}' not found. Available agents: auth, claim, general"
            )
        
        updated_fields = []
        
        # Update model settings
        if config.model:
            if config.model.deployment_id is not None:
                agent.model_id = config.model.deployment_id
                updated_fields.append(f"deployment_id -> {config.model.deployment_id}")
            
            if config.model.temperature is not None:
                if 0.0 <= config.model.temperature <= 2.0:
                    agent.temperature = config.model.temperature
                    updated_fields.append(f"temperature -> {config.model.temperature}")
                else:
                    raise HTTPException(status_code=400, detail="Temperature must be between 0.0 and 2.0")
            
            if config.model.top_p is not None:
                if 0.0 <= config.model.top_p <= 1.0:
                    agent.top_p = config.model.top_p
                    updated_fields.append(f"top_p -> {config.model.top_p}")
                else:
                    raise HTTPException(status_code=400, detail="top_p must be between 0.0 and 1.0")
            
            if config.model.max_tokens is not None:
                if 1 <= config.model.max_tokens <= 16384:
                    agent.max_tokens = config.model.max_tokens
                    updated_fields.append(f"max_tokens -> {config.model.max_tokens}")
                else:
                    raise HTTPException(status_code=400, detail="max_tokens must be between 1 and 16384")
        
        # Update voice settings per agent
        if config.voice:
            if config.voice.voice_name is not None:
                agent.voice_name = config.voice.voice_name
                updated_fields.append(f"voice_name -> {config.voice.voice_name}")
                logger.info(f"Updated {agent.name} voice to: {config.voice.voice_name}")
            
            if config.voice.voice_style is not None:
                agent.voice_style = config.voice.voice_style
                updated_fields.append(f"voice_style -> {config.voice.voice_style}")
                logger.info(f"Updated {agent.name} voice style to: {config.voice.voice_style}")
        
        response_time = round((time.time() - start_time) * 1000, 2)
        
        return {
            "status": "success",
            "agent_name": agent.name,
            "updated_fields": updated_fields,
            "message": f"Successfully updated {len(updated_fields)} settings for {agent.name}",
            "response_time_ms": response_time,
            "note": "Changes applied to runtime instance. Restart required for persistence."
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating agent config: {e}")
        return JSONResponse(
            content={
                "status": "error",
                "error": str(e),
                "response_time_ms": round((time.time() - start_time) * 1000, 2)
            },
            status_code=500
        )
