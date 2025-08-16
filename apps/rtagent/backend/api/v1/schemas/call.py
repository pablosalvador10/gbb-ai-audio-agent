"""
Call-related API schemas.

Pydantic schemas for call management API requests and responses.
"""

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class CallInitiateRequest(BaseModel):
    """Request model for initiating a call."""

    target_number: str = Field(
        ...,
        description="Phone number to call in E.164 format (e.g., +1234567890)",
        example="+1234567890",
        pattern=r"^\+[1-9]\d{1,14}$",
    )
    caller_id: Optional[str] = Field(
        None,
        description="Caller ID to display (optional, uses system default if not provided)",
        example="+1987654321",
    )
    session_id: Optional[str] = Field(
        None,
        description="Session identifier to correlate call with browser tab/session",
        example="sess_abc12345",
    )
    context: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Additional call context metadata",
        example={
            "customer_id": "cust_12345",
            "department": "support",
            "priority": "high",
            "source": "web_portal",
        },
    )

    class Config:
        json_schema_extra = {
            "example": {
                "target_number": "+1234567890",
                "caller_id": "+1987654321",
                "session_id": "sess_abc12345",
                "context": {"customer_id": "cust_12345", "department": "support"},
            }
        }


class CallInitiateResponse(BaseModel):
    """Response model for call initiation."""

    call_id: str = Field(
        ..., description="Unique call identifier", example="call_abc12345"
    )
    status: str = Field(..., description="Current call status", example="initiating")
    target_number: str = Field(
        ..., description="Target phone number", example="+1234567890"
    )
    message: str = Field(
        ...,
        description="Human-readable status message",
        example="Call initiation requested",
    )

    class Config:
        json_schema_extra = {
            "example": {
                "call_id": "call_abc12345",
                "status": "initiating",
                "target_number": "+1234567890",
                "message": "Call initiation requested for +1234567890",
            }
        }


class CallStatusResponse(BaseModel):
    """Response model for call status."""

    call_id: str = Field(
        ..., description="Unique call identifier", example="call_abc12345"
    )
    status: str = Field(
        ...,
        description="Current call status",
        example="connected",
        enum=[
            "initiating",
            "ringing",
            "connected",
            "on_hold",
            "disconnected",
            "failed",
        ],
    )
    duration: Optional[int] = Field(
        None,
        description="Call duration in seconds (null if not connected)",
        example=120,
    )
    participants: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="List of call participants",
        example=[
            {
                "id": "participant_1",
                "phone_number": "+1234567890",
                "role": "caller",
                "status": "connected",
            }
        ],
    )
    events: List[Dict[str, Any]] = Field(
        default_factory=list,
        description="Recent call events",
        example=[
            {
                "type": "call_connected",
                "timestamp": "2025-08-10T13:45:30Z",
                "details": {"connection_established": True},
            }
        ],
    )

    class Config:
        json_schema_extra = {
            "example": {
                "call_id": "call_abc12345",
                "status": "connected",
                "duration": 120,
                "participants": [
                    {
                        "id": "participant_1",
                        "phone_number": "+1234567890",
                        "role": "caller",
                        "status": "connected",
                    }
                ],
                "events": [
                    {
                        "type": "call_connected",
                        "timestamp": "2025-08-10T13:45:30Z",
                        "details": {"connection_established": True},
                    }
                ],
            }
        }


class CallUpdateRequest(BaseModel):
    """Request model for updating call properties."""

    status: Optional[str] = Field(
        None,
        description="New call status",
        enum=["on_hold", "connected", "muted", "unmuted"],
    )
    metadata: Optional[Dict[str, Any]] = Field(
        None, description="Updated metadata for the call"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "status": "on_hold",
                "metadata": {
                    "hold_reason": "customer_request",
                    "hold_duration_estimate": 120,
                },
            }
        }


class CallHangupResponse(BaseModel):
    """Response model for call hangup."""

    call_id: str = Field(
        ..., description="Unique call identifier", example="call_abc12345"
    )
    status: str = Field(..., description="Updated call status", example="hanging_up")
    message: str = Field(
        ...,
        description="Human-readable status message",
        example="Call hangup requested",
    )

    class Config:
        json_schema_extra = {
            "example": {
                "call_id": "call_abc12345",
                "status": "hanging_up",
                "message": "Call hangup requested",
            }
        }


class CallListResponse(BaseModel):
    """Response model for listing calls."""

    calls: List[CallStatusResponse] = Field(..., description="List of calls")
    total: int = Field(
        ..., description="Total number of calls matching criteria", example=25
    )
    page: int = Field(1, description="Current page number (1-based)", example=1)
    limit: int = Field(10, description="Number of items per page", example=10)

    class Config:
        json_schema_extra = {
            "example": {
                "calls": [
                    {
                        "call_id": "call_abc12345",
                        "status": "connected",
                        "duration": 120,
                        "participants": [],
                        "events": [],
                    }
                ],
                "total": 25,
                "page": 1,
                "limit": 10,
            }
        }
