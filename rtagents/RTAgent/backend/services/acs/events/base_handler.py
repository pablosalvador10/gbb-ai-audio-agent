"""
Base Event Handler for ACS Events
==================================
Abstract base class for all ACS event handlers following Azure best practices.
"""

from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from azure.core.messaging import CloudEvent
from fastapi import Request

from utils.ml_logging import get_logger


class BaseEventHandler(ABC):
    """
    Abstract base class for ACS event handlers
    
    Follows Azure best practices for:
    - Error handling and resilience
    - Structured logging with correlation IDs
    - Performance monitoring
    - Resource cleanup
    """
    
    def __init__(self, name: str):
        self.name = name
        self.logger = get_logger(f"acs.events.{name}")
        self._performance_metrics = {}
    
    async def handle_event(
        self, 
        event: CloudEvent, 
        request: Request,
        correlation_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Main event handling entry point with monitoring and error handling
        
        Args:
            event: CloudEvent from ACS
            request: FastAPI request object
            correlation_id: Optional correlation ID for tracking
            
        Returns:
            Dict containing event processing results
        """
        start_time = datetime.now(timezone.utc)
        event_type = event.type
        call_connection_id = event.data.get("callConnectionId", "unknown")
        
        # Generate correlation ID if not provided
        if not correlation_id:
            correlation_id = f"{call_connection_id}_{int(start_time.timestamp())}"
        
        try:
            self.logger.info(
                "🔄 Processing event",
                extra={
                    "event_type": event_type,
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                    "handler": self.name,
                }
            )
            
            # Validate event data
            await self._validate_event(event)
            
            # Process the specific event
            result = await self._process_event(event, request, correlation_id)
            
            # Calculate processing time
            processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            self._performance_metrics[event_type] = processing_time
            
            self.logger.info(
                "✅ Event processed successfully",
                extra={
                    "event_type": event_type,
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                    "processing_time_ms": processing_time * 1000,
                    "handler": self.name,
                }
            )
            
            return {
                "status": "success",
                "event_type": event_type,
                "call_connection_id": call_connection_id,
                "correlation_id": correlation_id,
                "processing_time_ms": processing_time * 1000,
                "result": result,
            }
            
        except Exception as e:
            processing_time = (datetime.now(timezone.utc) - start_time).total_seconds()
            
            self.logger.error(
                "❌ Event processing failed",
                extra={
                    "event_type": event_type,
                    "call_connection_id": call_connection_id,
                    "correlation_id": correlation_id,
                    "error": str(e),
                    "processing_time_ms": processing_time * 1000,
                    "handler": self.name,
                },
                exc_info=True
            )
            
            # Attempt cleanup on failure
            try:
                await self._cleanup_on_error(event, request, correlation_id)
            except Exception as cleanup_error:
                self.logger.error(
                    "💥 Cleanup failed after event processing error",
                    extra={
                        "event_type": event_type,
                        "call_connection_id": call_connection_id,
                        "correlation_id": correlation_id,
                        "cleanup_error": str(cleanup_error),
                        "handler": self.name,
                    }
                )
            
            return {
                "status": "error",
                "event_type": event_type,
                "call_connection_id": call_connection_id,
                "correlation_id": correlation_id,
                "error": str(e),
                "processing_time_ms": processing_time * 1000,
            }
    
    async def _validate_event(self, event: CloudEvent) -> None:
        """
        Validate event data before processing
        
        Args:
            event: CloudEvent to validate
            
        Raises:
            ValueError: If event data is invalid
        """
        if not event.data:
            raise ValueError("Event data is missing")
        
        if not event.data.get("callConnectionId"):
            raise ValueError("Call connection ID is missing from event data")
        
        # Allow subclasses to add specific validation
        await self._validate_specific_event(event)
    
    @abstractmethod
    async def _process_event(
        self, 
        event: CloudEvent, 
        request: Request,
        correlation_id: str
    ) -> Dict[str, Any]:
        """
        Process the specific event type
        
        Args:
            event: CloudEvent to process
            request: FastAPI request object
            correlation_id: Correlation ID for tracking
            
        Returns:
            Dict containing processing results
        """
        pass
    
    async def _validate_specific_event(self, event: CloudEvent) -> None:
        """
        Validate event-specific data (override in subclasses)
        
        Args:
            event: CloudEvent to validate
        """
        pass
    
    async def _cleanup_on_error(
        self, 
        event: CloudEvent, 
        request: Request,
        correlation_id: str
    ) -> None:
        """
        Cleanup resources on error (override in subclasses)
        
        Args:
            event: CloudEvent that failed
            request: FastAPI request object
            correlation_id: Correlation ID for tracking
        """
        pass
    
    def get_performance_metrics(self) -> Dict[str, float]:
        """
        Get performance metrics for this handler
        
        Returns:
            Dict of event types to processing times
        """
        return self._performance_metrics.copy()
    
    async def health_check(self) -> Dict[str, Any]:
        """
        Health check for this event handler
        
        Returns:
            Dict containing health status
        """
        return {
            "handler": self.name,
            "status": "healthy",
            "metrics": self.get_performance_metrics(),
        }
