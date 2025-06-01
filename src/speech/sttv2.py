"""
Azure Speech-to-Text v2 - Improved Implementation

Concise, maintainable implementation of Azure Speech Services following best practices.
Preserves core functionality from v1 while improving security, performance, and maintainability.

Key improvements:
- Managed Identity authentication (no hardcoded API keys)
- Async/await patterns for better performance
- Simplified error handling with retry logic
- Connection pooling and resource management
- Structured logging
- Clean separation of concerns
"""

import asyncio
import logging
import contextlib
import tempfile
import time
import uuid
import datetime
from datetime import timedelta
from pathlib import Path
from typing import List, Optional, Union, AsyncGenerator, Dict, Any
from enum import Enum
from enum import Enum
from urllib.parse import urlparse
from pydantic import BaseModel, Field, field_validator

import azure.cognitiveservices.speech as speechsdk
import numpy as np
from azure.core.exceptions import ClientAuthenticationError, HttpResponseError, ServiceRequestError
from azure.core.credentials import TokenCredential
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.storage.blob.aio import BlobServiceClient
from utils.ml_logging import get_logger
from azure.core.credentials import AccessToken

logger = get_logger()
class AudioFormat(str, Enum):
    """Supported audio formats for transcription."""
    WAV = "wav"
    MP3 = "mp3"
    FLAC = "flac"
    AAC = "aac"
    OGG = "ogg"

class LanguageCode(str, Enum):
    """Supported language codes for speech recognition."""
    """Supported language codes for speech recognition."""
    
    EN_US = "en-US"
    EN_GB = "en-GB"
    ES_ES = "es-ES"
    ES_MX = "es-MX"
    FR_FR = "fr-FR"
    DE_DE = "de-DE"
    IT_IT = "it-IT"
    PT_BR = "pt-BR"
    ZH_CN = "zh-CN"
    JA_JP = "ja-JP"
    KO_KR = "ko-KR"


class TranscriptionRequest(BaseModel):
    """Request model for transcription operations."""
    
    audio_source: Union[str, Path]
    language: Optional[LanguageCode] = None
    auto_detect_language: bool = False
    enable_diarization: bool = False
    enable_profanity_filter: bool = True
    enable_punctuation: bool = True
    timeout_seconds: int = Field(default=300, ge=1, le=3600)
    correlation_id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    
    @field_validator('audio_source')
    def validate_audio_source(cls, v):
        """Validate audio source is either a valid file path or blob URL."""
        if isinstance(v, str):
            if v.startswith(('http://', 'https://')):
                parsed = urlparse(v)
                if 'blob.core.windows.net' not in parsed.netloc:
                    raise ValueError("Invalid Azure blob URL format")
            else:
                path = Path(v)
                if not path.exists():
                    raise ValueError(f"Audio file does not exist: {v}")
        return v


class TranscriptionResult(BaseModel):
    """Result model for transcription operations."""
    model_config = {"arbitrary_types_allowed": True}
    
    text: str
    confidence: float = Field(ge=0.0, le=1.0)
    language_detected: Optional[str] = None
    duration_ms: int
    speakers: Optional[List[Dict[str, Any]]] = None
    correlation_id: str
    timestamp: datetime.datetime = Field(default_factory=datetime.datetime.now)
    error_details: Optional[str] = None


class CircuitBreakerState(str, Enum):
    """Circuit breaker states for resilience patterns."""
    
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """
    Circuit breaker implementation for Azure Speech Services.
    
    Implements the circuit breaker pattern to handle transient failures
    and prevent cascading failures in distributed systems.
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        timeout_seconds: int = 60,
        expected_exception: tuple = (ServiceRequestError, HttpResponseError)
    ):
        self.failure_threshold = failure_threshold
        self.timeout_seconds = timeout_seconds
        self.expected_exception = expected_exception
        self.failure_count = 0
        self.last_failure_time: Optional[datetime.datetime] = None
        self.state = CircuitBreakerState.CLOSED
        
    def can_execute(self) -> bool:
        """Check if the circuit breaker allows execution."""
        if self.state == CircuitBreakerState.CLOSED:
            return True
        elif self.state == CircuitBreakerState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitBreakerState.HALF_OPEN
                return True
            return False
        else:  # HALF_OPEN
            return True
    
    def on_success(self):
        """Handle successful operation."""
        self.failure_count = 0
        self.state = CircuitBreakerState.CLOSED
    
    def on_failure(self, exception: Exception):
        """Handle failed operation."""
        if isinstance(exception, self.expected_exception):
            self.failure_count += 1
            self.last_failure_time = datetime.datetime.now().isoformat()
            
            if self.failure_count >= self.failure_threshold:
                self.state = CircuitBreakerState.OPEN
    
    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self.last_failure_time is None:
            return True
        return datetime.datetime.now().isoformat() - self.last_failure_time > timedelta(seconds=self.timeout_seconds)


class AzureSpeechToTextV2:
    """
    Enterprise-grade Azure Speech-to-Text client with best practices.
    
    Features:
    - Managed Identity authentication
    - Async/await patterns for performance
    - Circuit breaker for resilience
    - Connection pooling and resource management
    - Comprehensive error handling
    - Structured logging with correlation IDs
    - Performance telemetry
    """
    
    def __init__(
        self,
        speech_region: str,
        credential: Optional[TokenCredential] = None,
        storage_account_name: Optional[str] = None,
        enable_logging: bool = True,
        log_level: str = "INFO"
    ):
        """
        Initialize Azure Speech-to-Text client with enterprise configurations.
        
        Args:
            speech_region: Azure region for Speech Services
            credential: Azure credential (defaults to DefaultAzidfcxureCredential)
            storage_account_name: Storage account for blob operations
            enable_logging: Enable structured logging
            log_level: Logging level (DEBUG, INFO, WARNING, ERROR)
        """
        self.speech_region = speech_region
        self.credential = credential or DefaultAzureCredential()
        self.storage_account_name = storage_account_name
        
        # Configure structured logging
        self.logger = self._setup_logging(enable_logging, log_level)
        
        # Acquire an auth token for Speech Services using the provided credential
        try:
            # Speech resource scope for Azure AD
            speech_scope = "https://cognitiveservices.azure.com/.default"

            # Get token from credential (DefaultAzureCredential or ManagedIdentityCredential)
            access_token: AccessToken = self.credential.get_token(speech_scope)
            self._speech_auth_token = access_token.token
            self._speech_auth_token_expiry = access_token.expires_on
        except Exception as e:
            self.logger.error(
            "Failed to acquire Speech Services auth token",
            extra={"error": str(e), "correlation_id": "init"}
            )
            raise
        # Initialize speech configuration with Managed Identity
        self.speech_config = self._create_speech_config(self._speech_auth_token)

        # Circuit breaker for resilience
        self.circuit_breaker = CircuitBreaker()
        
        # Performance tracking
        self.metrics = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "average_duration_ms": 0.0,
            "last_error": None
        }
        
        # Blob client for storage operations
        self._blob_client: Optional[BlobServiceClient] = None
        
        self.logger.info(
            "AzureSpeechToTextV2 initialized",
            extra={"speech_region": speech_region, "has_storage": bool(storage_account_name)}
        )
    
    def _setup_logging(self, enable_logging: bool, log_level: str) -> logging.Logger:
        """Setup structured logging with correlation IDs."""
        logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
        if enable_logging:
            logger.setLevel(getattr(logging, log_level.upper()))
            
            # Create formatter for structured logging
            class CorrelationIdFormatter(logging.Formatter):
                def format(self, record):
                    if not hasattr(record, "correlation_id") or record.correlation_id is None:
                        record.correlation_id = "N/A"
                    return super().format(record)
            formatter = CorrelationIdFormatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s - '
                'correlation_id=%(correlation_id)s'
            )
            
            # Only add handler if none exists
            if not logger.handlers:
                handler = logging.StreamHandler()
                handler.setFormatter(formatter)
                logger.addHandler(handler)
        
        return logger
    
    def _create_speech_config(self, auth_token) -> speechsdk.SpeechConfig:
        """Create speech configuration with Managed Identity authentication."""
        try:
            # Use Managed Identity for authentication (no API keys)
            speech_config = speechsdk.SpeechConfig(
                auth_token=auth_token,
                region=self.speech_region
            )
            
            # Configure advanced settings for enterprise use
            speech_config.set_property(
                speechsdk.PropertyId.SpeechServiceConnection_EnableAudioLogging, "false"
            )
            speech_config.set_property(
                speechsdk.PropertyId.Speech_SegmentationSilenceTimeoutMs, "2000"
            )
            speech_config.set_property(
                speechsdk.PropertyId.SpeechServiceResponse_DiarizeIntermediateResults, "true"
            )
            
            # Enable detailed error information
            speech_config.set_property(
                speechsdk.PropertyId.CancellationDetails_ReasonText, "true"
            )
            
            return speech_config
            
        except Exception as e:
            self.logger.error(
                "Failed to create speech configuration",
                extra={"error": str(e), "correlation_id": "init"}
            )
            raise
    
    async def get_blob_client(self) -> BlobServiceClient:
        """Get or create blob service client with connection pooling."""
        if self._blob_client is None:
            if not self.storage_account_name:
                raise ValueError("Storage account name required for blob operations")
            
            account_url = f"https://{self.storage_account_name}.blob.core.windows.net"
            self._blob_client = BlobServiceClient(
                account_url=account_url,
                credential=self.credential,
                max_pool_connections=20,  # Connection pooling
                retry_total=3
            )
            
            self.logger.info(
                "Blob service client created",
                extra={"storage_account": self.storage_account_name, "correlation_id": "blob_init"}
            )
        
        return self._blob_client
    
    async def transcribe_audio(self, request: TranscriptionRequest) -> TranscriptionResult:
        """
        Transcribe audio with comprehensive error handling and monitoring.
        
        Args:
            request: Transcription request with audio source and configuration
            
        Returns:
            TranscriptionResult with transcribed text and metadata
            
        Raises:
            ServiceRequestError: When Speech Services are unavailable
            ClientAuthenticationError: When authentication fails
            ValueError: When request parameters are invalid
        """
        start_time = time.time()
        correlation_id = request.correlation_id
        
        # Update metrics
        self.metrics["total_requests"] += 1
        
        self.logger.info(
            "Starting transcription",
            extra={
                "correlation_id": correlation_id,
                "audio_source": str(request.audio_source),
                "language": request.language,
                "auto_detect": request.auto_detect_language
            }
        )
        
        try:
            # Check circuit breaker
            if not self.circuit_breaker.can_execute():
                raise ServiceRequestError("Circuit breaker is OPEN - service temporarily unavailable")
            
            # Determine audio source type and transcribe
            if isinstance(request.audio_source, str) and request.audio_source.startswith(('http://', 'https://')):
                result = await self._transcribe_from_blob(request)
            else:
                result = await self._transcribe_from_file(request)
            
            # Calculate duration and update metrics
            duration_ms = int((time.time() - start_time) * 1000)
            result.duration_ms = duration_ms
            result.correlation_id = correlation_id
            
            self.metrics["successful_requests"] += 1
            self._update_average_duration(duration_ms)
            
            # Circuit breaker success
            self.circuit_breaker.on_success()
            
            self.logger.info(
                "Transcription completed successfully",
                extra={
                    "correlation_id": correlation_id,
                    "duration_ms": duration_ms,
                    "text_length": len(result.text),
                    "confidence": result.confidence
                }
            )
            
            return result
            
        except Exception as e:
            # Update metrics
            self.metrics["failed_requests"] += 1
            self.metrics["last_error"] = str(e)
            
            # Circuit breaker failure
            self.circuit_breaker.on_failure(e)
            
            duration_ms = int((time.time() - start_time) * 1000)
            
            self.logger.error(
                "Transcription failed",
                extra={
                    "correlation_id": correlation_id,
                    "error": str(e),
                    "duration_ms": duration_ms,
                    "error_type": type(e).__name__
                }
            )
            
            # Return error result instead of raising
            return TranscriptionResult(
                text="",
                confidence=0.0,
                duration_ms=duration_ms,
                correlation_id=correlation_id,
                error_details=str(e)
            )
    
    async def _transcribe_from_file(self, request: TranscriptionRequest) -> TranscriptionResult:
        """Transcribe audio from local file with async patterns."""
        file_path = Path(request.audio_source)
        
        if not file_path.exists():
            raise ValueError(f"Audio file not found: {file_path}")
        
        # Validate file format
        if file_path.suffix.lower().lstrip('.') not in [fmt.value for fmt in AudioFormat]:
            self.logger.warning(
                "Unsupported audio format",
                extra={"correlation_id": request.correlation_id, "format": file_path.suffix}
            )
        
        # Create audio configuration
        audio_config = speechsdk.AudioConfig(filename=str(file_path.absolute()))
        
        return await self._perform_transcription(request, audio_config)
    
    async def _transcribe_from_blob(self, request: TranscriptionRequest) -> TranscriptionResult:
        """Transcribe audio from Azure blob with secure access."""
        blob_url = request.audio_source
        parsed_url = urlparse(blob_url)
        
        # Extract container and blob name
        path_parts = parsed_url.path.strip('/').split('/')
        if len(path_parts) < 2:
            raise ValueError("Invalid blob URL format")
        
        container_name = path_parts[0]
        blob_name = '/'.join(path_parts[1:])
        
        self.logger.info(
            "Downloading blob for transcription",
            extra={
                "correlation_id": request.correlation_id,
                "container": container_name,
                "blob": blob_name
            }
        )
        
        # Download blob to temporary file
        blob_client = await self.get_blob_client()
        container_client = blob_client.get_container_client(container_name)
        blob_client_instance = container_client.get_blob_client(blob_name)
        
        try:
            # Use async context manager for secure temp file handling
            async with self._create_temp_file() as temp_path:
                # Download blob data
                async with blob_client_instance:
                    download_stream = await blob_client_instance.download_blob()
                    blob_data = await download_stream.readall()
                
                # Write to temp file
                with open(temp_path, 'wb') as temp_file:
                    temp_file.write(blob_data)
                
                # Create audio configuration from temp file
                audio_config = speechsdk.AudioConfig(filename=str(temp_path))
                
                return await self._perform_transcription(request, audio_config)
                
        except Exception as e:
            self.logger.error(
                "Failed to download blob",
                extra={
                    "correlation_id": request.correlation_id,
                    "container": container_name,
                    "blob": blob_name,
                    "error": str(e)
                }
            )
            raise
    
    @contextlib.asynccontextmanager
    async def _create_temp_file(self) -> AsyncGenerator[Path, None]:
        """Create and cleanup temporary file securely."""
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        temp_path = Path(temp_file.name)
        temp_file.close()
        
        try:
            yield temp_path
        finally:
            # Secure cleanup
            try:
                temp_path.unlink(missing_ok=True)
            except Exception as e:
                self.logger.warning(
                    "Failed to cleanup temp file",
                    extra={"temp_path": str(temp_path), "error": str(e)}
                )
    
    async def _perform_transcription(
        self, 
        request: TranscriptionRequest, 
        audio_config: speechsdk.AudioConfig
    ) -> TranscriptionResult:
        """Perform the actual transcription with comprehensive error handling."""
        
        # Configure language settings
        language = request.language.value if request.language else None
        
        if request.auto_detect_language:
            # Configure auto-detection
            supported_languages = [lang.value for lang in LanguageCode]
            auto_detect_config = speechsdk.languageconfig.AutoDetectSourceLanguageConfig(
                languages=supported_languages
            )
            language_config = auto_detect_config
            language = None
        else:
            auto_detect_config = None
            language_config = None
        
        # Create recognizer based on diarization requirement
        if request.enable_diarization:
            recognizer = speechsdk.transcription.ConversationTranscriber(
                speech_config=self.speech_config,
                audio_config=audio_config,
                auto_detect_source_language_config=auto_detect_config
            )
        else:
            recognizer = speechsdk.SpeechRecognizer(
                speech_config=self.speech_config,
                audio_config=audio_config,
                language=language,
                auto_detect_source_language_config=auto_detect_config
            )
        
        # Configure additional properties
        if request.enable_profanity_filter:
            recognizer.properties.set_property(
                speechsdk.PropertyId.SpeechServiceResponse_ProfanityOption, "Masked"
            )
        
        if request.enable_punctuation:
            recognizer.properties.set_property(
                speechsdk.PropertyId.SpeechServiceResponse_RequestDetailedResultTrueFalse, "true"
            )
        
        # Perform transcription with timeout
        return await asyncio.wait_for(
            self._execute_recognition(recognizer, request),
            timeout=request.timeout_seconds
        )
    
    async def _execute_recognition(
        self, 
        recognizer: Union[speechsdk.SpeechRecognizer, speechsdk.transcription.ConversationTranscriber],
        request: TranscriptionRequest
    ) -> TranscriptionResult:
        """Execute speech recognition with async patterns."""
        
        final_text = ""
        speakers = [] if request.enable_diarization else None
        confidence_scores = []
        detected_language = None
        
        # Event handling with async patterns
        recognition_done = asyncio.Event()
        error_occurred = None
        
        def transcribed_cb(evt: speechsdk.SpeechRecognitionEventArgs):
            nonlocal final_text, speakers, confidence_scores, detected_language
            
            if evt.result.reason == speechsdk.ResultReason.RecognizedSpeech:
                if request.enable_diarization and hasattr(evt.result, 'speaker_id'):
                    speaker_info = {
                        "speaker_id": evt.result.speaker_id,
                        "text": evt.result.text,
                        "confidence": getattr(evt.result, 'confidence', 0.0)
                    }
                    speakers.append(speaker_info)
                    final_text += f"Speaker {evt.result.speaker_id}: {evt.result.text}\n"
                else:
                    final_text += " " + evt.result.text
                
                # Extract confidence and language info
                if hasattr(evt.result, 'confidence'):
                    confidence_scores.append(evt.result.confidence)
                
                if hasattr(evt.result, 'language') and evt.result.language:
                    detected_language = evt.result.language
        
        def session_stopped_cb(evt: speechsdk.SessionEventArgs):
            recognition_done.set()
        
        def canceled_cb(evt: speechsdk.SpeechRecognitionCanceledEventArgs):
            nonlocal error_occurred
            if evt.reason == speechsdk.CancellationReason.Error:
                error_occurred = f"Recognition canceled: {evt.error_details}"
            recognition_done.set()
        
        # Connect event handlers
        if hasattr(recognizer, 'transcribed'):
            recognizer.transcribed.connect(transcribed_cb)
        else:
            recognizer.recognized.connect(transcribed_cb)
        
        recognizer.session_stopped.connect(session_stopped_cb)
        recognizer.canceled.connect(canceled_cb)
        
        # Start recognition
        if hasattr(recognizer, 'start_transcribing_async'):
            recognizer.start_transcribing_async()
        else:
            recognizer.start_continuous_recognition_async()
        
        try:
            # Wait for completion
            await recognition_done.wait()
            
            if error_occurred:
                raise ServiceRequestError(error_occurred)
            
            # Calculate average confidence
            avg_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0.0
            
            return TranscriptionResult(
                text=final_text.strip(),
                confidence=avg_confidence,
                language_detected=detected_language,
                duration_ms=0,  # Will be set by caller
                speakers=speakers,
                correlation_id=request.correlation_id
            )
            
        finally:
            # Cleanup
            if hasattr(recognizer, 'stop_transcribing_async'):
                recognizer.stop_transcribing_async()
            else:
                recognizer.stop_continuous_recognition_async()
    
    def _update_average_duration(self, duration_ms: int):
        """Update average duration metric."""
        total_successful = self.metrics["successful_requests"]
        if total_successful == 1:
            self.metrics["average_duration_ms"] = duration_ms
        else:
            current_avg = self.metrics["average_duration_ms"]
            self.metrics["average_duration_ms"] = (
                (current_avg * (total_successful - 1) + duration_ms) / total_successful
            )
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get service health status and metrics."""
        total_requests = self.metrics["total_requests"]
        success_rate = (
            self.metrics["successful_requests"] / total_requests 
            if total_requests > 0 else 0.0
        )
        
        return {
            "status": "healthy" if success_rate > 0.9 else "degraded" if success_rate > 0.5 else "unhealthy",
            "circuit_breaker_state": self.circuit_breaker.state.value,
            "metrics": {
                **self.metrics,
                "success_rate": success_rate
            },
            "timestamp": datetime.datetime.now().isoformat().isoformat()
        }
    
    async def close(self):
        """Cleanup resources and connections."""
        if self._blob_client:
            await self._blob_client.close()
            self._blob_client = None
        
        self.logger.info("AzureSpeechToTextV2 resources cleaned up")
    
    def create_realtime_recognizer(
        self, 
        audio_config: speechsdk.AudioConfig = None,
        language: str = None,
        enable_interim_results: bool = True,
        correlation_id: str = None
    ) -> speechsdk.SpeechRecognizer:
        """
        Create a realtime speech recognizer for streaming audio.
        
        Args:
            audio_config: AudioConfig for the recognizer. If None, uses default microphone.
            language: Language for recognition. If None, uses configured language.
            enable_interim_results: Whether to enable interim results during recognition.
            correlation_id: Unique identifier for tracking this recognition session.
            
        Returns:
            Configured SpeechRecognizer instance.
            
        Raises:
            RuntimeError: If circuit breaker is open or configuration fails.
            ValueError: If required configuration is missing.
        """
        if correlation_id is None:
            correlation_id = str(uuid.uuid4())
        
        try:
            # Check circuit breaker
            if not self.circuit_breaker.can_execute():
                error_msg = f"Speech service unavailable - circuit breaker open"
                self.logger.warning(
                    error_msg,
                    extra={"correlation_id": correlation_id}
                )
                raise RuntimeError(error_msg)
            
            # Create speech configuration
            speech_config = self._create_speech_config(self._speech_auth_token)
            
            # Set language if provided
            if language:
                speech_config.speech_recognition_language = language
            
            # Configure for realtime recognition
            if enable_interim_results:
                speech_config.set_property(
                    speechsdk.PropertyId.SpeechServiceResponse_RequestDetailedResultTrueFalse, 
                    "true"
                )
                # Removed invalid property SpeechServiceResponse_RequestConfidenceTrue
                # No replacement needed as confidence handling is already managed elsewhere
            
            # Configure continuous recognition settings
            speech_config.set_property(
                speechsdk.PropertyId.Speech_SegmentationSilenceTimeoutMs, 
                "5000"  # 5 seconds for realtime
            )
            speech_config.set_property(
                speechsdk.PropertyId.SpeechServiceConnection_InitialSilenceTimeoutMs,
                "10000"  # 10 seconds initial timeout
            )
            
            # Use provided audio config or default microphone
            if audio_config is None:
                audio_config = speechsdk.AudioConfig(use_default_microphone=True)
            
            # Create recognizer
            recognizer = speechsdk.SpeechRecognizer(
                speech_config=speech_config,
                audio_config=audio_config
            )
            
            # Add event handlers for comprehensive logging
            self._setup_recognizer_event_handlers(recognizer, correlation_id)
            
            self.logger.info(
                "Realtime speech recognizer created successfully",
                extra={
                    "correlation_id": correlation_id,
                    "language": speech_config.speech_recognition_language,
                    "interim_results": enable_interim_results
                }
            )
            
            return recognizer
            
        except Exception as e:
            self.circuit_breaker.on_failure(e)
            
            self.logger.error(
                "Failed to create realtime recognizer",
                extra={
                    "correlation_id": correlation_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            raise RuntimeError(f"Failed to create realtime recognizer: {str(e)}") from e
    
    def _setup_recognizer_event_handlers(
        self, 
        recognizer: speechsdk.SpeechRecognizer, 
        correlation_id: str
    ) -> None:
        """Setup comprehensive event handlers for the recognizer."""
        
        def on_recognizing(evt: speechsdk.SpeechRecognitionEventArgs):
            """Handle interim recognition results."""
            self.logger.debug(
                "Interim recognition result",
                extra={
                    "correlation_id": correlation_id,
                    "text": evt.result.text,
                    "reason": evt.result.reason.name
                }
            )
        
        def on_recognized(evt: speechsdk.SpeechRecognitionEventArgs):
            """Handle final recognition results."""
            if evt.result.reason == speechsdk.ResultReason.RecognizedSpeech:
                self.logger.info(
                    "Speech recognized",
                    extra={
                        "correlation_id": correlation_id,
                        "text": evt.result.text,
                        "confidence": getattr(evt.result, 'confidence', 'N/A')
                    }
                )
                # Update metrics
                self.metrics["successful_requests"] += 1
                self.circuit_breaker.on_success()
                
            elif evt.result.reason == speechsdk.ResultReason.NoMatch:
                self.logger.debug(
                    "No speech recognized",
                    extra={"correlation_id": correlation_id}
                )
        
        def on_canceled(evt: speechsdk.SpeechRecognitionCanceledEventArgs):
            """Handle recognition cancellation."""
            if evt.reason == speechsdk.CancellationReason.Error:
                error_msg = f"Recognition error: {evt.error_details}"
                self.logger.error(
                    error_msg,
                    extra={
                        "correlation_id": correlation_id,
                        "error_code": evt.error_code.name if evt.error_code else "Unknown"
                    }
                )
                # Update metrics
                self.metrics["failed_requests"] += 1
                self.circuit_breaker.on_failure(Exception(error_msg))
            else:
                self.logger.info(
                    "Recognition canceled",
                    extra={
                        "correlation_id": correlation_id,
                        "reason": evt.reason.name
                    }
                )
        
        def on_session_started(evt: speechsdk.SessionEventArgs):
            """Handle session start."""
            self.logger.info(
                "Recognition session started",
                extra={"correlation_id": correlation_id, "session_id": evt.session_id}
            )
        
        def on_session_stopped(evt: speechsdk.SessionEventArgs):
            """Handle session stop."""
            self.logger.info(
                "Recognition session stopped",
                extra={"correlation_id": correlation_id, "session_id": evt.session_id}
            )
        
        # Connect event handlers
        recognizer.recognizing.connect(on_recognizing)
        recognizer.recognized.connect(on_recognized)
        recognizer.canceled.connect(on_canceled)
        recognizer.session_started.connect(on_session_started)
        recognizer.session_stopped.connect(on_session_stopped)


# Convenience functions for backward compatibility and ease of use

async def transcribe_file(
    file_path: Union[str, Path],
    speech_region: str,
    language: Optional[LanguageCode] = None,
    enable_diarization: bool = False,
    credential: Optional[TokenCredential] = None
) -> TranscriptionResult:
    """
    Convenience function to transcribe a local audio file.
    
    Args:
        file_path: Path to audio file
        speech_region: Azure region for Speech Services
        language: Language for recognition (auto-detect if None)
        enable_diarization: Enable speaker diarization
        credential: Azure credential (defaults to DefaultAzureCredential)
    
    Returns:
        TranscriptionResult with transcribed text and metadata
    """
    client = AzureSpeechToTextV2(
        speech_region=speech_region,
        credential=credential
    )
    
    try:
        request = TranscriptionRequest(
            audio_source=file_path,
            language=language,
            auto_detect_language=language is None,
            enable_diarization=enable_diarization
        )
        
        return await client.transcribe_audio(request)
    finally:
        await client.close()


async def transcribe_blob(
    blob_url: str,
    speech_region: str,
    storage_account_name: str,
    language: Optional[LanguageCode] = None,
    enable_diarization: bool = False,
    credential: Optional[TokenCredential] = None
) -> TranscriptionResult:
    """
    Convenience function to transcribe audio from Azure blob storage.
    
    Args:
        blob_url: URL to audio blob
        speech_region: Azure region for Speech Services
        storage_account_name: Azure Storage account name
        language: Language for recognition (auto-detect if None)
        enable_diarization: Enable speaker diarization
        credential: Azure credential (defaults to DefaultAzureCredential)
    
    Returns:
        TranscriptionResult with transcribed text and metadata
    """
    client = AzureSpeechToTextV2(
        speech_region=speech_region,
        credential=credential,
        storage_account_name=storage_account_name
    )
    
    try:
        request = TranscriptionRequest(
            audio_source=blob_url,
            language=language,
            auto_detect_language=language is None,
            enable_diarization=enable_diarization
        )
        
        return await client.transcribe_audio(request)
    finally:
        await client.close()


# Example usage and testing
if __name__ == "__main__":
    import asyncio

    
    async def example_usage():
        """Example usage of the improved Azure STT client."""
        
        # Initialize client with Managed Identity
        client = AzureSpeechToTextV2(
            speech_region="eastus",  # Your Azure region
            enable_logging=True,
            log_level="INFO"
        )
        
        try:
            # Example 1: Transcribe local file with auto language detection
            request = TranscriptionRequest(
                audio_source="path/to/audio.wav",
                auto_detect_language=True,
                enable_diarization=True,
                enable_punctuation=True
            )
            
            result = await client.transcribe_audio(request)
            print(f"Transcription: {result.text}")
            print(f"Confidence: {result.confidence}")
            print(f"Language: {result.language_detected}")
            
            # Example 2: Health monitoring
            health = client.get_health_status()
            print(f"Service Health: {health}")
            
        except Exception as e:
            print(f"Error: {e}")
        finally:
            await client.close()
    
    # Run example
    asyncio.run(example_usage())
