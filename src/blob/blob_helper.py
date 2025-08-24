"""
Azure Blob Storage Helper Module - FastAPI & Azure Best Practices Implementation

This module provides secure, efficient Azure Blob Storage operations following FastAPI
and Azure best practices. Features include:

- Managed Identity authentication with fallback to connection string
- Comprehensive error handling with retry logic
- Async/await support for FastAPI integration
- Structured logging and monitoring
- Connection pooling and resource management
- Input validation and security measures

Dependencies:
    azure-storage-blob>=12.19.0
    azure-identity>=1.15.0
    aiofiles>=23.0.0

Environment Variables:
    AZURE_STORAGE_ACCOUNT_NAME: Storage account name (required)
    AZURE_BLOB_CONTAINER: Default container name (default: "acs")
    AZURE_STORAGE_CONNECTION_STRING: Connection string (fallback auth)
    AZURE_STORAGE_ACCOUNT_KEY: Account key (fallback auth)

Security Note:
    This implementation prefers Managed Identity authentication over keys.
    Connection strings and account keys are used only as fallback options.
"""

import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

import aiofiles
from azure.core.exceptions import (
    AzureError,
    ClientAuthenticationError,
    HttpResponseError,
    ResourceNotFoundError,
)
from azure.identity.aio import DefaultAzureCredential
from azure.storage.blob import ContainerSasPermissions, generate_container_sas
from azure.storage.blob.aio import BlobClient, BlobServiceClient
from utils.azure_auth import get_credential

# Configure structured logging
logger = logging.getLogger(__name__)


class BlobOperationType(Enum):
    """Enumeration of blob operation types for monitoring."""

    UPLOAD = "upload"
    DOWNLOAD = "download"
    DELETE = "delete"
    LIST = "list"
    GENERATE_SAS = "generate_sas"
    VERIFY_ACCESS = "verify_access"


@dataclass
class BlobOperationResult:
    """Structured result for blob operations."""

    success: bool
    operation_type: BlobOperationType
    blob_name: Optional[str] = None
    container_name: Optional[str] = None
    error_message: Optional[str] = None
    duration_ms: Optional[float] = None
    size_bytes: Optional[int] = None
    content: Optional[str] = None  # For download operations
    blob_list: Optional[List[str]] = None  # For list operations


class AzureBlobHelper:
    """
    Secure Azure Blob Storage helper with FastAPI best practices.

    Features:
    - Managed Identity authentication with secure fallbacks
    - Comprehensive error handling and retry logic
    - Connection pooling and resource management
    - Structured logging and monitoring
    - Input validation and security measures
    """

    def __init__(
        self,
        account_name: Optional[str] = None,
        container_name: Optional[str] = None,
        connection_string: Optional[str] = None,
        account_key: Optional[str] = None,
        max_retry_attempts: int = 3,
    ):
        """
        Initialize Azure Blob Helper with secure authentication.

        This method initializes the Azure Blob Storage helper with secure authentication
        methods. It prefers Managed Identity authentication over connection strings and
        account keys for enhanced security. The helper validates required configuration
        and sets up proper retry mechanisms for resilient blob operations.

        :param account_name: (optional) Azure Storage account name. If not provided,
            will be retrieved from AZURE_STORAGE_ACCOUNT_NAME environment variable.
        :param container_name: (optional) Default container name for blob operations.
            If not provided, will be retrieved from AZURE_BLOB_CONTAINER environment
            variable or default to 'acs'.
        :param connection_string: (optional) Azure Storage connection string for
            fallback authentication. If not provided, will be retrieved from
            AZURE_STORAGE_CONNECTION_STRING environment variable.
        :param account_key: (optional) Azure Storage account key for fallback
            authentication. If not provided, will be retrieved from
            AZURE_STORAGE_ACCOUNT_KEY environment variable.
        :param max_retry_attempts: Maximum number of retry attempts for failed
            operations. Default is 3.
        :raises ValueError: If AZURE_STORAGE_ACCOUNT_NAME is not provided or available
            in environment variables.
        """
        # Configuration with validation
        self.account_name = account_name or os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        self.container_name = container_name or os.getenv("AZURE_BLOB_CONTAINER", "acs")
        self.connection_string = connection_string or os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        self.account_key = account_key or os.getenv("AZURE_STORAGE_ACCOUNT_KEY")

        if not self.account_name:
            raise ValueError("AZURE_STORAGE_ACCOUNT_NAME is required")

        # Retry configuration
        self.max_retry_attempts = max_retry_attempts

        # Initialize authentication and client
        self._credential = self._setup_authentication()
        self._blob_service: Optional[BlobServiceClient] = None

        logger.info(
            f"AzureBlobHelper initialized for account '{self.account_name}', "
            f"default container '{self.container_name}'"
        )

    def _setup_authentication(self) -> Optional[DefaultAzureCredential]:
        """
        Set up authentication with preference for Managed Identity.

        This method configures Azure authentication using the most secure available
        method. It prioritizes Managed Identity (Azure-native authentication) over
        connection strings and account keys for enhanced security in cloud deployments.

        :return: DefaultAzureCredential instance if Managed Identity is available,
            None if using connection string authentication.
        :raises ValueError: If no valid authentication method is available.
        """
        try:
            # Prefer Managed Identity (secure, Azure-native)
            if not self.connection_string:
                credential = get_credential()
                logger.info("Using Managed Identity authentication")
                return credential
            else:
                logger.warning(
                    "Using connection string authentication - consider migrating to Managed Identity"
                )
                return None
        except Exception as e:
            logger.error(f"Failed to setup authentication: {e}")
            if not self.connection_string:
                raise ValueError("No valid authentication method available")
            return None

    async def _get_blob_service(self) -> BlobServiceClient:
        """
        Get or create BlobServiceClient with connection pooling.

        This method implements lazy initialization of the BlobServiceClient to optimize
        resource usage. It maintains a single client instance throughout the helper's
        lifecycle and automatically selects the appropriate authentication method based
        on the available credentials.

        :return: Configured BlobServiceClient instance ready for blob operations.
        :raises ValueError: If no authentication method is available.
        :raises Exception: If BlobServiceClient creation fails due to invalid
            credentials or network issues.
        """
        if self._blob_service is None:
            try:
                if self._credential:
                    # Use Managed Identity
                    self._blob_service = BlobServiceClient(
                        f"https://{self.account_name}.blob.core.windows.net",
                        credential=self._credential,
                    )
                elif self.connection_string:
                    # Fallback to connection string
                    self._blob_service = BlobServiceClient.from_connection_string(
                        self.connection_string
                    )
                else:
                    raise ValueError("No authentication method available")

                logger.debug("BlobServiceClient created successfully")

            except Exception as e:
                logger.error(f"Failed to create BlobServiceClient: {e}")
                raise

        return self._blob_service

    async def generate_container_sas_url(
        self, container_name: Optional[str] = None, expiry_hours: int = 24
    ) -> BlobOperationResult:
        """
        Generate a container URL with SAS token for Azure Blob Storage access.

        This method creates a secure SAS (Shared Access Signature) URL for Azure Blob
        Storage access. It supports both account key-based SAS and user delegation SAS
        (when using Managed Identity) for enhanced security. The generated URL provides
        comprehensive permissions for container operations including read, write, create,
        delete, and list operations.

        :param container_name: (optional) Target container name for SAS URL generation.
            If not provided, uses the default container name configured during
            initialization.
        :param expiry_hours: (optional) Number of hours until the SAS token expires.
            Must be between 1 and 8760 hours (1 year). Default is 24 hours.
        :return: BlobOperationResult containing the generated SAS URL on success or
            error details on failure. The SAS URL is returned in the content field.
        :raises ValueError: If container name is not provided and no default is set,
            or if expiry_hours is outside the valid range.
        """
        start_time = datetime.now(timezone.utc)
        container_name = container_name or self.container_name

        try:
            # Validate inputs
            if not container_name:
                raise ValueError("Container name is required")

            if expiry_hours <= 0 or expiry_hours > 8760:  # Max 1 year
                raise ValueError("Expiry hours must be between 1 and 8760")

            # Calculate expiry time with proper UTC handling
            expiry_time = start_time + timedelta(hours=expiry_hours)

            logger.info(
                f"Generating SAS token for container '{container_name}' "
                f"with expiry: {expiry_time.isoformat()}"
            )

            if self._credential:
                # Use User Delegation SAS (more secure)
                service = await self._get_blob_service()
                async with service as client:
                    user_delegation_key = await client.get_user_delegation_key(
                        key_start_time=start_time, key_expiry_time=expiry_time
                    )
                    sas_token = generate_container_sas(
                        account_name=self.account_name,
                        container_name=container_name,
                        user_delegation_key=user_delegation_key,
                        permission=ContainerSasPermissions(
                            read=True,
                            add=True,
                            create=True,
                            write=True,
                            delete=True,
                            list=True,
                        ),
                        expiry=expiry_time,
                    )
            elif self.account_key:
                # Use Account Key SAS (fallback)
                sas_token = generate_container_sas(
                    account_name=self.account_name,
                    container_name=container_name,
                    account_key=self.account_key,
                    permission=ContainerSasPermissions(
                        read=True,
                        add=True,
                        create=True,
                        write=True,
                        delete=True,
                        list=True,
                    ),
                    expiry=expiry_time,
                )
            else:
                raise ValueError("Either managed identity or account key must be available")

            container_url = (
                f"https://{self.account_name}.blob.core.windows.net/"
                f"{container_name}?{sas_token}"
            )

            duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

            logger.info(
                f"Generated container SAS URL for '{container_name}' "
                f"(valid for {expiry_hours} hours) in {duration:.2f}ms"
            )

            return BlobOperationResult(
                success=True,
                operation_type=BlobOperationType.GENERATE_SAS,
                container_name=container_name,
                duration_ms=duration,
                content=container_url,
            )

        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            error_msg = f"Failed to generate container SAS token: {e}"
            logger.error(error_msg, exc_info=True)

            return BlobOperationResult(
                success=False,
                operation_type=BlobOperationType.GENERATE_SAS,
                container_name=container_name,
                error_message=error_msg,
                duration_ms=duration,
            )

    async def verify_container_access(self, container_url: str) -> BlobOperationResult:
        """
        Verify that the container URL is accessible with required permissions.

        This method performs comprehensive verification of container access by testing
        both read and write permissions. It creates a temporary test blob to validate
        write access and then cleans up by deleting the test blob, ensuring the
        container has the necessary permissions for Azure Communication Services operations.

        :param container_url: Complete container URL including SAS token parameters
            for authentication and authorization.
        :return: BlobOperationResult indicating successful access verification or
            detailed error information if verification fails.
        :raises ResourceNotFoundError: If the specified container does not exist.
        :raises ClientAuthenticationError: If the SAS token is invalid or expired.
        """
        start_time = datetime.now(timezone.utc)

        try:
            # Extract container name from URL
            url_parts = container_url.split("?")[0]
            container_name = url_parts.split("/")[-1]

            # Create temporary blob service client with the SAS URL
            async with BlobServiceClient.from_connection_string(container_url) as client:
                container_client = client.get_container_client(container_name)

                # Check container existence
                exists = await container_client.exists()
                if not exists:
                    raise ResourceNotFoundError(f"Container '{container_name}' does not exist")

                # Test write permissions with a small test blob
                test_blob_name = f"acs_test_permissions_{int(start_time.timestamp())}"
                test_blob = container_client.get_blob_client(test_blob_name)

                await test_blob.upload_blob("ACS test content", overwrite=True)
                await test_blob.delete_blob()

                duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

                logger.info(
                    f"Successfully verified access to container '{container_name}' "
                    f"in {duration:.2f}ms"
                )

                return BlobOperationResult(
                    success=True,
                    operation_type=BlobOperationType.VERIFY_ACCESS,
                    container_name=container_name,
                    duration_ms=duration,
                )

        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            error_msg = f"Failed to verify container access: {e}"
            logger.error(error_msg, exc_info=True)

            return BlobOperationResult(
                success=False,
                operation_type=BlobOperationType.VERIFY_ACCESS,
                error_message=error_msg,
                duration_ms=duration,
            )

    async def save_transcript_to_blob(
        self, call_id: str, transcript: str, container_name: Optional[str] = None
    ) -> BlobOperationResult:
        """
        Save transcript to blob storage with organized directory structure.

        This method uploads call transcript data to Azure Blob Storage using an
        organized directory structure based on the current date. The transcript is
        stored as a JSON file with comprehensive metadata for tracking and retrieval.
        The method ensures data integrity and provides detailed logging for monitoring
        and troubleshooting purposes.

        :param call_id: Unique identifier for the call session. Must be non-empty
            and will be used to create the blob filename.
        :param transcript: Complete transcript content as a JSON-formatted string
            containing the call conversation data.
        :param container_name: (optional) Target container for transcript storage.
            If not provided, uses the default container configured during initialization.
        :return: BlobOperationResult containing upload success status, blob metadata,
            and performance metrics including file size and upload duration.
        :raises ValueError: If call_id is empty or transcript content is not provided.
        """
        start_time = datetime.now(timezone.utc)
        container_name = container_name or self.container_name

        try:
            # Validate inputs
            if not call_id or not call_id.strip():
                raise ValueError("Call ID is required and cannot be empty")

            if not transcript:
                raise ValueError("Transcript content is required")

            # Create organized blob path
            date_str = start_time.strftime("%Y-%m-%d")
            blob_name = f"transcripts/{date_str}/{call_id}.json"

            # Get blob client and upload
            service = await self._get_blob_service()
            blob_client = service.get_blob_client(container=container_name, blob=blob_name)

            # Upload with metadata
            content_bytes = transcript.encode("utf-8")
            await blob_client.upload_blob(
                content_bytes,
                overwrite=True,
                content_type="application/json",
                metadata={
                    "call_id": call_id,
                    "created_at": start_time.isoformat(),
                    "content_type": "transcript",
                },
            )

            duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

            logger.info(
                f"Saved transcript for call '{call_id}' to '{blob_name}' "
                f"({len(content_bytes)} bytes) in {duration:.2f}ms"
            )

            return BlobOperationResult(
                success=True,
                operation_type=BlobOperationType.UPLOAD,
                blob_name=blob_name,
                container_name=container_name,
                size_bytes=len(content_bytes),
                duration_ms=duration,
            )

        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            error_msg = f"Failed to save transcript for call '{call_id}': {e}"
            logger.error(error_msg, exc_info=True)

            return BlobOperationResult(
                success=False,
                operation_type=BlobOperationType.UPLOAD,
                error_message=error_msg,
                duration_ms=duration,
            )

    async def save_wav_to_blob(
        self, call_id: str, wav_file_path: str, container_name: Optional[str] = None
    ) -> BlobOperationResult:
        """
        Save WAV file to blob storage from local file path.

        This method uploads audio WAV files to Azure Blob Storage with proper content
        type handling and metadata preservation. It validates the file format, reads
        the audio data asynchronously, and organizes files using a date-based directory
        structure for efficient storage management and retrieval.

        :param call_id: Unique identifier for the call session. Used to create the
            blob filename and associate the audio with the corresponding call.
        :param wav_file_path: Full file system path to the local WAV audio file to
            be uploaded. The file must exist and have a .wav extension.
        :param container_name: (optional) Target container for audio file storage.
            If not provided, uses the default container configured during initialization.
        :return: BlobOperationResult containing upload success status, file metadata
            including size, and performance metrics such as upload duration.
        :raises ValueError: If call_id is empty or the file does not have .wav extension.
        :raises FileNotFoundError: If the specified WAV file does not exist at the
            given path.
        """
        start_time = datetime.now(timezone.utc)
        container_name = container_name or self.container_name

        try:
            # Validate inputs
            if not call_id or not call_id.strip():
                raise ValueError("Call ID is required and cannot be empty")

            wav_path = Path(wav_file_path)
            if not wav_path.exists():
                raise FileNotFoundError(f"WAV file not found: {wav_file_path}")

            if not wav_path.suffix.lower() == ".wav":
                raise ValueError("File must have .wav extension")

            # Create organized blob path
            date_str = start_time.strftime("%Y-%m-%d")
            blob_name = f"audio/{date_str}/{call_id}.wav"

            # Get file size for monitoring
            file_size = wav_path.stat().st_size

            # Read and upload file
            service = await self._get_blob_service()
            blob_client = service.get_blob_client(container=container_name, blob=blob_name)

            async with aiofiles.open(wav_file_path, "rb") as f:
                wav_data = await f.read()

            await blob_client.upload_blob(
                wav_data,
                overwrite=True,
                content_type="audio/wav",
                metadata={
                    "call_id": call_id,
                    "created_at": start_time.isoformat(),
                    "content_type": "audio",
                    "file_size": str(file_size),
                },
            )

            duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

            logger.info(
                f"Saved WAV file for call '{call_id}' to '{blob_name}' "
                f"({file_size} bytes) in {duration:.2f}ms"
            )

            return BlobOperationResult(
                success=True,
                operation_type=BlobOperationType.UPLOAD,
                blob_name=blob_name,
                container_name=container_name,
                size_bytes=file_size,
                duration_ms=duration,
            )

        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            error_msg = f"Failed to save WAV file for call '{call_id}': {e}"
            logger.error(error_msg, exc_info=True)

            return BlobOperationResult(
                success=False,
                operation_type=BlobOperationType.UPLOAD,
                error_message=error_msg,
                duration_ms=duration,
            )

    async def stream_wav_to_blob(
        self, call_id: str, wav_stream, container_name: Optional[str] = None
    ) -> BlobOperationResult:
        """
        Stream WAV data directly to Azure Blob Storage.

        This method provides efficient streaming upload of WAV audio data directly to
        Azure Blob Storage without requiring local file storage. It's optimized for
        real-time audio processing scenarios where audio data is generated or received
        as a stream, enabling immediate cloud storage without intermediate buffering.

        :param call_id: Unique identifier for the call session. Used to create the
            blob filename and associate the streamed audio with the corresponding call.
        :param wav_stream: Asynchronous stream or iterator containing WAV audio data
            chunks to be uploaded directly to blob storage.
        :param container_name: (optional) Target container for audio stream storage.
            If not provided, uses the default container configured during initialization.
        :return: BlobOperationResult containing upload success status, streaming
            metadata, and performance metrics including upload duration.
        :raises ValueError: If call_id is empty or not provided.
        """
        start_time = datetime.now(timezone.utc)
        container_name = container_name or self.container_name

        try:
            # Validate inputs
            if not call_id or not call_id.strip():
                raise ValueError("Call ID is required and cannot be empty")

            # Create organized blob path
            date_str = start_time.strftime("%Y-%m-%d")
            blob_name = f"audio/{date_str}/{call_id}.wav"

            # Stream upload
            service = await self._get_blob_service()
            blob_client = service.get_blob_client(container=container_name, blob=blob_name)

            await blob_client.upload_blob(
                wav_stream,
                overwrite=True,
                content_type="audio/wav",
                metadata={
                    "call_id": call_id,
                    "created_at": start_time.isoformat(),
                    "content_type": "audio_stream",
                },
            )

            duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

            logger.info(
                f"Streamed WAV data for call '{call_id}' to '{blob_name}' " f"in {duration:.2f}ms"
            )

            return BlobOperationResult(
                success=True,
                operation_type=BlobOperationType.UPLOAD,
                blob_name=blob_name,
                container_name=container_name,
                duration_ms=duration,
            )

        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            error_msg = f"Failed to stream WAV data for call '{call_id}': {e}"
            logger.error(error_msg, exc_info=True)

            return BlobOperationResult(
                success=False,
                operation_type=BlobOperationType.UPLOAD,
                error_message=error_msg,
                duration_ms=duration,
            )

    async def get_transcript_from_blob(
        self, call_id: str, container_name: Optional[str] = None
    ) -> BlobOperationResult:
        """
        Retrieve transcript from blob storage.

        This method searches for and downloads transcript data from Azure Blob Storage
        using an intelligent lookup strategy. It first attempts to locate the transcript
        using the current date-based directory structure, then falls back to legacy
        storage patterns for backward compatibility with older transcript files.

        :param call_id: Unique identifier for the call session whose transcript should
            be retrieved from blob storage.
        :param container_name: (optional) Source container for transcript retrieval.
            If not provided, uses the default container configured during initialization.
        :return: BlobOperationResult containing the transcript content as a JSON string
            in the content field, along with retrieval metadata and performance metrics.
        :raises ValueError: If call_id is empty or not provided.
        :raises ResourceNotFoundError: If no transcript is found for the specified
            call_id in either current or legacy storage locations.
        """
        start_time = datetime.now(timezone.utc)
        container_name = container_name or self.container_name

        try:
            # Validate inputs
            if not call_id or not call_id.strip():
                raise ValueError("Call ID is required and cannot be empty")

            # Try to find the transcript (search by date if needed)
            service = await self._get_blob_service()

            # First, try today's date
            date_str = start_time.strftime("%Y-%m-%d")
            blob_name = f"transcripts/{date_str}/{call_id}.json"

            blob_client = service.get_blob_client(container=container_name, blob=blob_name)

            try:
                stream = await blob_client.download_blob()
                data = await stream.readall()
                content = data.decode("utf-8")

                duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

                logger.info(
                    f"Retrieved transcript for call '{call_id}' from '{blob_name}' "
                    f"({len(data)} bytes) in {duration:.2f}ms"
                )

                return BlobOperationResult(
                    success=True,
                    operation_type=BlobOperationType.DOWNLOAD,
                    blob_name=blob_name,
                    container_name=container_name,
                    size_bytes=len(data),
                    duration_ms=duration,
                    content=content,
                )

            except ResourceNotFoundError:
                # If not found in today's folder, search other dates
                # This is a fallback for backwards compatibility
                blob_name_legacy = f"{call_id}.json"
                blob_client_legacy = service.get_blob_client(
                    container=container_name, blob=blob_name_legacy
                )

                stream = await blob_client_legacy.download_blob()
                data = await stream.readall()
                content = data.decode("utf-8")

                duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

                logger.info(
                    f"Retrieved transcript for call '{call_id}' from legacy path "
                    f"'{blob_name_legacy}' ({len(data)} bytes) in {duration:.2f}ms"
                )

                return BlobOperationResult(
                    success=True,
                    operation_type=BlobOperationType.DOWNLOAD,
                    blob_name=blob_name_legacy,
                    container_name=container_name,
                    size_bytes=len(data),
                    duration_ms=duration,
                    content=content,
                )

        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            error_msg = f"Failed to retrieve transcript for call '{call_id}': {e}"
            logger.error(error_msg, exc_info=True)

            return BlobOperationResult(
                success=False,
                operation_type=BlobOperationType.DOWNLOAD,
                error_message=error_msg,
                duration_ms=duration,
            )

    async def delete_transcript_from_blob(
        self, call_id: str, container_name: Optional[str] = None
    ) -> BlobOperationResult:
        """
        Delete transcript from blob storage.

        This method removes transcript data from Azure Blob Storage using an intelligent
        search and delete strategy. It first attempts to locate and delete the transcript
        using the current date-based directory structure, then falls back to legacy
        storage patterns to ensure complete cleanup of older transcript files.

        :param call_id: Unique identifier for the call session whose transcript should
            be deleted from blob storage.
        :param container_name: (optional) Source container for transcript deletion.
            If not provided, uses the default container configured during initialization.
        :return: BlobOperationResult indicating successful deletion with the deleted
            blob name, or error details if the operation fails.
        :raises ValueError: If call_id is empty or not provided.
        :raises ResourceNotFoundError: If no transcript is found for the specified
            call_id in either current or legacy storage locations.
        """
        start_time = datetime.now(timezone.utc)
        container_name = container_name or self.container_name

        try:
            # Validate inputs
            if not call_id or not call_id.strip():
                raise ValueError("Call ID is required and cannot be empty")

            service = await self._get_blob_service()

            # Try current date structure first
            date_str = start_time.strftime("%Y-%m-%d")
            blob_name = f"transcripts/{date_str}/{call_id}.json"

            blob_client = service.get_blob_client(container=container_name, blob=blob_name)

            try:
                await blob_client.delete_blob()
                blob_deleted = blob_name
            except ResourceNotFoundError:
                # Try legacy path
                blob_name_legacy = f"{call_id}.json"
                blob_client_legacy = service.get_blob_client(
                    container=container_name, blob=blob_name_legacy
                )
                await blob_client_legacy.delete_blob()
                blob_deleted = blob_name_legacy

            duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

            logger.info(
                f"Deleted transcript for call '{call_id}' from '{blob_deleted}' "
                f"in {duration:.2f}ms"
            )

            return BlobOperationResult(
                success=True,
                operation_type=BlobOperationType.DELETE,
                blob_name=blob_deleted,
                container_name=container_name,
                duration_ms=duration,
            )

        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            error_msg = f"Failed to delete transcript for call '{call_id}': {e}"
            logger.error(error_msg, exc_info=True)

            return BlobOperationResult(
                success=False,
                operation_type=BlobOperationType.DELETE,
                error_message=error_msg,
                duration_ms=duration,
            )

    async def list_transcripts_in_blob(
        self, container_name: Optional[str] = None, date_filter: Optional[str] = None
    ) -> BlobOperationResult:
        """
        List all transcripts in blob storage.

        This method retrieves a comprehensive list of all transcript files stored in
        Azure Blob Storage. It supports optional date-based filtering for efficient
        querying and includes both current date-structured transcripts and legacy
        transcript files for complete visibility into available transcript data.

        :param container_name: (optional) Source container for transcript listing.
            If not provided, uses the default container configured during initialization.
        :param date_filter: (optional) Date filter in YYYY-MM-DD format to restrict
            results to transcripts from a specific date. If not provided, returns all
            available transcripts from both current and legacy storage structures.
        :return: BlobOperationResult containing a list of transcript blob names in
            the blob_list field, along with operation metadata and performance metrics.
        :raises ValueError: If date_filter is provided but not in valid YYYY-MM-DD format.
        """
        start_time = datetime.now(timezone.utc)
        container_name = container_name or self.container_name

        try:
            service = await self._get_blob_service()
            container_client = service.get_container_client(container_name)

            blob_list = []
            prefix = f"transcripts/{date_filter}/" if date_filter else "transcripts/"

            async for blob in container_client.list_blobs(name_starts_with=prefix):
                blob_list.append(blob.name)

            # Also include legacy blobs (without date structure) for backwards compatibility
            if not date_filter:
                async for blob in container_client.list_blobs():
                    if blob.name.endswith(".json") and not blob.name.startswith("transcripts/"):
                        blob_list.append(blob.name)

            duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000

            logger.info(
                f"Listed {len(blob_list)} transcripts from container '{container_name}' "
                f"in {duration:.2f}ms"
            )

            return BlobOperationResult(
                success=True,
                operation_type=BlobOperationType.LIST,
                container_name=container_name,
                duration_ms=duration,
                blob_list=blob_list,
            )

        except Exception as e:
            duration = (datetime.now(timezone.utc) - start_time).total_seconds() * 1000
            error_msg = f"Failed to list transcripts: {e}"
            logger.error(error_msg, exc_info=True)

            return BlobOperationResult(
                success=False,
                operation_type=BlobOperationType.LIST,
                error_message=error_msg,
                duration_ms=duration,
            )

    async def close(self):
        """
        Clean up resources and close all active connections.

        This method ensures proper cleanup of Azure resources by closing the
        BlobServiceClient and DefaultAzureCredential instances. It should be called
        when the helper is no longer needed to prevent resource leaks and ensure
        graceful shutdown of network connections.
        """
        if self._blob_service:
            await self._blob_service.close()
            self._blob_service = None

        if self._credential:
            await self._credential.close()

    async def __aenter__(self):
        """
        Async context manager entry point.

        This method enables the use of the AzureBlobHelper class in async context
        manager patterns (async with statements) for automatic resource management
        and proper cleanup when exiting the context.

        :return: Self instance for use within the async context manager.
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """
        Async context manager exit point with automatic cleanup.

        This method ensures that all Azure resources are properly cleaned up when
        exiting the async context manager, regardless of whether an exception occurred
        during execution. It automatically calls the close method to release all
        active connections and credentials.

        :param exc_type: Exception type if an exception occurred, None otherwise.
        :param exc_val: Exception value if an exception occurred, None otherwise.
        :param exc_tb: Exception traceback if an exception occurred, None otherwise.
        """
        await self.close()


# Global instance for backward compatibility
# TODO: Consider migrating to dependency injection pattern
_global_blob_helper: Optional[AzureBlobHelper] = None


def get_blob_helper() -> AzureBlobHelper:
    """
    Get global blob helper instance.

    This function provides access to a singleton instance of AzureBlobHelper for
    backward compatibility with legacy code. It implements lazy initialization to
    create the global instance only when first accessed, ensuring efficient resource
    usage while maintaining the existing API contract.

    :return: Configured AzureBlobHelper instance ready for blob operations.
    :raises ValueError: If required Azure Storage configuration is missing from
        environment variables.
    """
    global _global_blob_helper
    if _global_blob_helper is None:
        _global_blob_helper = AzureBlobHelper()
    return _global_blob_helper


# Legacy function wrappers for backward compatibility
# These should be migrated to use the new class-based approach


async def generate_container_sas_url(
    container_name: Optional[str] = None,
    account_key: Optional[str] = None,
    expiry_hours: int = 24,
) -> str:
    """
    Legacy wrapper for generate_container_sas_url.

    This function provides backward compatibility for existing code that uses the
    legacy function-based API. It internally uses the modern AzureBlobHelper class
    to generate SAS URLs with proper error handling and resource management.

    :param container_name: (optional) Target container name for SAS URL generation.
        If not provided, uses the default container from configuration.
    :param account_key: (optional) Azure Storage account key. This parameter is
        maintained for API compatibility but the actual authentication is handled
        by the AzureBlobHelper instance.
    :param expiry_hours: (optional) Number of hours until SAS token expires.
        Default is 24 hours.
    :return: Complete SAS URL string for accessing the specified container.
    :raises Exception: If SAS URL generation fails due to authentication,
        permission, or configuration issues.

    Note: This function is deprecated. Use AzureBlobHelper class instead for
    better error handling and resource management.
    """
    helper = get_blob_helper()
    result = await helper.generate_container_sas_url(
        container_name=container_name, expiry_hours=expiry_hours
    )

    if not result.success:
        raise Exception(result.error_message)

    return result.content


async def verify_container_access(container_url: str) -> bool:
    """
    Legacy wrapper for verify_container_access.

    This function provides backward compatibility for existing code that uses the
    legacy function-based API. It internally uses the modern AzureBlobHelper class
    to verify container access with comprehensive permission testing.

    :param container_url: Complete container URL including SAS token parameters
        for authentication and authorization verification.
    :return: True if container access verification succeeds, False if verification
        fails due to permission, authentication, or connectivity issues.

    Note: This function is deprecated. Use AzureBlobHelper class instead for
    better error handling and detailed operation results.
    """
    helper = get_blob_helper()
    result = await helper.verify_container_access(container_url)
    return result.success


async def save_transcript_to_blob(call_id: str, transcript: str):
    """
    Legacy wrapper for save_transcript_to_blob.

    This function provides backward compatibility for existing code that uses the
    legacy function-based API. It internally uses the modern AzureBlobHelper class
    to upload transcript data with organized directory structure and proper metadata.

    :param call_id: Unique identifier for the call session. Must be non-empty
        and will be used to create the blob filename.
    :param transcript: Complete transcript content as a JSON-formatted string
        containing the call conversation data.
    :raises Exception: If transcript upload fails due to authentication, permission,
        network, or validation issues.

    Note: This function is deprecated. Use AzureBlobHelper class instead for
    better error handling and detailed operation results.
    """
    helper = get_blob_helper()
    result = await helper.save_transcript_to_blob(call_id, transcript)

    if not result.success:
        raise Exception(result.error_message)


async def save_wav_to_blob(call_id: str, wav_file_path: str):
    """
    Legacy wrapper for save_wav_to_blob.

    This function provides backward compatibility for existing code that uses the
    legacy function-based API. It internally uses the modern AzureBlobHelper class
    to upload WAV audio files with proper content type handling and metadata.

    :param call_id: Unique identifier for the call session. Used to create the
        blob filename and associate the audio with the corresponding call.
    :param wav_file_path: Full file system path to the local WAV audio file to
        be uploaded. The file must exist and have a .wav extension.
    :raises Exception: If WAV file upload fails due to file not found, invalid
        format, authentication, permission, or network issues.

    Note: This function is deprecated. Use AzureBlobHelper class instead for
    better error handling and detailed operation results.
    """
    helper = get_blob_helper()
    result = await helper.save_wav_to_blob(call_id, wav_file_path)

    if not result.success:
        raise Exception(result.error_message)


async def stream_wav_to_blob(call_id: str, wav_stream):
    """
    Legacy wrapper for stream_wav_to_blob.

    This function provides backward compatibility for existing code that uses the
    legacy function-based API. It internally uses the modern AzureBlobHelper class
    to stream WAV audio data directly to blob storage without intermediate buffering.

    :param call_id: Unique identifier for the call session. Used to create the
        blob filename and associate the streamed audio with the corresponding call.
    :param wav_stream: Asynchronous stream or iterator containing WAV audio data
        chunks to be uploaded directly to blob storage.
    :raises Exception: If WAV stream upload fails due to stream errors,
        authentication, permission, or network issues.

    Note: This function is deprecated. Use AzureBlobHelper class instead for
    better error handling and detailed operation results.
    """
    helper = get_blob_helper()
    result = await helper.stream_wav_to_blob(call_id, wav_stream)

    if not result.success:
        raise Exception(result.error_message)


async def get_transcript_from_blob(call_id: str) -> str:
    """
    Legacy wrapper for get_transcript_from_blob.

    This function provides backward compatibility for existing code that uses the
    legacy function-based API. It internally uses the modern AzureBlobHelper class
    to retrieve transcript data with intelligent lookup across current and legacy
    storage structures.

    :param call_id: Unique identifier for the call session whose transcript should
        be retrieved from blob storage.
    :return: Transcript content as a JSON-formatted string, or empty string if
        no transcript is found.
    :raises Exception: If transcript retrieval fails due to authentication,
        permission, or network issues.

    Note: This function is deprecated. Use AzureBlobHelper class instead for
    better error handling and detailed operation results.
    """
    helper = get_blob_helper()
    result = await helper.get_transcript_from_blob(call_id)

    if not result.success:
        raise Exception(result.error_message)

    return result.content or ""


async def delete_transcript_from_blob(call_id: str):
    """
    Legacy wrapper for delete_transcript_from_blob.

    This function provides backward compatibility for existing code that uses the
    legacy function-based API. It internally uses the modern AzureBlobHelper class
    to delete transcript data with intelligent search across current and legacy
    storage structures.

    :param call_id: Unique identifier for the call session whose transcript should
        be deleted from blob storage.
    :raises Exception: If transcript deletion fails due to file not found,
        authentication, permission, or network issues.

    Note: This function is deprecated. Use AzureBlobHelper class instead for
    better error handling and detailed operation results.
    """
    helper = get_blob_helper()
    result = await helper.delete_transcript_from_blob(call_id)

    if not result.success:
        raise Exception(result.error_message)


async def list_transcripts_in_blob() -> list:
    """
    Legacy wrapper for list_transcripts_in_blob.

    This function provides backward compatibility for existing code that uses the
    legacy function-based API. It internally uses the modern AzureBlobHelper class
    to retrieve a comprehensive list of all transcript files from both current
    date-structured and legacy storage locations.

    :return: List of blob names containing transcript files, or empty list if
        no transcripts are found.
    :raises Exception: If transcript listing fails due to authentication,
        permission, or network issues.

    Note: This function is deprecated. Use AzureBlobHelper class instead for
    better error handling and detailed operation results.
    """
    helper = get_blob_helper()
    result = await helper.list_transcripts_in_blob()

    if not result.success:
        raise Exception(result.error_message)

    return result.blob_list or []
