from typing import Optional, Dict, Any, List
import os
import redis.asyncio as redis
from utils.ml_logging import get_logger

class AsyncAzureRedisManager:
    """
    AzureRedisManager provides a simplified async interface to connect, store,
    retrieve, and manage session data using Azure Cache for Redis.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        access_key: Optional[str] = None,
        port: int = 6380,
        ssl: bool = True,
        credential: Optional[object] = None,  # For DefaultAzureCredential
        user_name: Optional[str] = None,
        scope: Optional[str] = None,
    ):
        self.logger = get_logger(__name__)
        self.host = host or os.getenv("REDIS_ENDPOINT")
        self.access_key = access_key or os.getenv("REDIS_ACCESS_KEY")
        self.port = port or os.getenv("REDIS_PORT")
        self.ssl = ssl
        if not self.host:
            raise ValueError("Redis host must be provided either as argument or environment variable.")

        if ":" in self.host:
            self.host, port = self.host.rsplit(":", 1)
            if port.isdigit():
                self.port = int(port)

        if self.access_key:
            self.redis_client = redis.Redis(
                host=self.host, 
                port=self.port, 
                password=self.access_key, 
                ssl=self.ssl, 
                decode_responses=True
            )
            self.logger.info("Azure Redis async connection initialized with access key.")
        else:
            from azure.identity import DefaultAzureCredential

            cred = credential or DefaultAzureCredential()
            scope = scope or os.getenv("REDIS_SCOPE", "https://redis.azure.com/.default")
            user_name = user_name or os.getenv("REDIS_USER_NAME", "user")
            token = cred.get_token(scope)

            self.redis_client = redis.Redis(
                host=self.host, 
                port=self.port, 
                ssl=self.ssl, 
                username=user_name, 
                password=token.token, 
                decode_responses=True
            )
            self.logger.info("Azure Redis async connection initialized with DefaultAzureCredential.")

    async def ping(self) -> bool:
        """Check Redis connectivity."""
        return await self.redis_client.ping()

    async def set_value(self, key: str, value: str) -> bool:
        """Set a string value in Redis."""
        return await self.redis_client.set(key, value)

    async def get_value(self, key: str) -> Optional[str]:
        """Get a string value from Redis."""
        value = await self.redis_client.get(key)
        return value if value else None

    async def store_session_data(self, session_id: str, data: Dict[str, Any], ttl_seconds: Optional[int] = None) -> bool:
        """Store session data using a Redis hash. Optionally set TTL (in seconds)."""
        result = await self.redis_client.hset(session_id, mapping=data)
        if ttl_seconds is not None:
            await self.redis_client.expire(session_id, ttl_seconds)
        return result

    async def get_session_data(self, session_id: str) -> Dict[str, str]:
        """Retrieve all session data for a given session ID."""
        data = await self.redis_client.hgetall(session_id)
        return {k: v for k, v in data.items()}
    
    async def update_session_field(self, session_id: str, field: str, value: str) -> bool:
        """Update a single field in the session hash."""
        return await self.redis_client.hset(session_id, field, value)

    async def delete_session(self, session_id: str) -> int:
        """Delete a session from Redis."""
        return await self.redis_client.delete(session_id)

    async def list_connected_clients(self) -> List[Dict[str, str]]:
        """List currently connected clients."""
        return await self.redis_client.client_list()
