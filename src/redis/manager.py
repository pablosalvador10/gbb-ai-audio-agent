from typing import Optional, Dict, Any, List
import redis
from utils.ml_logging import get_logger
import os


class AzureRedisManager:
    """
    AzureRedisManager provides a simplified interface to connect, store,
    retrieve, and manage session data using Azure Cache for Redis.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        access_key: Optional[str] = None,
        port: int = 6380,
        db: int = 0,
        ssl: bool = True,
    ):
        """
        Initialize the Redis connection.

        Args:
            host (str, optional): The Redis host name. If not provided, uses REDIS_HOST env variable.
            access_key (str, optional): The Redis access key. If not provided, uses REDIS_ACCESS_KEY env variable.
            port (int): Redis port, default is 6380.
            db (int): Redis database index.
            ssl (bool): Use SSL for the connection.
        """
        self.logger = get_logger()
        self.host = host or os.getenv("REDIS_HOST")
        self.access_key = access_key or os.getenv("REDIS_ACCESS_KEY")
        if not self.host or not self.access_key:
            raise ValueError(
                "Redis host and access key must be provided either as arguments or environment variables."
            )
        self.redis_client = redis.Redis(
            host=self.host, port=port, db=db, password=self.access_key, ssl=ssl
        )
        self.logger.info("Azure Redis connection initialized.")

    def ping(self) -> bool:
        """Check Redis connectivity."""
        return self.redis_client.ping()

    def set_value(self, key: str, value: str) -> bool:
        """Set a string value in Redis."""
        return self.redis_client.set(key, value)

    def get_value(self, key: str) -> Optional[str]:
        """Get a string value from Redis."""
        value = self.redis_client.get(key)
        return value.decode() if value else None

    def store_session_data(self, session_id: str, data: Dict[str, Any]) -> bool:
        """Store session data using a Redis hash."""
        return self.redis_client.hset(session_id, mapping=data)

    def get_session_data(self, session_id: str) -> Dict[str, str]:
        """Retrieve all session data for a given session ID."""
        return {
            k.decode(): v.decode()
            for k, v in self.redis_client.hgetall(session_id).items()
        }

    def update_session_field(self, session_id: str, field: str, value: str) -> bool:
        """Update a single field in the session hash."""
        return self.redis_client.hset(session_id, field, value)

    def delete_session(self, session_id: str) -> int:
        """Delete a session from Redis."""
        return self.redis_client.delete(session_id)

    def list_connected_clients(self) -> List[Dict[str, str]]:
        """List currently connected clients."""
        return self.redis_client.client_list()
