"""
services/redis_services.py
--------------------------
Re-export thin wrappers around Azure Redis that your code already
implements in `src.redis.*`. Keeping them here isolates the rest of
the app from the direct SDK dependency.
"""

from src.redis.async_manager import AsyncAzureRedisManager as AzureRedisManager

__all__ = [
    "AzureRedisManager",
]

