import asyncio
import logging
from app.models.psql_models import (
    Content,
    ContentType,
    UserContent,
    AccessLevel,
    Thread,
    Entity,
    EntityType
)
from app.utils.db import get_session
from sqlalchemy import select, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Optional, List, Dict, Any
from uuid import UUID
import redis.asyncio as redis
from .redis_keys import RedisKeys

class MeetingsMonitor:
    def __init__(self, redis_url: str = "redis://redis:6379/0"):
        self.redis = redis.from_url(redis_url)
        
    async def add_to_queue(self, content_id: UUID) -> None:
        await self.redis.lpush(RedisKeys.INDEXING_QUEUE, str(content_id))
        await self.redis.set(
            RedisKeys.INDEXING_STATUS.format(content_id=content_id),
            "queued"
        )
        
    async def get_status(self, content_id: UUID) -> str:
        status = await self.redis.get(RedisKeys.INDEXING_STATUS.format(content_id=content_id))
        return status.decode() if status else "not_found"
        
    async def get_error(self, content_id: UUID) -> Optional[str]:
        error = await self.redis.get(RedisKeys.INDEXING_ERROR.format(content_id=content_id))
        return error.decode() if error else None