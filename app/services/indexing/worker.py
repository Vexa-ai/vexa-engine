import asyncio
import json
import logging
from datetime import datetime
from typing import Optional, Set, Any
from uuid import UUID

import redis.asyncio as redis
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.psql_models import Content, ContentType, UserContent, AccessLevel
from app.utils.db import get_session
from app.services.content.access import get_content_token, get_user_token
from .processor import ContentProcessor, ProcessingError
from .redis_keys import RedisKeys

class IndexingWorker:
    def __init__(self, redis_url: str = "redis://redis:6379/0"):
        self.redis = redis.from_url(redis_url)
        self.processor = ContentProcessor()
        
    async def process_content(self, content_id: UUID, session: Optional[AsyncSession] = None) -> None:
        async with (session or get_session()) as session:
            # Get content from database
            query = select(Content).where(Content.id == content_id)
            result = await session.execute(query)
            content = result.scalar_one_or_none()
            
            if not content:
                logging.error(f"Content {content_id} not found")
                return
                
            # Process content
            try:
                await self.processor.process(content, session)
                await self.redis.set(
                    RedisKeys.INDEXING_STATUS.format(content_id=content_id),
                    "completed"
                )
            except ProcessingError as e:
                logging.error(f"Error processing content {content_id}: {str(e)}")
                await self.redis.set(
                    RedisKeys.INDEXING_ERROR.format(content_id=content_id),
                    str(e)
                )
                await self.redis.set(
                    RedisKeys.INDEXING_STATUS.format(content_id=content_id),
                    "error"
                )
                
    async def run(self):
        while True:
            try:
                # Get next content ID from queue
                content_id = await self.redis.lpop(RedisKeys.INDEXING_QUEUE)
                if content_id:
                    content_id = UUID(content_id.decode())
                    await self.process_content(content_id)
                else:
                    await asyncio.sleep(1)  # Wait if queue is empty
            except Exception as e:
                logging.error(f"Error in indexing worker: {str(e)}")
                await asyncio.sleep(1)  # Wait after error