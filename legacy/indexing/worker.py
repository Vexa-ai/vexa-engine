from redis import Redis
import asyncio
from datetime import datetime
import logging
from time import time
from collections import deque
from sqlalchemy.ext.asyncio import AsyncSession
from elasticsearch import AsyncElasticsearch
from qdrant_client import AsyncQdrantClient
from uuid import UUID
from typing import Set, Any
from sqlalchemy import select, and_

from .processor import ContentProcessor, ProcessingError
from .redis_keys import RedisKeys
from psql_helpers import get_session
from psql_access import get_content_token, get_user_token
from psql_models import ContentType, Content, UserContent, AccessLevel
from vexa import VexaAPI
from qdrant_search import QdrantSearchEngine
from bm25_search import ElasticsearchBM25
from .decorators import handle_processing_errors, with_logging, with_redis_ops

logger = logging.getLogger(__name__)

class IndexingWorker:
    def __init__(self, redis: Redis, qdrant_api_key: str, max_concurrent: int = 1, retry_delay: int = 300, debug: bool = False):
        self.redis = redis
        self.semaphore = asyncio.Semaphore(1)
        self.retry_delay = retry_delay
        self.max_retries = 3
        self.max_concurrent = 1
        self.debug = debug
        self.error_delay = 5
        self.processing_delay = 1
        
        self.logger = logging.getLogger('indexing_worker')
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)
        handler = logging.FileHandler('indexing_worker.log')
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'))
        self.logger.addHandler(handler)

        self.qdrant_engine = QdrantSearchEngine(qdrant_api_key)
        self.es_engine = ElasticsearchBM25()
        self.processor = ContentProcessor(qdrant_engine=self.qdrant_engine, es_engine=self.es_engine)
        
        self.logger.info(f"IndexingWorker initialized with: max_concurrent={self.max_concurrent}, retry_delay={self.retry_delay}, max_retries={self.max_retries}, debug={self.debug}")

    @handle_processing_errors()
    @with_logging('info')
    async def _process_content_safe(self, content_id: str):
        start_time = time()
        async with self.semaphore:
            try:
                self.redis.sadd(RedisKeys.PROCESSING_SET, content_id)
                async with get_session() as session:
                    content_uuid = UUID(content_id)
                    content = await session.get(Content, content_uuid)
                    if not content:
                        raise ProcessingError(f"Content {content_id} not found")

                token = await self._get_token(content_uuid, content)
                vexa = VexaAPI(token=token)
                user_id = (await vexa.get_user_info())['id']
                
                async with get_session() as session:
                    await self.processor.process_content(content_id=content_id, user_id=user_id, token=token, session=session)
                    await self._cleanup_success(content_id)
            finally:
                self.redis.srem(RedisKeys.PROCESSING_SET, content_id)

    async def _get_token(self, content_uuid: UUID, content: Content) -> str:
        if self.debug:
            async with get_session() as session:
                user_content = await session.execute(
                    select(UserContent)
                    .where(and_(UserContent.content_id == content_uuid, UserContent.access_level != AccessLevel.REMOVED.value))
                    .order_by(UserContent.access_level.desc())
                    .limit(1)
                )
                user_content = user_content.scalar_one_or_none()
                if not user_content:
                    raise ProcessingError("No user content found")
                test_token = await get_user_token(user_content.user_id)
                if not test_token:
                    raise ProcessingError("Failed to get test token")
                # if test_token != "3ae04e20124d40babc5107e658c666b6":
                #     await self._cleanup_success(str(content_uuid))
                #     raise ProcessingError("Invalid test token")
                return test_token
        else:
            token = await get_content_token(str(content_uuid))
            if not token and content.type == ContentType.NOTE.value:
                async with get_session() as session:
                    user_content = await session.execute(
                        select(UserContent)
                        .where(and_(UserContent.content_id == content_uuid, UserContent.access_level != AccessLevel.REMOVED.value))
                        .order_by(UserContent.access_level.desc())
                        .limit(1)
                    )
                    user_content = user_content.scalar_one_or_none()
                    if user_content:
                        token = await get_user_token(user_content.user_id)
            if not token:
                raise ProcessingError("No token found")
            return token

    @with_redis_ops("cleanup")
    async def _cleanup_success(self, content_id: str):
        self.redis.zrem(RedisKeys.INDEXING_QUEUE, content_id)
        self.redis.srem(RedisKeys.PROCESSING_SET, content_id)

    @with_redis_ops("error handling")
    async def _handle_error(self, content_id: str, error: Exception):
        retry_count = self.redis.hincrby(RedisKeys.RETRY_COUNTS, content_id, 1)
        if retry_count <= self.max_retries:
            next_try = datetime.now().timestamp() + self.retry_delay
            self.redis.zadd(RedisKeys.INDEXING_QUEUE, {content_id: next_try})
        else:
            await self._cleanup_success(content_id)

    @handle_processing_errors()
    @with_logging('info')
    async def run(self):
        processing_tasks = set()
        pending_meetings = deque()
        processed_meetings = set()
        
        while True:
            try:
                await self._cleanup_stale_processing(processing_tasks)
                if not pending_meetings:
                    await self._refill_pending_meetings(pending_meetings, processing_tasks)
                
                if not processing_tasks and pending_meetings:
                    content_id = pending_meetings.popleft()
                    if self.redis.sismember(RedisKeys.PROCESSING_SET, content_id):
                        if content_id not in processed_meetings:
                            processed_meetings.add(content_id)
                        continue
                    
                    task = asyncio.create_task(self._process_content_safe(content_id))
                    task.set_name(content_id)
                    processing_tasks.add(task)
                    task.add_done_callback(processing_tasks.discard)
                
                if processing_tasks:
                    await asyncio.wait(processing_tasks, return_when=asyncio.ALL_COMPLETED)
                await asyncio.sleep(self.processing_delay)
            except Exception as e:
                await asyncio.sleep(self.error_delay)

    @with_redis_ops("refill pending")
    async def _refill_pending_meetings(self, pending_meetings: deque, processing_tasks: set):
        if len(pending_meetings) < self.max_concurrent * 2:
            next_contents = self.redis.zrangebyscore(RedisKeys.INDEXING_QUEUE, '-inf', datetime.now().timestamp(), start=0, num=self.max_concurrent * 2)
            
            for m in next_contents:
                content_id = m.decode() if isinstance(m, bytes) else m
                if not content_id:
                    self.redis.zrem(RedisKeys.INDEXING_QUEUE, m)
                    continue
                try:
                    UUID(content_id)
                except ValueError:
                    self.redis.zrem(RedisKeys.INDEXING_QUEUE, m)
                    continue
                
                if content_id not in pending_meetings and not any(t for t in processing_tasks if t.get_name() == content_id):
                    pending_meetings.append(content_id)

    @with_redis_ops("cleanup stale")
    async def _cleanup_stale_processing(self, processing_tasks: set):
        stale_timeout = 30 * 60
        now = datetime.now().timestamp()
        processing = self.redis.smembers(RedisKeys.PROCESSING_SET)
        
        for mid in processing:
            content_id = mid.decode() if isinstance(mid, bytes) else mid
            if not any(t for t in processing_tasks if t.get_name() == content_id):
                retry_score = self.redis.zscore(RedisKeys.INDEXING_QUEUE, content_id)
                if not retry_score or (now - retry_score) > stale_timeout:
                    self.redis.srem(RedisKeys.PROCESSING_SET, content_id)