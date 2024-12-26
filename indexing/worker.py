from redis import Redis
import asyncio
from datetime import datetime, timezone, time, timedelta
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

logger = logging.getLogger(__name__)

class IndexingWorker:
    def __init__(
        self, 
        redis: Redis,
        qdrant_api_key: str,
        max_concurrent: int = 1,
        retry_delay: int = 300,
        debug: bool = False
    ):
        self.redis = redis
        self.semaphore = asyncio.Semaphore(1)
        self.retry_delay = retry_delay
        self.max_retries = 3
        self.max_concurrent = 1
        self.debug = debug
        self.error_delay = 5
        self.processing_delay = 1
        
        # Setup logging
        self.logger = logging.getLogger('indexing_worker')
        self.logger.setLevel(logging.DEBUG if debug else logging.INFO)
        handler = logging.FileHandler('indexing_worker.log')
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - [%(name)s] - %(message)s'
        ))
        self.logger.addHandler(handler)

        # Initialize engines with proper configuration
        self.qdrant_engine = QdrantSearchEngine(qdrant_api_key)
        self.es_engine = ElasticsearchBM25()
        
        # Pass the full engine instances
        self.processor = ContentProcessor(
            qdrant_engine=self.qdrant_engine,
            es_engine=self.es_engine
        )
        
        self.logger.info("IndexingWorker initialized with configuration:")
        self.logger.info(f"- Max concurrent: {self.max_concurrent}")
        self.logger.info(f"- Retry delay: {self.retry_delay}")
        self.logger.info(f"- Max retries: {self.max_retries}")
        self.logger.info(f"- Debug mode: {self.debug}")
        
    async def _process_content_safe(self, content_id: str):
        start_time = time()
        self.logger.info(f"Starting processing content: {content_id}")
        
        async with self.semaphore:
            try:
                self.redis.sadd(RedisKeys.PROCESSING_SET, content_id)
                self.logger.debug(f"Added {content_id} to processing set")
                
                # Get content type
                async with get_session() as session:
                    try:
                        content_uuid = UUID(content_id)
                    except ValueError:
                        self.logger.error(f"Invalid content ID format: {content_id}")
                        raise ProcessingError(f"Invalid content ID format: {content_id}")
                        
                    content = await session.get(Content, content_uuid)
                    if not content:
                        raise ProcessingError(f"Content {content_id} not found")
                    self.logger.info(f"Processing content type: {content.type}")

                # In debug mode, validate user access
                if self.debug:
                    async with get_session() as session:
                        user_content = await session.execute(
                            select(UserContent)
                            .where(and_(
                                UserContent.content_id == content_uuid,
                                UserContent.access_level != AccessLevel.REMOVED.value
                            ))
                            .order_by(UserContent.access_level.desc())
                            .limit(1)
                        )
                        user_content = user_content.scalar_one_or_none()
                        if not user_content:
                            self.logger.error(f"No user content found for {content_id}")
                            raise ProcessingError("No user content found")
                            
                        test_token = await get_user_token(user_content.user_id)
                        if test_token != "3ae04e20124d40babc5107e658c666b6":  # Default test user token
                            self.logger.info(f"Debug mode: Skipping non-test token content: {content_id}")
                            self._cleanup_success(content_id)  # Mark as processed
                            return
                        self.logger.info(f"Debug mode: Processing test token content: {content_id}")
                        token = test_token
                else:
                    # Get required info for non-debug mode
                    token = await get_content_token(content_id)
                    if not token:
                        # If no token found, try getting user token for notes
                        if content.type == ContentType.NOTE.value:
                            async with get_session() as session:
                                user_content = await session.execute(
                                    select(UserContent)
                                    .where(and_(
                                        UserContent.content_id == content_uuid,
                                        UserContent.access_level != AccessLevel.REMOVED.value
                                    ))
                                    .order_by(UserContent.access_level.desc())
                                    .limit(1)
                                )
                                user_content = user_content.scalar_one_or_none()
                                if user_content:
                                    token = await get_user_token(user_content.user_id)
                        
                        if not token:
                            self.logger.error(f"No token found for content {content_id}")
                            raise ProcessingError("No token found")

                self.logger.debug(f"Retrieved token for content {content_id}")

                # Process the content based on type
                vexa = VexaAPI(token=token)
                user_info = await vexa.get_user_info()
                user_id = user_info['id']
                self.logger.debug(f"Retrieved user info for content {content_id}: user_id={user_id}")
                
                # Process using processor
                async with get_session() as session:
                    try:
                        self.logger.info(f"Starting content processing for {content_id}")
                        await self.processor.process_content(
                            content_id=content_id,
                            user_id=user_id,
                            token=token,
                            session=session
                        )
                        processing_time = time() - start_time
                        self.logger.info(f"Successfully processed content {content_id} in {processing_time:.2f} seconds")
                        self._cleanup_success(content_id)
                    except Exception as e:
                        self.logger.error(f"Failed processing content {content_id} after {time() - start_time:.2f} seconds")
                        self.logger.error(f"Error details: {str(e)}", exc_info=True)
                        await self._handle_error(content_id, e)
                        await asyncio.sleep(self.error_delay)  # Add delay after error
            finally:
                self.redis.srem(RedisKeys.PROCESSING_SET, content_id)
                self.logger.debug(f"Removed {content_id} from processing set")

    def _cleanup_success(self, content_id: str):
        """Remove from queue and processing set on success"""
        self.logger.debug(f"Cleaning up successful processing for {content_id}")
        self.redis.zrem(RedisKeys.INDEXING_QUEUE, content_id)
        self.redis.srem(RedisKeys.PROCESSING_SET, content_id)
        self.logger.info(f"Successfully cleaned up content {content_id}")

    async def _handle_error(self, content_id: str, error: Exception):
        """Handle processing errors with retries"""
        retry_count = self.redis.hincrby(RedisKeys.RETRY_COUNTS, content_id, 1)
        self.logger.warning(f"Processing error for content {content_id} (retry {retry_count}/{self.max_retries})")
        self.logger.error(f"Error details: {str(error)}", exc_info=True)
        
        if retry_count <= self.max_retries:
            next_try = datetime.now().timestamp() + self.retry_delay
            self.redis.zadd(RedisKeys.INDEXING_QUEUE, {content_id: next_try})
            self.logger.info(f"Scheduled retry {retry_count} for content {content_id} at {datetime.fromtimestamp(next_try)}")
        else:
            self.logger.error(f"Content {content_id} failed after {self.max_retries} retries - removing from queue")
            self._cleanup_success(content_id)  # Remove from queue

    async def run(self):
        """Single concurrent processing loop"""
        self.logger.info("Starting indexing worker run loop")
        processing_tasks = set()
        pending_meetings = deque()
        processed_meetings = set()
        
        while True:
            try:
                # Cleanup stale processing entries
                await self._cleanup_stale_processing(processing_tasks)
                
                # Refill pending_meetings if empty
                if not pending_meetings:
                    self.logger.debug("Refilling pending meetings queue")
                    await self._refill_pending_meetings(pending_meetings, processing_tasks)
                
                # Process single meeting if none processing
                if not processing_tasks and pending_meetings:
                    content_id = pending_meetings.popleft()
                    self.logger.debug(f"Starting new task for content {content_id}")
                    
                    # Skip if already processing
                    if self.redis.sismember(RedisKeys.PROCESSING_SET, content_id):
                        if content_id not in processed_meetings:
                            self.logger.info(f"Skipping queue add for content {content_id} - already processing")
                            processed_meetings.add(content_id)
                        continue
                    
                    # Create and track new task
                    task = asyncio.create_task(self._process_content_safe(content_id))
                    task.set_name(content_id)
                    processing_tasks.add(task)
                    task.add_done_callback(processing_tasks.discard)
                    self.logger.debug(f"Created new task for content {content_id}")
                
                # Wait for task completion if processing
                if processing_tasks:
                    self.logger.debug(f"Waiting for {len(processing_tasks)} tasks to complete")
                    await asyncio.wait(
                        processing_tasks,
                        return_when=asyncio.ALL_COMPLETED
                    )
                
                # Small sleep to prevent tight loop
                await asyncio.sleep(self.processing_delay)
                
            except Exception as e:
                self.logger.error(f"Worker loop error: {str(e)}", exc_info=True)
                await asyncio.sleep(self.error_delay)

    async def _refill_pending_meetings(self, pending_meetings: deque, processing_tasks: set):
        """Refill pending meetings queue if below threshold"""
        if len(pending_meetings) < self.max_concurrent * 2:
            now = datetime.now().timestamp()
            next_contents = self.redis.zrangebyscore(
                RedisKeys.INDEXING_QUEUE,
                '-inf', now,
                start=0,
                num=self.max_concurrent * 2
            )
            
            self.logger.debug(f"Found {len(next_contents)} contents ready for processing")
            for m in next_contents:
                content_id = m.decode() if isinstance(m, bytes) else m
                
                # Validate content ID
                if not content_id:
                    self.logger.error("Found None/empty content ID in queue - removing")
                    self.redis.zrem(RedisKeys.INDEXING_QUEUE, m)
                    continue
                    
                try:
                    UUID(content_id)
                except ValueError:
                    self.logger.error(f"Found invalid content ID in queue: {content_id} - removing")
                    self.redis.zrem(RedisKeys.INDEXING_QUEUE, m)
                    continue
                
                if (content_id not in pending_meetings and 
                    not any(t for t in processing_tasks if t.get_name() == content_id)):
                    pending_meetings.append(content_id)
                    self.logger.debug(f"Added content {content_id} to pending queue")

    async def _cleanup_stale_processing(self, processing_tasks: set):
        """Remove stale entries from processing set"""
        stale_timeout = 30 * 60  # 30 minutes
        now = datetime.now().timestamp()
        
        processing = self.redis.smembers(RedisKeys.PROCESSING_SET)
        self.logger.debug(f"Checking {len(processing)} items in processing set for staleness")
        
        for mid in processing:
            content_id = mid.decode() if isinstance(mid, bytes) else mid
            if not any(t for t in processing_tasks if t.get_name() == content_id):
                retry_score = self.redis.zscore(RedisKeys.INDEXING_QUEUE, content_id)
                if not retry_score or (now - retry_score) > stale_timeout:
                    self.logger.warning(f"Removing stale processing entry for content {content_id}")
                    self.redis.srem(RedisKeys.PROCESSING_SET, content_id)