from redis import Redis
import asyncio
from datetime import datetime
import logging
from time import time
from collections import deque
from sqlalchemy.ext.asyncio import AsyncSession
from elasticsearch import AsyncElasticsearch
from qdrant_client import AsyncQdrantClient

from .processor import ContentProcessor, ProcessingError
from .redis_keys import RedisKeys
from psql_helpers import get_session
from psql_access import get_meeting_token
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
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler('indexing_worker.log')
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
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

    async def _process_content_safe(self, content_id: str):
        start_time = time()
        self.logger.info(f"Starting processing content: {content_id}")
        
        async with self.semaphore:
            try:
                self.redis.sadd(RedisKeys.PROCESSING_SET, content_id)
                
                # Get required info
                token = await get_meeting_token(content_id)
                if not token:
                    raise ProcessingError("No token found")

                # In debug mode, validate test user token
                if self.debug:
                    test_token = "3ae04e20124d40babc5107e658c666b6"  # Default test user token
                    if token != test_token:
                        self.logger.info(f"Debug mode: Skipping non-test token content: {content_id}")
                        self._cleanup_success(content_id)  # Mark as processed
                        return
                    self.logger.info(f"Debug mode: Processing test token content: {content_id}")

                vexa = VexaAPI(token=token)
                user_id = (await vexa.get_user_info())['id']
                
                # Process using processor
                async with get_session() as session:
                    try:
                        await self.processor.process_content(
                            content_id=content_id,
                            user_id=user_id,
                            token=token,
                            session=session
                        )
                        self._cleanup_success(content_id)
                    except Exception as e:
                        self.logger.error(f"Failed processing content {content_id} after {time() - start_time:.2f} seconds. Error: {str(e)}")
                        await self._handle_error(content_id, e)
                        await asyncio.sleep(self.error_delay)  # Add delay after error
            finally:
                self.redis.srem(RedisKeys.PROCESSING_SET, content_id)

    def _cleanup_success(self, content_id: str):
        """Remove from queue and processing set on success"""
        self.redis.zrem(RedisKeys.INDEXING_QUEUE, content_id)
        self.redis.srem(RedisKeys.PROCESSING_SET, content_id)

    async def _handle_error(self, content_id: str, error: Exception):
        """Handle processing errors with retries"""
        retry_count = self.redis.hincrby(RedisKeys.RETRY_COUNTS, content_id, 1)
        
        if retry_count <= self.max_retries:
            next_try = datetime.now().timestamp() + self.retry_delay
            self.redis.zadd(RedisKeys.INDEXING_QUEUE, {content_id: next_try})
            self.logger.info(f"Scheduled retry {retry_count} for content {content_id}")
        else:
            self.logger.error(f"Content {content_id} failed after {self.max_retries} retries")
            self._cleanup_success(content_id)  # Remove from queue

    async def run(self):
        """Single concurrent processing loop"""
        processing_tasks = set()
        pending_meetings = deque()
        processed_meetings = set()
        
        while True:
            try:
                # Cleanup stale processing entries
                await self._cleanup_stale_processing(processing_tasks)
                
                # Refill pending_meetings if empty
                if not pending_meetings:
                    await self._refill_pending_meetings(pending_meetings, processing_tasks)
                
                # Process single meeting if none processing
                if not processing_tasks and pending_meetings:
                    content_id = pending_meetings.popleft()
                    
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
                
                # Wait for task completion if processing
                if processing_tasks:
                    await asyncio.wait(
                        processing_tasks,
                        return_when=asyncio.ALL_COMPLETED
                    )
                
                # Small sleep to prevent tight loop
                await asyncio.sleep(self.processing_delay)
                
            except Exception as e:
                self.logger.error(f"Worker loop error: {e}")
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
            
            for m in next_contents:
                content_id = m.decode() if isinstance(m, bytes) else m
                if (content_id not in pending_meetings and 
                    not any(t for t in processing_tasks if t.get_name() == content_id)):
                    pending_meetings.append(content_id)

    async def _cleanup_stale_processing(self, processing_tasks: set):
        """Remove stale entries from processing set"""
        stale_timeout = 30 * 60  # 30 minutes
        now = datetime.now().timestamp()
        
        processing = self.redis.smembers(RedisKeys.PROCESSING_SET)
        for mid in processing:
            content_id = mid.decode() if isinstance(mid, bytes) else mid
            if not any(t for t in processing_tasks if t.get_name() == content_id):
                retry_score = self.redis.zscore(RedisKeys.INDEXING_QUEUE, content_id)
                if not retry_score or (now - retry_score) > stale_timeout:
                    self.logger.warning(f"Removing stale processing entry for content {content_id}")
                    self.redis.srem(RedisKeys.PROCESSING_SET, content_id)