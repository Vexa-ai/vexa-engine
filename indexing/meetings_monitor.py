from datetime import datetime, timezone, timedelta
from sqlalchemy import select, and_, func
from psql_models import (
    User, Content, UserContent, ContentType, AccessLevel, 
    Entity, content_entity_association, UserToken
)
from psql_helpers import get_session
from vexa import VexaAuth, VexaAPI
from redis import Redis
from typing import List, Optional
import logging
from functools import wraps
import asyncio
import json
from .redis_keys import RedisKeys
from .content_relations import update_child_content

logger = logging.getLogger(__name__)

import os
from uuid import UUID
REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = os.getenv('REDIS_PORT', '6379')



def log_execution(func):
    if asyncio.iscoroutinefunction(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            fname = func.__name__
            logger.info(f"Starting {fname}")
            try:
                result = await func(*args, **kwargs)
                logger.info(f"Completed {fname}")
                return result
            except Exception as e:
                logger.error(f"Error in {fname}: {str(e)}")
                raise
        return wrapper
    else:
        @wraps(func)
        def wrapper(*args, **kwargs):
            fname = func.__name__
            logger.info(f"Starting {fname}")
            try:
                result = func(*args, **kwargs)
                logger.info(f"Completed {fname}")
                return result
            except Exception as e:
                logger.error(f"Error in {fname}: {str(e)}")
                raise
        return wrapper

class MeetingsMonitor:
    def __init__(self):
        self.vexa_auth = VexaAuth()
        self.redis = Redis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        self.active_seconds = 60 *30
        
        # Setup logging
        self.logger = logging.getLogger('indexing_worker')
        self.logger.setLevel(logging.INFO)

        # Create logs directory if it doesn't exist
        os.makedirs('logs', exist_ok=True)

        if not self.logger.handlers:  # Prevent duplicate handlers
            handler = logging.FileHandler('logs/indexing_worker.log')  # Updated path
            handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s'
            ))
            self.logger.addHandler(handler)
    

    def _add_to_queue(self, meeting_id: str, score: float = None):
        # Skip if already processing or in queue
        if self.redis.sismember(RedisKeys.PROCESSING_SET, meeting_id):
            self.logger.info(f"Skipping queue add for meeting {meeting_id} - already processing")
            return
        
        if self.redis.zscore(RedisKeys.INDEXING_QUEUE, meeting_id) is not None:
            self.logger.info(f"Skipping queue add for meeting {meeting_id} - already in queue")
            return

        if score is None:
            score = datetime.now().timestamp()
        self.redis.zadd(RedisKeys.INDEXING_QUEUE, {meeting_id: score})
        self.logger.info(f"Added meeting {meeting_id} to queue with score {datetime.fromtimestamp(score).isoformat()}")


    async def _queue_user_content(self, user_id: str, session, days: int = 30):
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        cutoff = (now - timedelta(days=days)).replace(tzinfo=None)
        active_cutoff = (now - timedelta(seconds=self.active_seconds))
        
        # Query for meetings (Content with type=meeting) that user has access to
        stmt = select(Content.content_id).join(UserContent)\
            .where(and_(
                UserContent.user_id == user_id,
                Content.type == ContentType.MEETING.value,
                Content.last_update >= cutoff,
                Content.last_update <= active_cutoff,
                Content.is_indexed == False,
                UserContent.access_level.in_([
                    AccessLevel.OWNER.value,
                    AccessLevel.TRANSCRIPT.value
                ])
            ))
        
        contents = await session.execute(stmt)
        for (content_id,) in contents:
            self._add_to_queue(str(content_id))


    async def sync_meetings_queue(self, last_days:int=30):
        async with get_session() as session:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            last_days = (now - timedelta(days=last_days))
            cutoff = (now - timedelta(seconds=self.active_seconds))
            
            # Get failed content IDs
            failed_contents = set(
                m.decode() if isinstance(m, bytes) else m 
                for m in self.redis.hkeys(RedisKeys.FAILED_SET)
            )
            
            # Get all non-active unindexed meetings
            stmt = select(Content.content_id)\
                .where(and_(
                    Content.type == ContentType.MEETING.value,
                    Content.last_update >= last_days,
                    Content.last_update <= cutoff,
                    Content.is_indexed == False
                ))
            contents = await session.execute(stmt)
            
            # Queue non-failed contents
            for (content_id,) in contents:
                if str(content_id) not in failed_contents:
                    self._add_to_queue(str(content_id))
            
            # Track active meetings
            stmt = select(Content.content_id, UserContent.user_id)\
                .join(UserContent)\
                .where(and_(
                    Content.type == ContentType.MEETING.value,
                    Content.last_update > cutoff,
                    Content.is_indexed == False,
                    UserContent.access_level.in_([
                        AccessLevel.OWNER.value,
                        AccessLevel.TRANSCRIPT.value
                    ])
                ))
            recent_contents = await session.execute(stmt)
            
            # Update active meetings in Redis
            pipe = self.redis.pipeline()
            pipe.delete(RedisKeys.ACTIVE_MEETINGS)
            for content_id, user_id in recent_contents:
                pipe.zadd(RedisKeys.ACTIVE_MEETINGS, {str(content_id): now.timestamp()})
                if not self.redis.sismember(RedisKeys.SEEN_USERS, str(user_id)):
                    await self._queue_user_content(str(user_id), session)
                    self.redis.sadd(RedisKeys.SEEN_USERS, str(user_id))
            pipe.execute()


    async def _ensure_user_exists(self, user_id: str, session) -> None:
        # Get token and user info first
        token_stmt = select(UserToken.token).where(UserToken.user_id == user_id)
        token = await session.scalar(token_stmt) or await self.vexa_auth.get_user_token(user_id=user_id)
        
        if token:
            vexa = VexaAPI(token=token)
            user_info = await vexa.get_user_info()
            # Check by email since it's our unique constraint
            user = await session.scalar(select(User).where(User.email == user_info.get('email')))
            if not user:
                user = User(
                    id=user_id,
                    email=user_info.get('email', ''),
                    username=user_info.get('username', ''),
                    first_name=user_info.get('first_name', ''),
                    last_name=user_info.get('last_name', ''),
                    image=user_info.get('image', ''),
                    created_timestamp=datetime.now(timezone.utc),
                    updated_timestamp=datetime.now(timezone.utc)
                )
                session.add(user)
                
                if not await session.scalar(token_stmt):
                    session.add(UserToken(
                        token=token,
                        user_id=user_id,
                        created_at=datetime.now(timezone.utc)
                    ))
                await session.commit()


    def _parse_timestamp(self, timestamp) -> datetime:
        # If already datetime, just return it
        if isinstance(timestamp, datetime):
            return timestamp
        # Otherwise parse string
        try:
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')


    async def upsert_meetings(self, meetings_data: list):
        async with get_session() as session:
            cutoff = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(seconds=self.active_seconds)
            
            for meeting in meetings_data:
                meeting_id = meeting.meeting_session_id
                user_id = meeting.user_id
                
                # Ensure user exists and is committed before proceeding
                await self._ensure_user_exists(str(user_id), session)
                user = await session.scalar(select(User).where(User.id == user_id))
                if not user:
                    logger.error(f"Failed to create user {user_id}")
                    continue
                
                # Process meeting as content
                meeting_time = self._parse_timestamp(meeting.start_timestamp).replace(tzinfo=None)
                last_update = self._parse_timestamp(meeting.last_finish).replace(tzinfo=None)
                meeting_name = f"{meeting_time.strftime('%H:%M')}"
                cutoff = cutoff.replace(tzinfo=None)
                
                # Check if content exists
                stmt = select(Content).where(and_(
                    Content.content_id == meeting_id,
                    Content.type == ContentType.MEETING.value
                ))
                existing_content = await session.scalar(stmt)
                
                if last_update > cutoff:
                    # Active meeting - just upsert, no queuing
                    if not existing_content:
                        existing_content = Content(
                            content_id=meeting_id,
                            type=ContentType.MEETING.value,
                            timestamp=meeting_time,
                            last_update=last_update,
                            is_indexed=False
                        )
                        session.add(existing_content)
                        await session.flush()  # Get content ID
                        
                        # Create title as child content
                        await update_child_content(
                            session,
                            meeting_id,
                            ContentType.TITLE.value,
                            meeting_name,
                            order=0
                        )
                    else:
                        existing_content.timestamp = meeting_time
                        existing_content.last_update = last_update
                        
                        # Update title
                        await update_child_content(
                            session,
                            meeting_id,
                            ContentType.TITLE.value,
                            meeting_name,
                            order=0
                        )
                else:
                    # Inactive meeting - ONLY queue if it's been inactive long enough
                    if not existing_content:
                        existing_content = Content(
                            content_id=meeting_id,
                            type=ContentType.MEETING.value,
                            timestamp=meeting_time,
                            last_update=last_update,
                            is_indexed=False
                        )
                        session.add(existing_content)
                        if last_update <= cutoff:
                            self._add_to_queue(str(meeting_id))
                    else:
                        # Check if it was previously active
                        active_score = self.redis.zscore(RedisKeys.ACTIVE_MEETINGS, str(meeting_id))
                        if active_score is not None:
                            existing_content.last_update = last_update
                            self._add_to_queue(str(meeting_id))
                
                # Handle UserContent association
                stmt = select(UserContent).where(and_(
                    UserContent.content_id == meeting_id,
                    UserContent.user_id == user_id
                ))
                user_content = await session.scalar(stmt)
                
                if not user_content:
                    user_content = UserContent(
                        content_id=meeting_id,
                        user_id=user_id,
                        created_at=meeting_time,
                        created_by=user_id,
                        is_owner=True,
                        access_level=AccessLevel.OWNER.value
                    )
                    session.add(user_content)
                
                # Handle speakers as entities
                for speaker_name in meeting.speakers:
                    # Check if entity exists
                    stmt = select(Entity).where(Entity.name == speaker_name)
                    entity = await session.scalar(stmt)
                    
                    if not entity:
                        entity = Entity(
                            name=speaker_name,
                            type='speaker'
                        )
                        session.add(entity)
                        await session.flush()  # Get entity ID
                    
                    # Check content-entity association
                    stmt = select(content_entity_association).where(and_(
                        content_entity_association.c.content_id == existing_content.id,
                        content_entity_association.c.entity_id == entity.id
                    ))
                    existing = await session.scalar(stmt)
                    
                    if not existing:
                        await session.execute(
                            content_entity_association.insert().values(
                                content_id=existing_content.id,
                                entity_id=entity.id
                            )
                        )
                
                await session.commit()

    async def _get_stored_max_timestamp(self, session) -> datetime:
        # Get max timestamp from Content table for meetings
        stmt = select(func.max(Content.last_update)).where(
            Content.type == ContentType.MEETING.value
        )
        result = await session.scalar(stmt)
        return result
    

    async def sync_meetings(self, overlap_minutes: int = 20):
        async with get_session() as session:
            cursor = await self._get_stored_max_timestamp(session)
            if cursor:
                cursor = cursor - timedelta(minutes=overlap_minutes)  # Small overlap to ensure no records are missed
            
            while True:
                meetings = await self.vexa_auth.get_speech_stats(
                    after_time=cursor,
                )
                
                if not meetings:
                    break
                    
                await self.upsert_meetings(meetings)
                
                if len(meetings) < 1000:  # API page size
                    break
                    
                # Update cursor to last updated_timestamp we've seen
                cursor = max(m.updated_timestamp for m in meetings)

    @log_execution 
    def get_queue_status(self):
        """Get current status of indexing queue"""
        now = datetime.now().timestamp()
        
        # Get queue stats
        queue_size = self.redis.zcard(RedisKeys.INDEXING_QUEUE)
        processing_count = self.redis.scard(RedisKeys.PROCESSING_SET)
        failed_count = self.redis.hlen(RedisKeys.FAILED_SET)
        seen_users = self.redis.scard(RedisKeys.SEEN_USERS)
        
        # Get ready to process items
        ready_to_process = self.redis.zrangebyscore(
            RedisKeys.INDEXING_QUEUE,
            0,
            now,
            withscores=True
        )
        ready_formatted = [
            {
                'content_id': cid.decode() if isinstance(cid, bytes) else cid,
                'retry_after': datetime.fromtimestamp(score).isoformat()
            }
            for cid, score in ready_to_process
        ]
        
        # Get delayed retry items
        delayed_retry = self.redis.zrangebyscore(
            RedisKeys.INDEXING_QUEUE,
            now,
            '+inf',
            withscores=True
        )
        delayed_formatted = [
            {
                'content_id': cid.decode() if isinstance(cid, bytes) else cid,
                'retry_after': datetime.fromtimestamp(score).isoformat()
            }
            for cid, score in delayed_retry
        ]
        
        # Get failures
        failures = [
            {
                'content_id': cid.decode() if isinstance(cid, bytes) else cid,
                'error': json.loads(error.decode() if isinstance(error, bytes) else error)
            }
            for cid, error in self.redis.hgetall(RedisKeys.FAILED_SET).items()
        ]
        
        # Get currently processing
        processing = [
            mid.decode() if isinstance(mid, bytes) else mid
            for mid in self.redis.smembers(RedisKeys.PROCESSING_SET)
        ]
        
        # Get active contents
        active_contents = self.redis.zrange(
            RedisKeys.ACTIVE_MEETINGS,
            0,
            -1,
            withscores=True
        )
        active_formatted = [
            {
                'content_id': cid.decode() if isinstance(cid, bytes) else cid,
                'started_at': datetime.fromtimestamp(score).isoformat()
            }
            for cid, score in active_contents
        ]
        
        return {
            'stats': {
                'queue_size': queue_size,
                'processing': processing_count,
                'failed': failed_count,
                'seen_users': seen_users,
                'active_contents': len(active_formatted),
                'timestamp': datetime.now().isoformat()
            },
            'ready_to_process': ready_formatted,
            'delayed_retry': delayed_formatted,
            'currently_processing': processing,
            'recent_failures': failures,
            'active_contents': active_formatted
        }

    @log_execution
    async def print_queue_status(self):
        """Pretty print queue status for monitoring"""
        status = self.get_queue_status()
        
        print("\n=== Queue Status ===")
        print(f"Time: {status['stats']['timestamp']}")
        print(f"Queue Size: {status['stats']['queue_size']}")
        print(f"Processing: {status['stats']['processing']}")
        print(f"Failed: {status['stats']['failed']}")
        print(f"Seen Users: {status['stats']['seen_users']}")
        print(f"Active Contents: {status['stats']['active_contents']}")
        
        print("\n=== Ready to Process ===")
        for content in status['ready_to_process']:
            print(f"Content {content['content_id']} - {content['retry_after']}")
        
        print("\n=== Delayed for Retry ===")
        for content in status['delayed_retry']:
            print(f"Content {content['content_id']} - retry after {content['retry_after']}")
        
        print("\n=== Currently Processing ===")
        for cid in status['currently_processing']:
            print(f"Processing: {cid}")
        
        if status['recent_failures']:
            print("\n=== Recent Failures ===")
            for failure in status['recent_failures']:
                error = failure['error']
                print(f"\nContent: {failure['content_id']}")
                print(f"Error: {error['error']}")
                print(f"Retries: {error['retry_count']}")
                print(f"Last Attempt: {error['last_attempt']}")
        
        print("\n=== Active Contents ===")
        for content in status['active_contents']:
            print(f"Content {content['content_id']} - last updated at {content['started_at']}")

    def flush_queues(self):
        """Flush all Redis queues and sets used for indexing"""
        keys_to_flush = [
            RedisKeys.INDEXING_QUEUE,    # Main queue
            RedisKeys.PROCESSING_SET,    # Currently processing
            RedisKeys.FAILED_SET,        # Failed meetings
            RedisKeys.SEEN_USERS,        # Tracked users
            RedisKeys.RETRY_ERRORS,      # Error tracking
            RedisKeys.LAST_CHECK         # Last check timestamp
        ]
        
        for key in keys_to_flush:
            self.redis.delete(key)
            logger.info(f"Flushed Redis key: {key}")
        
        logger.info("All indexing queues flushed")

    def flush_failed_meetings(self):
        """Flush only failed meetings and retry-related Redis keys"""
        keys_to_flush = [
            RedisKeys.FAILED_SET,        # Failed meetings
            RedisKeys.RETRY_ERRORS       # Error tracking
        ]
        
        for key in keys_to_flush:
            self.redis.delete(key)
            logger.info(f"Flushed Redis key: {key}")
        
        # Remove any delayed retries from the indexing queue
        now = datetime.now().timestamp()
        self.redis.zremrangebyscore(RedisKeys.INDEXING_QUEUE, now, '+inf')
        logger.info("Cleared delayed retries from indexing queue")