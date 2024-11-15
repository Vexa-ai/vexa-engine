from datetime import datetime, timezone, timedelta
from sqlalchemy import select, and_, func
from psql_models import User, Meeting, UserMeeting, UserToken, Speaker, meeting_speaker_association
from psql_helpers import get_session
from vexa import VexaAuth, VexaAPI
from redis import Redis
from typing import List
import logging
from functools import wraps
import asyncio
import json
from .redis_keys import RedisKeys

logger = logging.getLogger(__name__)

import os
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
        self.active_seconds = 30
    

    def _add_to_queue(self, meeting_id: str, score: float = None):
        if score is None:
            score = datetime.now().timestamp()
        self.redis.zadd(RedisKeys.INDEXING_QUEUE, {meeting_id: score})
        logger.debug(f"Queued meeting {meeting_id}")


    async def _queue_user_meetings(self, user_id: str, session, days: int = 30):
        # Create naive datetime for PostgreSQL
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).replace(tzinfo=None)
        stmt = select(Meeting.meeting_id).join(UserMeeting)\
            .where(and_(
                UserMeeting.user_id == user_id,
                Meeting.timestamp >= cutoff,
                Meeting.is_indexed == False
            ))
        meetings = await session.execute(stmt)
        for (meeting_id,) in meetings:
            self._add_to_queue(str(meeting_id))


    async def sync_meetings_queue(self):
        async with get_session() as session:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            cutoff = (now - timedelta(seconds=self.active_seconds))
            hour_ago = (now - timedelta(hours=1))
            
            # Get meetings between 5min and 1hr old
            stmt = select(Meeting.meeting_id, UserMeeting.user_id)\
                .join(UserMeeting)\
                .where(and_(
                    Meeting.timestamp.between(hour_ago, cutoff),
                    Meeting.is_indexed == False,
                    UserMeeting.is_owner == True
                ))
            older_meetings = await session.execute(stmt)
            for meeting_id, _ in older_meetings:
                self._add_to_queue(str(meeting_id))
            
            # Handle recent meetings and their users
            stmt = select(Meeting.meeting_id, UserMeeting.user_id)\
                .join(UserMeeting)\
                .where(and_(
                    Meeting.timestamp > cutoff,
                    Meeting.is_indexed == False
                ))
            recent_meetings = await session.execute(stmt)
            
            # Track active meetings in Redis
            pipe = self.redis.pipeline()
            pipe.delete(RedisKeys.ACTIVE_MEETINGS)
            for meeting_id, user_id in recent_meetings:
                pipe.zadd(RedisKeys.ACTIVE_MEETINGS, {str(meeting_id): now.timestamp()})
                if not self.redis.sismember(RedisKeys.SEEN_USERS, str(user_id)):
                    await self._queue_user_meetings(str(user_id), session)
                    self.redis.sadd(RedisKeys.SEEN_USERS, str(user_id))
            pipe.execute()


    async def _ensure_user_exists(self, user_id: str, session) -> None:
        # Check if user exists
        stmt = select(User).where(User.id == user_id)
        user = await session.scalar(stmt)
        
        if not user:
            # Check for existing token first
            token_stmt = select(UserToken.token).where(UserToken.user_id == user_id)
            token = await session.scalar(token_stmt)
            
            if not token:
                # Get new token if none exists
                token = await self.vexa_auth.get_user_token(user_id=user_id)
            
            if token:
                vexa = VexaAPI(token=token)
                user_info = await vexa.get_user_info()
                
                # Create new user
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
                
                # Create token record if it's new
                if not await session.scalar(token_stmt):
                    token_record = UserToken(
                        token=token,
                        user_id=user_id,
                        created_at=datetime.now(timezone.utc)
                    )
                    session.add(token_record)
                
                await session.flush()


    def _parse_timestamp(self, timestamp: str) -> datetime:
        # Handle both formats: with and without microseconds
        try:
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            return datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%SZ')


    async def upsert_meetings(self, meetings_data: list):
        async with get_session() as session:
            cutoff = datetime.utcnow() - timedelta(seconds=self.active_seconds)
            
            for meeting_id, user_id, timestamp, speakers in meetings_data:
                # Ensure user exists before creating meeting
                await self._ensure_user_exists(user_id, session)
                
                # Parse timestamp
                meeting_time = self._parse_timestamp(timestamp)
                # Format: "Call - Jan 15, 2024 at 14:30"
                meeting_name = f"{meeting_time.strftime('%H:%M')}"
                
                # Check if meeting exists
                stmt = select(Meeting).where(Meeting.meeting_id == meeting_id)
                meeting = await session.scalar(stmt)
                
                if meeting_time > cutoff:
                    # Active meeting - update or create
                    if not meeting:
                        meeting = Meeting(
                            meeting_id=meeting_id,
                            timestamp=meeting_time,
                            meeting_name=meeting_name
                        )
                        session.add(meeting)
                    else:
                        meeting.timestamp = meeting_time
                        meeting.meeting_name = meeting_name
                else:
                    # Inactive meeting - only create if doesn't exist
                    if not meeting:
                        meeting = Meeting(
                            meeting_id=meeting_id,
                            timestamp=meeting_time,
                            meeting_name=meeting_name
                        )
                        session.add(meeting)
                
                # Handle UserMeeting association
                stmt = select(UserMeeting).where(
                    and_(
                        UserMeeting.meeting_id == meeting_id,
                        UserMeeting.user_id == user_id
                    )
                )
                user_meeting = await session.scalar(stmt)
                
                if not user_meeting:
                    user_meeting = UserMeeting(
                        meeting_id=meeting_id,
                        user_id=user_id,
                        created_at=meeting_time,
                        created_by=user_id,
                        is_owner=True,
                        access_level='search'
                    )
                    session.add(user_meeting)
                
                # Handle speakers
                for speaker_name in speakers:
                    # Get or create speaker
                    stmt = select(Speaker).where(Speaker.name == speaker_name)
                    speaker = await session.scalar(stmt)
                    if not speaker:
                        speaker = Speaker(name=speaker_name)
                        session.add(speaker)
                        await session.flush()  # To get speaker.id
                    
                    # Associate speaker with meeting if not already associated
                    stmt = select(meeting_speaker_association).where(
                        and_(
                            meeting_speaker_association.c.meeting_id == meeting.id,
                            meeting_speaker_association.c.speaker_id == speaker.id
                        )
                    )
                    existing = await session.scalar(stmt)
                    if not existing:
                        await session.execute(
                            meeting_speaker_association.insert().values(
                                meeting_id=meeting.id,
                                speaker_id=speaker.id
                            )
                        )
                
                await session.commit()

    async def _get_stored_max_timestamp(self, session) -> datetime:
        stmt = select(func.max(Meeting.timestamp))
        result = await session.scalar(stmt)
        return result
    

    async def sync_meetings(self):
        async with get_session() as session:
            max_timestamp = await self._get_stored_max_timestamp(session)
            meetings = await self.vexa_auth.get_speech_stats(max_timestamp)
            await self.upsert_meetings(meetings)

    def get_queue_status(self) -> dict:
        now = datetime.now().timestamp()
        
        # Get queue statistics
        queue_size = self.redis.zcard(RedisKeys.INDEXING_QUEUE)
        processing_count = self.redis.scard(RedisKeys.PROCESSING_SET)
        failed_count = self.redis.hlen(RedisKeys.FAILED_SET)
        seen_users = self.redis.scard(RedisKeys.SEEN_USERS)
        
        # Get next meetings to process (score <= now)
        ready_to_process = self.redis.zrangebyscore(
            RedisKeys.INDEXING_QUEUE, 
            '-inf', 
            now,
            start=0,
            num=5,
            withscores=True
        )
        ready_formatted = [
            {
                'meeting_id': mid.decode() if isinstance(mid, bytes) else mid,
                'retry_after': datetime.fromtimestamp(score).isoformat() if score > now else 'ready'
            }
            for mid, score in ready_to_process
        ]
        
        # Get delayed meetings (score > now)
        delayed = self.redis.zrangebyscore(
            RedisKeys.INDEXING_QUEUE, 
            now,
            '+inf',
            start=0,
            num=5,
            withscores=True
        )
        delayed_formatted = [
            {
                'meeting_id': mid.decode() if isinstance(mid, bytes) else mid,
                'retry_after': datetime.fromtimestamp(score).isoformat()
            }
            for mid, score in delayed
        ]
        
        # Get recent failures
        failed_meetings = self.redis.hgetall(RedisKeys.FAILED_SET)
        failures = [
            {
                'meeting_id': mid.decode() if isinstance(mid, bytes) else mid,
                'error': json.loads(error.decode() if isinstance(error, bytes) else error)
            }
            for mid, error in failed_meetings.items()
        ][:5]
        
        # Currently processing
        processing = [
            mid.decode() if isinstance(mid, bytes) else mid
            for mid in self.redis.smembers(RedisKeys.PROCESSING_SET)
        ]
        
        # Get active meetings
        active_meetings = self.redis.zrange(
            RedisKeys.ACTIVE_MEETINGS,
            0,
            -1,
            withscores=True
        )
        active_formatted = [
            {
                'meeting_id': mid.decode() if isinstance(mid, bytes) else mid,
                'started_at': datetime.fromtimestamp(score).isoformat()
            }
            for mid, score in active_meetings
        ]
        
        return {
            'stats': {
                'queue_size': queue_size,
                'processing': processing_count,
                'failed': failed_count,
                'seen_users': seen_users,
                'active_meetings': len(active_formatted),
                'timestamp': datetime.now().isoformat()
            },
            'ready_to_process': ready_formatted,
            'delayed_retry': delayed_formatted,
            'currently_processing': processing,
            'recent_failures': failures,
            'active_meetings': active_formatted
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
        print(f"Active Meetings: {status['stats']['active_meetings']}")
        
        print("\n=== Ready to Process ===")
        for meeting in status['ready_to_process']:
            print(f"Meeting {meeting['meeting_id']} - {meeting['retry_after']}")
        
        print("\n=== Delayed for Retry ===")
        for meeting in status['delayed_retry']:
            print(f"Meeting {meeting['meeting_id']} - retry after {meeting['retry_after']}")
        
        print("\n=== Currently Processing ===")
        for mid in status['currently_processing']:
            print(f"Processing: {mid}")
        
        if status['recent_failures']:
            print("\n=== Recent Failures ===")
            for failure in status['recent_failures']:
                error = failure['error']
                print(f"\nMeeting: {failure['meeting_id']}")
                print(f"Error: {error['error']}")
                print(f"Retries: {error['retry_count']}")
                print(f"Last Attempt: {error['last_attempt']}")
        
        print("\n=== Active Meetings ===")
        for meeting in status['active_meetings']:
            print(f"Meeting {meeting['meeting_id']} - started at {meeting['started_at']}")

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