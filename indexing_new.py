from redis import Redis
import asyncio
from typing import Optional, Set
import json
from datetime import datetime, timedelta
import os
from vexa import VexaAPI, VexaAuth
from psql_models import Meeting, DiscussionPoint, async_session
from sqlalchemy import select, exists
import logging
from qdrant_search import QdrantSearchEngine
from pydantic_models import MeetingExtraction, EntityExtraction, MeetingNameAndSummary
from psql_models import Meeting, Speaker, DiscussionPoint, UserMeeting, AccessLevel
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
import json
from redis import Redis
from sqlalchemy import func, and_,distinct
from psql_models import UserToken, User
from datetime import datetime, timezone
import traceback


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisKeys:
    ACTIVE_MEETINGS = "active_meetings_set"
    INDEXING_QUEUE = "indexing_queue"
    PROCESSING_SET = "processing_set"
    FAILED_SET = "failed_set"
    SEEN_USERS = "seen_users_set"
    LAST_CHECK = "last_active_check"
    RETRY_ERRORS = "retry_errors"  # New key for storing retry errors

class ActiveMeetingsMonitor:
    def __init__(self, redis: Redis, vexa_auth: VexaAuth, check_interval: int = 5):
        self.redis = redis
        self.vexa_auth = vexa_auth
        self.check_interval = check_interval
        
    async def _is_meeting_indexed(self, meeting_id: str) -> bool:
        async with async_session() as session:
            query = select(exists().where(DiscussionPoint.meeting_id == meeting_id))
            result = await session.execute(query)
            return result.scalar()

    async def _process_active_meetings(self):
        try:
            # Get current active meetings
            active_meetings = await self.vexa_auth.get_active_meetings()
            current_active_set = set()
            
            # First process current active meetings and users
            for meeting in active_meetings:
                for session in meeting.get('sessions', []):
                    if session.get('finish_timestamp') is None:  # Only process active sessions
                        session_id = str(session['id'])
                        current_active_set.add(session_id)
                        self.redis.sadd(RedisKeys.ACTIVE_MEETINGS, session_id)
                        
                # Extract and store user IDs immediately for all meetings
                for user_id in meeting.get('user_ids', []):
                    self.redis.sadd(RedisKeys.SEEN_USERS, str(user_id))
                    logger.debug(f"Added user {user_id} to seen users for meeting {meeting['id']}")
            
            # Get previously known active meetings
            previous_active = self.redis.smembers(RedisKeys.ACTIVE_MEETINGS)
            
            # Find finished meetings (were active before but not now)
            finished_meetings = previous_active - current_active_set
            
            # Process finished meetings
            for meeting_id in finished_meetings:
                if not await self._is_meeting_indexed(meeting_id):
                    # Add to indexing queue with current timestamp as score
                    self.redis.zadd(RedisKeys.INDEXING_QUEUE, {meeting_id: datetime.now().timestamp()})
                    logger.info(f"Added finished meeting {meeting_id} to indexing queue")
                
                # Remove from active meetings set
                self.redis.srem(RedisKeys.ACTIVE_MEETINGS, meeting_id)
            
            # Update last check timestamp
            self.redis.set(RedisKeys.LAST_CHECK, datetime.now().isoformat())
            
        except Exception as e:
            logger.error(f"Error in processing active meetings: {e}")
            raise

    async def run(self):
        logger.info("Starting Active Meetings Monitor")
        while True:
            try:
                await self._process_active_meetings()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in Active Meetings Monitor: {e}")
                await asyncio.sleep(self.check_interval)

class UserMeetingsScanner:
    def __init__(
        self, 
        redis: Redis, 
        vexa_auth: VexaAuth, 
        scan_interval: int = 5,
        meetings_per_user: int = 10,
        max_depth: int = 20  # New parameter for max historical meetings to check
    ):
        self.redis = redis
        self.vexa_auth = vexa_auth
        self.scan_interval = scan_interval
        self.meetings_per_user = meetings_per_user
        self.max_depth = max_depth

    async def _process_user_meetings(self, user_id: str):
        try:
            async with async_session() as session:
                # First check token in database
                db_token = await session.execute(select(UserToken.token).where(UserToken.user_id == user_id))
                token = db_token.scalar_one_or_none()
                
                if not token:
                    # Get token from vexa_auth
                    token = await self.vexa_auth.get_user_token(user_id=user_id)
                    if not token:
                        logger.warning(f"No token found for user {user_id}")
                        return
                    
                    # Get user info using VexaAPI
                    vexa = VexaAPI(token=token)
                    user_info = await vexa.get_user_info()
                    
                    # Get or create user record
                    user_query = await session.execute(select(User).where(User.id == user_id))
                    user = user_query.scalar_one_or_none()
                    
                    # Create or update user record
                    if not user:
                        user = User(
                            id=user_id,
                            email=user_info.get('email', ''),
                            username=user_info.get('username', ''),
                            first_name=user_info.get('first_name', ''),
                            last_name=user_info.get('last_name', ''),
                            image=user_info.get('image', ''),
                            created_timestamp=datetime.now(timezone.utc)
                        )
                        session.add(user)
                    else:
                        user.email = user_info.get('email', user.email)
                        user.username = user_info.get('username', user.username)
                        user.first_name = user_info.get('first_name', user.first_name)
                        user.last_name = user_info.get('last_name', user.last_name)
                        user.image = user_info.get('image', user.image)
                        user.updated_timestamp = datetime.now(timezone.utc)
                    
                    # Prepare user_name for token
                    user_name = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}".strip()
                    if not user_name:
                        user_name = user_info.get('username', 'Unknown User')
                    
                    # Save new token to database
                    new_token = UserToken(
                        token=token,
                        user_id=user_id,
                        user_name=user_name,
                        created_at=datetime.now(timezone.utc),
                        last_used_at=datetime.now(timezone.utc)
                    )
                    session.add(new_token)
                    await session.commit()
                    logger.info(f"Saved new token and updated user info for user {user_id}")

            # Initialize VexaAPI with token
            vexa_api = VexaAPI(token=token)
            
            # Get all user meetings
            meetings = await vexa_api.get_meetings()
            
            # Store meetings in database before processing
            async with async_session() as session:
                for meeting in meetings:
                    meeting_id = str(meeting['id'])
                    # Check if meeting exists
                    existing = await session.execute(
                        select(Meeting).where(Meeting.meeting_id == meeting_id)
                    )
                    if not existing.scalar_one_or_none():
                        # Create basic meeting record
                        start_time = datetime.fromisoformat(meeting.get('start_timestamp', '').replace('Z', '+00:00'))
                        new_meeting = Meeting(
                            meeting_id=meeting_id,
                            timestamp=start_time.replace(tzinfo=None),
                            meeting_name=meeting.get('title', ''),
                        )
                        session.add(new_meeting)
                        
                        # Create owner UserMeeting record
                        user_meeting = UserMeeting(
                            meeting_id=meeting_id,
                            user_id=user_id,
                            access_level=AccessLevel.OWNER.value,
                            is_owner=True,
                            created_by=user_id
                        )
                        session.add(user_meeting)
                await session.commit()
            
            # Sort meetings by timestamp (newest first)
            meetings.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
            meetings = meetings[:self.max_depth]  # Apply max_depth limit
            
            # Track how many meetings we've added for this user
            meetings_added = 0
            
            # Process each meeting within max_depth limit
            for meeting in meetings:
                if meetings_added >= self.meetings_per_user:
                    break
                    
                meeting_id = str(meeting['id'])
                
                # Skip if meeting is currently active
                if self.redis.sismember(RedisKeys.ACTIVE_MEETINGS, meeting_id):
                    continue
                    
                # Skip if already indexed
                if await self._is_meeting_indexed(meeting_id):
                    continue
                    
                # Skip if already in queue or processing
                if (self.redis.zscore(RedisKeys.INDEXING_QUEUE, meeting_id) or 
                    self.redis.sismember(RedisKeys.PROCESSING_SET, meeting_id)):
                    continue
                
                # Add to indexing queue with priority based on recency
                score = datetime.now().timestamp()
                self.redis.zadd(RedisKeys.INDEXING_QUEUE, {meeting_id: score})
                logger.info(f"Added meeting {meeting_id} to indexing queue for user {user_id}")
                
                meetings_added += 1
                
            logger.info(f"Added {meetings_added} meetings for user {user_id} (checked {len(meetings)} recent meetings)")
                
        except Exception as e:
            logger.error(f"Error processing meetings for user {user_id}: {e}")

    async def _is_meeting_indexed(self, meeting_id: str) -> bool:
        async with async_session() as session:
            query = select(exists().where(DiscussionPoint.meeting_id == meeting_id))
            result = await session.execute(query)
            return result.scalar()

    async def run(self):
        logger.info("Starting User Meetings Scanner")
        while True:
            try:
                # Get all seen users
                users = self.redis.smembers(RedisKeys.SEEN_USERS)
                
                # Process each user's meetings
                for user_id in users:
                    await self._process_user_meetings(user_id)
                    # Add small delay between users to avoid overwhelming the system
                    await asyncio.sleep(1)
                
                await asyncio.sleep(self.scan_interval)
            except Exception as e:
                logger.error(f"Error in User Meetings Scanner: {e}")
                await asyncio.sleep(self.scan_interval)

class ProcessingError(Exception):
    """Custom exception for processing errors"""
    pass

class IndexingWorker:
    def __init__(
        self, 
        redis: Redis, 
        max_concurrent: int = 3, 
        process_interval: int = 5,
        max_retries: int = 3,
        retry_delay: int = 300  # 5 minutes
    ):
        self.redis = redis
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.process_interval = process_interval
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.qdrant = QdrantSearchEngine()

    def _get_retry_count(self, meeting_id: str) -> int:
        retry_key = f"meeting:retry:{meeting_id}"
        return int(self.redis.get(retry_key) or 0)

    def _increment_retry_count(self, meeting_id: str):
        retry_key = f"meeting:retry:{meeting_id}"
        self.redis.incr(retry_key)
        self.redis.expire(retry_key, 86400)  # 24 hour expiry

    async def _process_meeting_data(self, formatted_input: str, df: pd.DataFrame) -> tuple:
        extraction_tasks = [
            MeetingExtraction.extract(formatted_input),
            EntityExtraction.extract(formatted_input)
        ]
        discussion_points_df, topics_df = await asyncio.gather(*extraction_tasks)
        
        discussion_points_df['model'] = 'MeetingExtraction'
        topics_df['model'] = 'EntityExtraction'
        
        discussion_points_df = discussion_points_df.rename(columns={
            'item': 'topic_name', 
            'type': 'topic_type'
        })
        topics_df = topics_df.rename(columns={
            'entity': 'topic_name', 
            'type': 'topic_type'
        })
        
        points_df = pd.concat([discussion_points_df, topics_df]).reset_index(drop=True)
        points_df = points_df.reset_index().rename(columns={'index': 'summary_index'})
        
        final_df = points_df.groupby('summary_index').agg({
            'topic_name': 'first',
            'topic_type': 'first',
            'summary': 'first',
            'details': 'first',
            'speaker': 'first',
            'model': 'first'
        }).reset_index()
        
        meeting_name_and_summary = await MeetingNameAndSummary.extract(
            formatted_input, 
            final_df.drop(columns=['model']).to_markdown(index=False)
        )
        
        return final_df, meeting_name_and_summary

    async def _save_to_database(
        self, 
        final_df: pd.DataFrame, 
        meeting_id: UUID, 
        transcript: list,
        meeting_datetime: datetime,
        user_id: UUID,
        meeting_name_and_summary: dict,
        session: AsyncSession
    ):
        try:
            # Check for existing meeting
            existing_meeting = await session.execute(
                select(Meeting).where(Meeting.meeting_id == meeting_id)
            )
            existing_meeting = existing_meeting.scalar_one_or_none()

            if not existing_meeting:
                naive_datetime = meeting_datetime.replace(tzinfo=None) - meeting_datetime.utcoffset()
                new_meeting = Meeting(
                    meeting_id=meeting_id, 
                    transcript=str(transcript),
                    timestamp=naive_datetime,
                    meeting_name=meeting_name_and_summary.meeting_name,
                    meeting_summary=meeting_name_and_summary.summary
                )
                session.add(new_meeting)
                
                # Create owner UserMeeting record
                user_meeting = UserMeeting(
                    meeting_id=meeting_id,
                    user_id=user_id,
                    access_level=AccessLevel.OWNER.value,
                    is_owner=True,
                    created_by=user_id
                )
                session.add(user_meeting)
                await session.flush()
            else:
                existing_meeting.meeting_name = meeting_name_and_summary.meeting_name
                existing_meeting.meeting_summary = meeting_name_and_summary.summary
                new_meeting = existing_meeting

            # Handle speakers
            unique_speakers = final_df['speaker'].unique()
            existing_speakers = {
                s.name: s for s in (await session.execute(
                    select(Speaker).where(Speaker.name.in_(unique_speakers))
                )).scalars()
            }
            
            new_speakers = []
            for speaker_name in unique_speakers:
                if speaker_name not in existing_speakers:
                    new_speaker = Speaker(name=speaker_name)
                    new_speakers.append(new_speaker)
                    existing_speakers[speaker_name] = new_speaker
            
            if new_speakers:
                session.add_all(new_speakers)
                await session.flush()

            # Create discussion points
            discussion_points = [
                DiscussionPoint(
                    summary_index=row['summary_index'],
                    summary=row['summary'],
                    details=row['details'],
                    meeting_id=new_meeting.meeting_id,
                    speaker_id=existing_speakers[row['speaker']].id,
                    topic_name=row['topic_name'],
                    topic_type=row['topic_type'],
                    model=row['model']
                )
                for _, row in final_df.iterrows()
            ]
            session.add_all(discussion_points)
            await session.commit()
            
            return new_meeting
            
        except Exception as e:
            await session.rollback()
            raise ProcessingError(f"Database error: {str(e)}")

    async def _process_meeting(self, meeting_id: str):
        """Process a single meeting"""
        processing_key = f"processing:{meeting_id}"
        
        try:
            # Mark as processing
            self.redis.sadd(RedisKeys.PROCESSING_SET, meeting_id)
            
            # Get user token for this meeting
            token = await self._get_meeting_token(meeting_id)
            if not token:
                raise ProcessingError("Could not find token for meeting")

            # Initialize VexaAPI
            vexa_api = VexaAPI(token=token)
            await vexa_api.get_user_info()
            user_id = vexa_api.user_id

            # Get transcription
            transcription = await vexa_api.get_transcription(
                meeting_session_id=meeting_id, 
                use_index=True
            )
            
            if not transcription:
                raise ProcessingError("No transcription available")

            df, formatted_input, start_datetime, speakers, transcript = transcription
            
            # Process meeting data
            final_df, meeting_name_and_summary = await asyncio.wait_for(
                self._process_meeting_data(formatted_input, df),
                timeout=60
            )

            # Save to database and update Qdrant
            async with async_session() as session:
                meeting = await self._save_to_database(
                    final_df=final_df,
                    meeting_id=meeting_id,
                    transcript=transcript,
                    meeting_datetime=start_datetime,
                    user_id=user_id,
                    meeting_name_and_summary=meeting_name_and_summary,
                    session=session
                )
                
                # Update Qdrant index
                await self.qdrant.sync_meeting(meeting_id, session)

            # Remove from queues on success
            self.redis.zrem(RedisKeys.INDEXING_QUEUE, meeting_id)
            self.redis.srem(RedisKeys.PROCESSING_SET, meeting_id)
            self.redis.delete(f"meeting:retry:{meeting_id}")
            
            logger.info(f"Successfully processed meeting {meeting_id}")

        except Exception as e:
            logger.error(f"Error processing meeting {meeting_id}: {e}")
            error_data = {
                "error": str(e),
                "timestamp": datetime.now().isoformat(),
                "traceback": traceback.format_exc()
            }
            
            retry_count = self._get_retry_count(meeting_id)
            if retry_count >= self.max_retries:
                # Move to failed set with detailed error info
                error_data.update({
                    "retry_count": retry_count,
                    "final_failure": True
                })
                self.redis.hset(RedisKeys.FAILED_SET, meeting_id, json.dumps(error_data))
                self.redis.zrem(RedisKeys.INDEXING_QUEUE, meeting_id)
                logger.error(f"Meeting {meeting_id} failed permanently after {retry_count} retries")
            else:
                # Store retry error and schedule next attempt
                self._increment_retry_count(meeting_id)
                error_data.update({
                    "retry_count": retry_count + 1,
                    "next_retry": datetime.now().timestamp() + self.retry_delay
                })
                self.redis.hset(RedisKeys.RETRY_ERRORS, meeting_id, json.dumps(error_data))
                new_score = datetime.now().timestamp() + self.retry_delay
                self.redis.zadd(RedisKeys.INDEXING_QUEUE, {meeting_id: new_score})
                logger.warning(f"Scheduled retry {retry_count + 1} for meeting {meeting_id}")
        
        finally:
            self.redis.srem(RedisKeys.PROCESSING_SET, meeting_id)

    async def _get_meeting_token(self, meeting_id: str) -> Optional[str]:
        """Get the appropriate token for processing a meeting"""
        async with async_session() as session:
            # Get owner's token for the meeting
            query = select(UserToken.token)\
                .join(UserMeeting, UserToken.user_id == UserMeeting.user_id)\
                .where(
                    and_(
                        UserMeeting.meeting_id == meeting_id,
                        UserMeeting.is_owner == True
                    )
                )
            result = await session.execute(query)
            return result.scalar_one_or_none()

    async def run(self):
        """Main worker loop"""
        logger.info("Starting Indexing Worker")
        while True:
            try:
                # Get next meeting from queue (newest first)
                next_meetings = self.redis.zrevrange(
                    RedisKeys.INDEXING_QUEUE, 
                    0, 
                    0, 
                    withscores=True
                )
                
                if next_meetings:
                    meeting_id, score = next_meetings[0]
                    
                    # Skip if scheduled for future (retry delay)
                    if score > datetime.now().timestamp():
                        await asyncio.sleep(self.process_interval)
                        continue
                    
                    # Skip if already being processed
                    if self.redis.sismember(RedisKeys.PROCESSING_SET, meeting_id):
                        continue
                    
                    async with self.semaphore:
                        await self._process_meeting(meeting_id)
                
                await asyncio.sleep(self.process_interval)
            except Exception as e:
                logger.error(f"Error in Indexing Worker main loop: {e}")
                await asyncio.sleep(self.process_interval)

class QueueStats:
    def __init__(self, 
        active_meetings: set,
        queued_meetings: list,
        processing_meetings: set,
        failed_meetings: dict,
        seen_users: set,
        retry_counts: dict,
        retry_errors: dict,
        last_check: str
    ):
        self.active_meetings = active_meetings
        self.queued_meetings = queued_meetings
        self.processing_meetings = processing_meetings
        self.failed_meetings = failed_meetings
        self.seen_users = seen_users
        self.retry_counts = retry_counts
        self.retry_errors = retry_errors
        self.last_check = last_check

    def to_dict(self):
        return {
            "active_meetings": {
                "count": len(self.active_meetings),
                "items": list(self.active_meetings)
            },
            "queued_meetings": {
                "count": len(self.queued_meetings),
                "items": [
                    {"id": mid, "scheduled_time": datetime.fromtimestamp(score).isoformat()}
                    for mid, score in self.queued_meetings
                ]
            },
            "processing_meetings": {
                "count": len(self.processing_meetings),
                "items": list(self.processing_meetings)
            },
            "failed_meetings": {
                "count": len(self.failed_meetings),
                "items": self.failed_meetings
            },
            "seen_users": {
                "count": len(self.seen_users),
                "items": list(self.seen_users)
            },
            "retry_counts": self.retry_counts,
            "retry_errors": self.retry_errors,
            "last_active_check": self.last_check
        }

class IndexingPipeline:
    def __init__(self, initialize_worker: bool = True):
        self.redis = Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=0,
            decode_responses=True
        )
        self.vexa_auth = VexaAuth()
        
        self.monitor = ActiveMeetingsMonitor(self.redis, self.vexa_auth)
        self.scanner = UserMeetingsScanner(self.redis, self.vexa_auth)
        # Only initialize worker if needed (it loads the embedding model)
        self.worker = IndexingWorker(self.redis) if initialize_worker else None

    async def start(self):
        if not self.worker:
            raise RuntimeError("Worker not initialized. Create IndexingPipeline with initialize_worker=True")
        logger.info("Starting Indexing Pipeline")
        await asyncio.gather(
            self.monitor.run(),
            self.scanner.run(),
            self.worker.run()
        )

    def reset_queues(self):
        """Reset all Redis queues"""
        keys = [
            RedisKeys.ACTIVE_MEETINGS,
            RedisKeys.INDEXING_QUEUE,
            RedisKeys.PROCESSING_SET,
            RedisKeys.FAILED_SET,
            RedisKeys.SEEN_USERS,
            RedisKeys.LAST_CHECK
        ]
        self.redis.delete(*keys)
        return {"status": "success", "message": "All queues have been reset"}

    def get_queue_stats(self) -> QueueStats:
        """Get current status of all queues"""
        # Get active meetings
        active_meetings = self.redis.smembers(RedisKeys.ACTIVE_MEETINGS)
        
        # Get queued meetings with scores
        queued_meetings = self.redis.zrange(
            RedisKeys.INDEXING_QUEUE, 
            0, 
            -1, 
            withscores=True
        )
        
        # Get processing meetings
        processing_meetings = self.redis.smembers(RedisKeys.PROCESSING_SET)
        
        # Get failed meetings with error details
        failed_meetings = {}
        for meeting_id in self.redis.hkeys(RedisKeys.FAILED_SET):
            error_data = self.redis.hget(RedisKeys.FAILED_SET, meeting_id)
            failed_meetings[meeting_id] = json.loads(error_data)
        
        # Get seen users
        seen_users = self.redis.smembers(RedisKeys.SEEN_USERS)
        
        # Get retry counts
        retry_keys = self.redis.keys("meeting:retry:*")
        retry_counts = {}
        for key in retry_keys:
            meeting_id = key.split(":")[-1]
            count = self.redis.get(key)
            retry_counts[meeting_id] = int(count)
        
        # Get retry errors
        retry_errors = {}
        for meeting_id in self.redis.hkeys(RedisKeys.RETRY_ERRORS):
            error_data = self.redis.hget(RedisKeys.RETRY_ERRORS, meeting_id)
            retry_errors[meeting_id] = json.loads(error_data)
        
        # Get last check timestamp
        last_check = self.redis.get(RedisKeys.LAST_CHECK)

        return QueueStats(
            active_meetings=active_meetings,
            queued_meetings=queued_meetings,
            processing_meetings=processing_meetings,
            failed_meetings=failed_meetings,
            seen_users=seen_users,
            retry_counts=retry_counts,
            retry_errors=retry_errors,
            last_check=last_check
        )

    async def get_detailed_stats(self) -> dict:
        """Get detailed statistics including database info"""
        queue_stats = self.get_queue_stats()
        
        async with async_session() as session:
            # Get total meetings in database
            total_meetings_query = select(func.count(distinct(Meeting.meeting_id)))
            total_meetings = await session.scalar(total_meetings_query)
            
            # Get total indexed meetings (with discussion points)
            indexed_meetings_query = select(
                func.count(distinct(Meeting.meeting_id))
            ).join(DiscussionPoint)
            indexed_meetings = await session.scalar(indexed_meetings_query)
            
            # Get total users
            total_users_query = select(func.count(distinct(UserMeeting.user_id)))
            total_users = await session.scalar(total_users_query)

        return {
            "queue_stats": queue_stats.to_dict(),
            "database_stats": {
                "total_meetings": total_meetings,
                "indexed_meetings": indexed_meetings,
                "total_users": total_users,
                "indexing_progress": f"{(indexed_meetings/total_meetings)*100:.2f}%" if total_meetings else "0%"
            }
        }

    async def retry_failed_meetings(self) -> dict:
        """Retry all failed meetings by moving them back to the indexing queue"""
        failed_meetings = self.redis.hkeys(RedisKeys.FAILED_SET)
        count = 0
        
        for meeting_id in failed_meetings:
            # Reset retry count
            self.redis.delete(f"meeting:retry:{meeting_id}")
            
            # Add back to indexing queue
            self.redis.zadd(RedisKeys.INDEXING_QUEUE, {
                meeting_id: datetime.now().timestamp()
            })
            
            # Remove from failed set
            self.redis.hdel(RedisKeys.FAILED_SET, meeting_id)
            count += 1
        
        return {
            "status": "success",
            "retried_meetings": count,
            "message": f"Moved {count} meetings back to indexing queue"
        }

    async def remove_meeting(self, meeting_id: str) -> dict:
        """Remove a meeting from all queues"""
        removed_from = []
        
        if self.redis.srem(RedisKeys.ACTIVE_MEETINGS, meeting_id):
            removed_from.append("active_meetings")
            
        if self.redis.zrem(RedisKeys.INDEXING_QUEUE, meeting_id):
            removed_from.append("indexing_queue")
            
        if self.redis.srem(RedisKeys.PROCESSING_SET, meeting_id):
            removed_from.append("processing_set")
            
        if self.redis.hdel(RedisKeys.FAILED_SET, meeting_id):
            removed_from.append("failed_set")
            
        # Clean up retry counter
        self.redis.delete(f"meeting:retry:{meeting_id}")
        
        return {
            "status": "success",
            "meeting_id": meeting_id,
            "removed_from": removed_from
        }