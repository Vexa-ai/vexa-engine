from redis import Redis
import asyncio
from typing import Optional
from datetime import datetime, timezone
import os
from vexa import VexaAPI, VexaAuth
from psql_models import (Meeting, DiscussionPoint, async_session, Speaker, 
                        UserMeeting, AccessLevel, UserToken, User)
from sqlalchemy import select, exists, and_, func, distinct, cast
from sqlalchemy.dialects.postgresql import UUID
from qdrant_search import QdrantSearchEngine
from pydantic_models import MeetingExtraction, EntityExtraction, MeetingNameAndSummary
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
import json

class RedisKeys:
    INDEXING_QUEUE = "indexing_queue"
    PROCESSING_SET = "processing_set"
    FAILED_SET = "failed_set"
    SEEN_USERS = "seen_users_set"
    LAST_CHECK = "last_active_check"
    RETRY_ERRORS = "retry_errors"

class ActiveMeetingsMonitor:
    def __init__(self, redis: Redis, vexa_auth: VexaAuth, check_interval: int = 5):
        self.redis = redis
        self.vexa_auth = vexa_auth
        self.check_interval = check_interval

    async def _collect_active_users(self):
        active_meetings = await self.vexa_auth.get_active_meetings()
        for meeting in active_meetings:
            for user_id in meeting.get('user_ids', []):
                self.redis.sadd(RedisKeys.SEEN_USERS, str(user_id))

    async def run(self):
        while True:
            try:
                await self._collect_active_users()
                await asyncio.sleep(self.check_interval)
            except Exception:
                await asyncio.sleep(self.check_interval)

class MeetingsScanner:
    def __init__(self, redis: Redis, vexa_auth: VexaAuth, scan_interval: int = 5):
        self.redis = redis
        self.vexa_auth = vexa_auth
        self.scan_interval = scan_interval

    async def _get_or_create_user_token(self, user_id: str) -> str:
        async with async_session() as session:
            token = (await session.execute(
                select(UserToken.token).where(UserToken.user_id == user_id)
            )).scalar_one_or_none()
            if token:
                return token

            token = await self.vexa_auth.get_user_token(user_id=user_id)
            if not token:
                return None

            # Store user info
            vexa = VexaAPI(token=token)
            user_info = await vexa.get_user_info()
            
            # Create user record
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

            # Create token record
            user_name = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}".strip() or user_info.get('username', 'Unknown User')
            new_token = UserToken(
                token=token,
                user_id=user_id,
                user_name=user_name,
                created_at=datetime.now(timezone.utc)
            )
            session.add(new_token)
            await session.commit()
            return token

    async def _scan_user_meetings(self, user_id: str):
        token = await self._get_or_create_user_token(user_id)
        if not token:
            return

        vexa_api = VexaAPI(token=token)
        meetings = await vexa_api.get_meetings()
        
        # Store meetings in database first
        async with async_session() as session:
            # Get user info for new tokens
            user_info = await vexa_api.get_user_info()
            
            # Create or update user record
            user = User(
                id=user_id,
                email=user_info.get('email', ''),
                username=user_info.get('username', ''),
                first_name=user_info.get('first_name', ''),
                last_name=user_info.get('last_name', ''),
                image=user_info.get('image', ''),
                created_timestamp=datetime.now(timezone.utc)
            )
            await session.merge(user)
            
            # Create token record if doesn't exist
            token_query = await session.execute(
                select(UserToken).where(UserToken.user_id == user_id)
            )
            if not token_query.scalar_one_or_none():
                user_name = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}".strip()
                if not user_name:
                    user_name = user_info.get('username', 'Unknown User')
                
                new_token = UserToken(
                    token=token,
                    user_id=user_id,
                    user_name=user_name,
                    created_at=datetime.now(timezone.utc),
                    last_used_at=datetime.now(timezone.utc)
                )
                session.add(new_token)

            # Store meetings in database
            for meeting in meetings:
                meeting_id = str(meeting['id'])
                # Check if meeting exists
                existing = await session.execute(
                    select(Meeting).where(Meeting.meeting_id == meeting_id)
                )
                if not existing.scalar_one_or_none():
                    start_time = datetime.fromisoformat(meeting.get('start_timestamp', '').replace('Z', '+00:00'))
                    new_meeting = Meeting(
                        meeting_id=meeting_id,
                        timestamp=start_time.replace(tzinfo=None),
                        meeting_name=meeting.get('title', '')
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
        
        # Process only finished meetings, sort by finish_timestamp descending
        finished_meetings = [
            meeting for meeting in meetings 
            if meeting.get('finish_timestamp')
        ]
        finished_meetings.sort(
            key=lambda x: x.get('finish_timestamp', ''), 
            reverse=True
        )
        
        # Queue finished meetings for processing
        for meeting in finished_meetings[:10]:
            meeting_id = str(meeting['id'])
            # Skip if already processing or failed
            if not (self.redis.sismember(RedisKeys.PROCESSING_SET, meeting_id) or 
                   self.redis.hexists(RedisKeys.FAILED_SET, meeting_id)):
                self.redis.zadd(RedisKeys.INDEXING_QUEUE, {
                    meeting_id: datetime.now().timestamp()
                })

    async def run(self):
        while True:
            try:
                # Get all users from SEEN_USERS set
                seen_users = self.redis.smembers(RedisKeys.SEEN_USERS)
                for user_id in seen_users:
                    await self._scan_user_meetings(user_id)
                await asyncio.sleep(self.scan_interval)
            except Exception:
                await asyncio.sleep(self.scan_interval)

class ProcessingError(Exception): pass

class IndexingWorker:
    def __init__(self, redis: Redis, max_concurrent: int = 3, retry_delay: int = 300):
        self.redis = redis
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.retry_delay = retry_delay
        self.max_retries = 3
        self.qdrant = QdrantSearchEngine()

    async def _process_meeting_data(self, formatted_input: str, df: pd.DataFrame) -> tuple:
        # Extract meeting data using AI models
        discussion_points_df, topics_df = await asyncio.gather(
            MeetingExtraction.extract(formatted_input),
            EntityExtraction.extract(formatted_input)
        )
        
        # Prepare and merge dataframes
        discussion_points_df['model'] = 'MeetingExtraction'
        topics_df['model'] = 'EntityExtraction'
        
        for df, name_col in [(discussion_points_df, 'item'), (topics_df, 'entity')]:
            df.rename(columns={name_col: 'topic_name', 'type': 'topic_type'}, inplace=True)
        
        points_df = pd.concat([discussion_points_df, topics_df]).reset_index(drop=True)
        points_df = points_df.reset_index().rename(columns={'index': 'summary_index'})
        
        final_df = points_df.groupby('summary_index').agg({
            'topic_name': 'first', 'topic_type': 'first', 'summary': 'first',
            'details': 'first', 'speaker': 'first', 'model': 'first'
        }).reset_index()
        
        meeting_summary = await MeetingNameAndSummary.extract(
            formatted_input, 
            final_df.drop(columns=['model']).to_markdown(index=False)
        )
        
        return final_df, meeting_summary
    async def _save_to_database(self, final_df: pd.DataFrame, meeting_id: UUID, 
                              transcript: list, meeting_datetime: datetime, 
                              user_id: UUID, meeting_summary: dict, session: AsyncSession):
        try:
            # Get or create meeting
            meeting = await self._get_or_create_meeting(
                meeting_id, transcript, meeting_datetime, 
                meeting_summary, user_id, session
            )
            
            # Handle speakers
            speakers = await self._handle_speakers(final_df['speaker'].unique(), session)
            
            # Create discussion points
            points = [
                DiscussionPoint(
                    summary_index=row['summary_index'],
                    summary=row['summary'],
                    details=row['details'],
                    meeting_id=meeting.meeting_id,
                    speaker_id=speakers[row['speaker']].id,
                    topic_name=row['topic_name'],
                    topic_type=row['topic_type'],
                    model=row['model']
                )
                for _, row in final_df.iterrows()
            ]
            session.add_all(points)
            await session.commit()
            return meeting
            
        except Exception as e:
            await session.rollback()
            raise ProcessingError(f"Database error: {str(e)}")

    async def _process_meeting(self, meeting_id: str):
        self.redis.sadd(RedisKeys.PROCESSING_SET, meeting_id)
   #     try:
        #Get meeting data
        token = await self._get_meeting_token(meeting_id)
        if not token:
            raise ProcessingError("No token found")

        vexa_api = VexaAPI(token=token)
        user_id = (await vexa_api.get_user_info())['id']
        transcription = await vexa_api.get_transcription(meeting_session_id=meeting_id, use_index=True)
        
        if not transcription:
            raise ProcessingError("No transcription")

        # Process and save
        df, formatted_input, start_time, _, transcript = transcription
        final_df, meeting_summary = await self._process_meeting_data(formatted_input, df)
        
        async with async_session() as session:
            meeting = await self._save_to_database(
                final_df, meeting_id, transcript, start_time,
                user_id, meeting_summary, session
            )
            await self.qdrant.sync_meeting(meeting_id, session)
        
        self._cleanup_success(meeting_id)

        # except Exception as e:
        #     await self._handle_error(meeting_id, e)
    #    finally:
        self.redis.srem(RedisKeys.PROCESSING_SET, meeting_id)

    async def run(self):
        while True:
            try:
                next_meetings = self.redis.zrevrange(RedisKeys.INDEXING_QUEUE, 0, 0, withscores=True)
                if next_meetings:
                    meeting_id, score = next_meetings[0]
                    if score > datetime.now().timestamp() or \
                       self.redis.sismember(RedisKeys.PROCESSING_SET, meeting_id):
                        continue
                    
                    async with self.semaphore:
                        await self._process_meeting(meeting_id)
                        
                await asyncio.sleep(5)
            except Exception:
                await asyncio.sleep(5)

    async def _get_meeting_token(self, meeting_id: str) -> Optional[str]:
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
            token = result.scalar_one_or_none()
            
            if not token:
                # Get user_id from UserMeeting
                user_query = select(UserMeeting.user_id)\
                    .where(
                        and_(
                            UserMeeting.meeting_id == meeting_id,
                            UserMeeting.is_owner == True
                        )
                    )
                user_result = await session.execute(user_query)
                user_id = user_result.scalar_one_or_none()
                print(user_id)
                if user_id:
                    # Try to get token from vexa_auth
                    token = await self.vexa_auth.get_user_token(user_id=user_id)
                    if token:
                        # Get user info using VexaAPI
                        vexa = VexaAPI(token=token)
                        user_info = await vexa.get_user_info()
                        
                        # Create token record
                        user_name = f"{user_info.get('first_name', '')} {user_info.get('last_name', '')}".strip()
                        if not user_name:
                            user_name = user_info.get('username', 'Unknown User')
                        
                        new_token = UserToken(
                            token=token,
                            user_id=user_id,
                            user_name=user_name,
                            created_at=datetime.now(timezone.utc),
                            last_used_at=datetime.now(timezone.utc)
                        )
                        session.add(new_token)
                        await session.commit()
        
            return token

    def _cleanup_success(self, meeting_id: str):
        # Remove from all tracking sets/queues on success
        self.redis.zrem(RedisKeys.INDEXING_QUEUE, meeting_id)
        self.redis.srem(RedisKeys.PROCESSING_SET, meeting_id)
        self.redis.hdel(RedisKeys.FAILED_SET, meeting_id)
        self.redis.delete(f"meeting:retry:{meeting_id}")

    async def _handle_error(self, meeting_id: str, error: Exception):
        # Get current retry count
        retry_key = f"meeting:retry:{meeting_id}"
        retry_count = int(self.redis.get(retry_key) or 0)
        retry_count += 1
        
        if retry_count >= self.max_retries:
            # Move to failed set with error info
            error_info = {
                "error": str(error),
                "retry_count": retry_count,
                "last_attempt": datetime.now().isoformat()
            }
            self.redis.hset(RedisKeys.FAILED_SET, meeting_id, json.dumps(error_info))
            self.redis.zrem(RedisKeys.INDEXING_QUEUE, meeting_id)
        else:
            # Update retry count and requeue with delay
            self.redis.set(retry_key, retry_count)
            next_attempt = datetime.now().timestamp() + self.retry_delay
            self.redis.zadd(RedisKeys.INDEXING_QUEUE, {meeting_id: next_attempt})

    async def _get_or_create_meeting(self, meeting_id: UUID, transcript: list, 
                                   meeting_datetime: datetime, meeting_summary: dict, 
                                   user_id: UUID, session: AsyncSession) -> Meeting:
        # Create meeting object with updated data
        meeting = Meeting(
            meeting_id=meeting_id,
            meeting_name=meeting_summary.meeting_name,
            meeting_summary=meeting_summary.summary,
            transcript=transcript,
            timestamp=meeting_datetime,
        )
        
        # Merge will update if exists, create if not
        meeting = await session.merge(meeting)
        
        # Check if UserMeeting record exists
        user_meeting_exists = await session.execute(
            select(exists().where(and_(
                UserMeeting.meeting_id == meeting_id,
                UserMeeting.user_id == user_id
            )))
        )
        
        if not user_meeting_exists.scalar():
            user_meeting = UserMeeting(
                user_id=user_id,
                meeting_id=meeting_id,
                access_level=AccessLevel.OWNER.value,
                is_owner=True,
                created_by=user_id
            )
            session.add(user_meeting)
        
        return meeting

    async def _handle_speakers(self, speaker_names: list, session: AsyncSession) -> dict:
        speakers = {}
        for name in speaker_names:
            speaker = await session.execute(
                select(Speaker).where(Speaker.name == name)
            )
            speaker = speaker.scalar_one_or_none()
            
            if not speaker:
                speaker = Speaker(name=name)
                session.add(speaker)
                await session.flush()  # Get ID for new speaker
                
            speakers[name] = speaker
            
        return speakers

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
        self.scanner = MeetingsScanner(self.redis, self.vexa_auth)
        self.worker = IndexingWorker(self.redis) if initialize_worker else None

    async def start(self):
        if not self.worker:
            raise RuntimeError("Worker not initialized")
        await asyncio.gather(
            self.monitor.run(),
            self.scanner.run(),
            self.worker.run()
        )

class RedisMonitor:
    def __init__(self, redis: Redis):
        self.redis = redis

    def get_pipeline_stats(self) -> dict:
        seen_users = self.redis.smembers(RedisKeys.SEEN_USERS)
        queued = self.redis.zrange(RedisKeys.INDEXING_QUEUE, 0, -1, withscores=True)
        processing = self.redis.smembers(RedisKeys.PROCESSING_SET)
        failed = {
            mid: json.loads(err) 
            for mid in self.redis.hkeys(RedisKeys.FAILED_SET)
            if (err := self.redis.hget(RedisKeys.FAILED_SET, mid))
        }
        
        retry_counts = {
            key.split(":")[-1]: int(self.redis.get(key) or 0)
            for key in self.redis.keys("meeting:retry:*")
        }
        
        return {
            "users": {
                "count": len(seen_users),
                "items": list(seen_users)
            },
            "meetings": {
                "queued": {
                    "count": len(queued),
                    "items": [{"id": mid, "score": score} for mid, score in queued]
                },
                "processing": {
                    "count": len(processing),
                    "items": list(processing)
                },
                "failed": {
                    "count": len(failed),
                    "items": failed
                },
                "retries": retry_counts
            }
        }

    def get_meeting_status(self, meeting_id: str) -> dict:
        status = []
        if score := self.redis.zscore(RedisKeys.INDEXING_QUEUE, meeting_id):
            status.append(f"queued (score: {score})")
        if self.redis.sismember(RedisKeys.PROCESSING_SET, meeting_id):
            status.append("processing")
        if error := self.redis.hget(RedisKeys.FAILED_SET, meeting_id):
            status.append(f"failed: {json.loads(error)}")
        if retry := self.redis.get(f"meeting:retry:{meeting_id}"):
            status.append(f"retries: {retry}")
            
        return {"meeting_id": meeting_id, "status": status or ["not found"]}

    def cleanup_meeting(self, meeting_id: str) -> dict:
        removed = []
        for key, type_ in [
            (RedisKeys.PROCESSING_SET, "srem"),
            (RedisKeys.FAILED_SET, "hdel"),
            (RedisKeys.INDEXING_QUEUE, "zrem")
        ]:
            if getattr(self.redis, type_)(key, meeting_id):
                removed.append(key)
        
        self.redis.delete(f"meeting:retry:{meeting_id}")
        return {"meeting_id": meeting_id, "cleaned_from": removed}

    def reset_pipeline(self) -> dict:
        keys = [
            RedisKeys.INDEXING_QUEUE,
            RedisKeys.PROCESSING_SET,
            RedisKeys.FAILED_SET,
            RedisKeys.SEEN_USERS,
            *self.redis.keys("meeting:retry:*")
        ]
        deleted = self.redis.delete(*keys)
        return {"status": "reset", "keys_deleted": deleted}

    async def get_db_stats(self) -> dict:
        async with async_session() as session:
            total = await session.scalar(
                select(func.count(distinct(Meeting.meeting_id)))
            )
            indexed = await session.scalar(
                select(func.count(distinct(Meeting.meeting_id)))
                .join(DiscussionPoint)
            )
            users = await session.scalar(
                select(func.count(distinct(UserMeeting.user_id)))
            )
            
        return {
            "total_meetings": total,
            "indexed_meetings": indexed,
            "total_users": users,
            "progress": f"{(indexed/total)*100:.1f}%" if total else "0%"
        }