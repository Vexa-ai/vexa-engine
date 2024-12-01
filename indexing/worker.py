from redis import Redis
import asyncio
from typing import Optional
from datetime import datetime, timezone
import os
from vexa import VexaAPI, VexaAuth
from psql_models import (Meeting, DiscussionPoint, async_session, Speaker, 
                        UserMeeting, AccessLevel, UserToken, User)
from sqlalchemy import select, exists, and_, func, distinct, cast
from sqlalchemy.dialects.postgresql import UUID, insert
from qdrant_search import QdrantSearchEngine
from pydantic_models import MeetingExtraction, EntityExtraction, MeetingNameAndSummary
import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
import json
from .redis_keys import RedisKeys
from asyncio import TaskGroup
from collections import deque
from psql_helpers import get_meeting_token
import logging
from time import time

class ProcessingError(Exception):
    pass

class IndexingWorker:
    def __init__(self, redis: Redis, max_concurrent: int = 10, retry_delay: int = 300):
        self.redis = redis
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.retry_delay = retry_delay
        self.max_retries = 3
        self.qdrant = QdrantSearchEngine()
        self.max_concurrent = max_concurrent
        
        # Setup logging
        self.logger = logging.getLogger('indexing_worker')
        self.logger.setLevel(logging.INFO)
        handler = logging.FileHandler('indexing_worker.log')
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        ))
        self.logger.addHandler(handler)

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
        try:
            # Get meeting data
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
            
        except Exception as e:
            raise ProcessingError(f"Meeting processing failed: {str(e)}")

    async def _get_meeting_token(self, meeting_id: str) -> Optional[str]:
        return await get_meeting_token(meeting_id)

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
        # Convert transcript to string and ensure timestamp is UTC
        transcript_str = str(transcript)
        meeting_time = meeting_datetime.replace(tzinfo=None)  # Remove timezone info
        
        # Prepare insert statement with ON CONFLICT
        stmt = insert(Meeting).values(
            meeting_id=meeting_id,
            meeting_name=meeting_summary.meeting_name,
            meeting_summary=meeting_summary.summary,
            transcript=transcript_str,
            is_indexed=True
        ).on_conflict_do_update(
            index_elements=['meeting_id'],
            set_={
                'meeting_name': meeting_summary.meeting_name,
                'meeting_summary': meeting_summary.summary,
                'transcript': transcript_str,
                'is_indexed': True
            }
        ).returning(Meeting)
        
        # Execute upsert and get result
        result = await session.execute(stmt)
        meeting = result.scalar_one()
        
        # Handle UserMeeting association
        user_meeting_stmt = insert(UserMeeting).values(
            user_id=user_id,
            meeting_id=meeting_id,
            access_level=AccessLevel.OWNER.value,
            is_owner=True,
            created_by=user_id
        ).on_conflict_do_nothing(
            index_elements=['meeting_id', 'user_id']
        )
        await session.execute(user_meeting_stmt)
        
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
    
    async def run(self):
        processing_tasks = set()
        pending_meetings = deque()
        processed_meetings = set()
        
        async def cleanup_stale_processing():
            # Consider entries stale after 30 minutes
            stale_timeout = 30 * 60  
            now = datetime.now().timestamp()
            
            # Get all processing meetings
            processing = self.redis.smembers(RedisKeys.PROCESSING_SET)
            for mid in processing:
                meeting_id = mid.decode() if isinstance(mid, bytes) else mid
                # Check if meeting is actually being processed
                if not any(t for t in processing_tasks if t.get_name() == meeting_id):
                    # Check if meeting is in retry queue
                    retry_score = self.redis.zscore(RedisKeys.INDEXING_QUEUE, meeting_id)
                    if not retry_score or (now - retry_score) > stale_timeout:
                        self.logger.warning(f"Removing stale processing entry for meeting {meeting_id}")
                        self.redis.srem(RedisKeys.PROCESSING_SET, meeting_id)
        
        while True:
            try:
                # Add periodic cleanup
                await cleanup_stale_processing()
                
                # Refill pending_meetings if below threshold
                if len(pending_meetings) < self.max_concurrent:
                    now = datetime.now().timestamp()
                    next_meetings = self.redis.zrevrangebyscore(
                        RedisKeys.INDEXING_QUEUE,
                        now, '-inf',
                        start=0,
                        num=self.max_concurrent * 2
                    )
                    # Only add meetings that aren't already pending or processing
                    for m in next_meetings:
                        meeting_id = m.decode() if isinstance(m, bytes) else m
                        if (meeting_id not in pending_meetings and 
                            not any(t for t in processing_tasks if t.get_name() == meeting_id)):
                            pending_meetings.append(meeting_id)
                
                # Start new tasks if we're below max_concurrent
                while len(processing_tasks) < self.max_concurrent and pending_meetings:
                    meeting_id = pending_meetings.popleft()
                    
                    # Skip if already processing, log only once per meeting
                    if self.redis.sismember(RedisKeys.PROCESSING_SET, meeting_id):
                        if meeting_id not in processed_meetings:
                            self.logger.info(f"Skipping queue add for meeting {meeting_id} - already processing")
                            processed_meetings.add(meeting_id)
                        continue
                        
                    # Create and track new task
                    task = asyncio.create_task(self._process_meeting_safe(meeting_id))
                    task.set_name(meeting_id)  # Set task name to meeting_id for tracking
                    processing_tasks.add(task)
                    task.add_done_callback(processing_tasks.discard)
                
                    # Clean up processed meetings set periodically
                    if len(processed_meetings) > 1000:  # Arbitrary limit
                        processed_meetings.clear()
                
                # Wait a bit if we have no work
                if not pending_meetings and not processing_tasks:
                    await asyncio.sleep(1)
                    continue
                    
                # Wait for at least one task to complete if we're at capacity
                if len(processing_tasks) >= self.max_concurrent:
                    await asyncio.wait(
                        processing_tasks,
                        return_when=asyncio.FIRST_COMPLETED
                    )
                
                # Small sleep to prevent tight loop
                await asyncio.sleep(0.1)
                
            except Exception as e:
                self.logger.error(f"Worker loop error: {e}")
                await asyncio.sleep(5)

    async def _process_meeting_safe(self, meeting_id: str):
        start_time = time()
        self.logger.info(f"Starting processing meeting: {meeting_id}")
        
        async with self.semaphore:
            try:
                self.redis.sadd(RedisKeys.PROCESSING_SET, meeting_id)
                await self._process_meeting(meeting_id)
                processing_time = time() - start_time
                self.logger.info(f"Successfully processed meeting {meeting_id} in {processing_time:.2f} seconds")
                self._cleanup_success(meeting_id)
            except Exception as e:
                processing_time = time() - start_time
                self.logger.error(f"Failed processing meeting {meeting_id} after {processing_time:.2f} seconds. Error: {str(e)}")
                await self._handle_error(meeting_id, e)
            finally:
                self.redis.srem(RedisKeys.PROCESSING_SET, meeting_id)