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

class ProcessingError(Exception):
    pass

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
            return result.scalars().first()  #TODO 1
           

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
        while True:
            try:
                now = datetime.now().timestamp()
                # Get meetings ready for processing using zrangebyscore instead
                next_meetings = self.redis.zrangebyscore(
                    RedisKeys.INDEXING_QUEUE,
                    '-inf',  # min score
                    now,    # max score
                    start=0,
                    num=1,
                    withscores=True
                )
                
                if next_meetings:
                    meeting_id, score = next_meetings[0]
                    meeting_id = meeting_id.decode() if isinstance(meeting_id, bytes) else meeting_id
                    
                    # Skip if already processing
                    if self.redis.sismember(RedisKeys.PROCESSING_SET, meeting_id):
                        await asyncio.sleep(1)
                        continue
                    
                    async with self.semaphore:
                        try:
                            # Mark as processing
                            self.redis.sadd(RedisKeys.PROCESSING_SET, meeting_id)
                            await self._process_meeting(meeting_id)
                            # Clean up on success
                            self._cleanup_success(meeting_id)
                        except Exception as e:
                            await self._handle_error(meeting_id, e)
                        finally:
                            # Always remove from processing set
                            self.redis.srem(RedisKeys.PROCESSING_SET, meeting_id)
                
                await asyncio.sleep(1)
            except Exception as e:
                print(f"Worker loop error: {e}")
                await asyncio.sleep(5)