import pandas as pd
import datetime
import os
import asyncio
from sqlalchemy import func, exists, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from redis import Redis

from vexa import VexaAPI
from core import system_msg, user_msg
from prompts import Prompts
from pydantic_models import MeetingExtraction, EntityExtraction, SummaryIndexesRefs, MeetingSummary
from psql_models import Speaker, Meeting, DiscussionPoint, engine, UserMeeting, AccessLevel, DefaultAccess
from psql_helpers import get_session
from qdrant_search import QdrantSearchEngine


async def check_item_exists(meeting_id):
    async with get_session() as session:
        meeting_id_str = str(meeting_id)
        query = select(exists().where(DiscussionPoint.meeting_id == meeting_id_str))
        result = await session.execute(query)
        return result.scalar()

def flatten_context(context):
    flattened = []
    for item in context:
        base = {k: v for k, v in item.items() if k != 'objects'}
        if 'objects' in item:
            for obj in item['objects']:
                flattened.append({**base, **obj})
        else:
            flattened.append(base)
    return flattened

async def process_meeting_data(formatted_input, df):
    extraction_tasks = [
        MeetingExtraction.extract(formatted_input),
        EntityExtraction.extract(formatted_input)
    ]
    discussion_points_df, topics_df = await asyncio.gather(*extraction_tasks)
    
    discussion_points_df['model'] = 'MeetingExtraction'
    topics_df['model'] = 'EntityExtraction'
    
    # Rename columns to match the new schema
    discussion_points_df = discussion_points_df.rename(columns={'item': 'topic_name', 'type': 'topic_type'})
    topics_df = topics_df.rename(columns={'entity': 'topic_name', 'type': 'topic_type'})
    
    # Combine the dataframes
    summary_df = pd.concat([discussion_points_df, topics_df]).reset_index(drop=True)
    
    summary_refs = await SummaryIndexesRefs.extract(summary_df, formatted_input)

    # Create a new dataframe for the references
    ref_df = pd.DataFrame([(ref['summary_index'], r['s'], r['e']) 
                           for ref in summary_refs 
                           for r in ref['references']],
                          columns=['summary_index', 'start', 'end'])

    # Merge the ref_df with summary_df
    entities_with_refs = summary_df.reset_index().rename(columns={'index': 'summary_index'})
    entities_with_refs = entities_with_refs.merge(ref_df, on='summary_index', how='left')

    # Function to extract text from df based on start and end indices, including speaker
    def get_text_range_with_speaker(row):
        text_range = df.loc[row['start']:row['end']]
        return ' | '.join(f"{speaker}: {content}" for speaker, content in zip(text_range['speaker'], text_range['content']))

    # Apply the function to get the referenced text with speakers
    entities_with_refs['referenced_text'] = entities_with_refs.apply(get_text_range_with_speaker, axis=1)

    # Group by summary_index to combine multiple references
    try:
        final_df = entities_with_refs.groupby('summary_index').agg({
            'topic_name': 'first',
            'topic_type': 'first',
            'summary': 'first',
            'details': 'first',
            'speaker': 'first',
            'referenced_text': ' | '.join,
            'model': 'first'
        }).reset_index()

        return final_df
    except Exception as e:
        print(f"Error processing meeting data: {e}")
        return pd.DataFrame()

async def save_meeting_data_to_db(final_df, meeting_id, transcript, meeting_datetime, user_id):
    async with AsyncSession(engine) as session:
        try:
            existing_meeting = await session.execute(
                select(Meeting).where(Meeting.meeting_id == meeting_id)
            )
            existing_meeting = existing_meeting.scalar_one_or_none()

            if not existing_meeting:
                naive_datetime = meeting_datetime.replace(tzinfo=None) - meeting_datetime.utcoffset()
                new_meeting = Meeting(
                    meeting_id=meeting_id, 
                    transcript=str(transcript),
                    timestamp=naive_datetime
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

                # Check and propagate default access settings
                default_access_query = await session.execute(
                    select(DefaultAccess)
                    .where(DefaultAccess.owner_user_id == user_id)
                )
                default_access_records = default_access_query.scalars().all()

                # Create UserMeeting records for users with default access
                for default_access in default_access_records:
                    new_user_meeting = UserMeeting(
                        meeting_id=meeting_id,
                        user_id=default_access.granted_user_id,
                        access_level=default_access.access_level,
                        is_owner=False,
                        created_by=user_id
                    )
                    session.add(new_user_meeting)
            else:
                new_meeting = existing_meeting

            # Bulk insert speakers
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

            # Bulk insert discussion points
            discussion_points = [
                DiscussionPoint(
                    summary_index=row['summary_index'],
                    summary=row['summary'],
                    details=row['details'],
                    referenced_text=row['referenced_text'],
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
            
        except Exception as e:
            await session.rollback()
            raise

class Indexing:
    def __init__(self, token: str):
        self.vexa = VexaAPI(token=token)
        self.qdrant = QdrantSearchEngine()
        
        # Use connection pooling for Redis
        self.redis = Redis(
            host=os.getenv('REDIS_HOST', '127.0.0.1'),
            port=int(os.getenv('REDIS_PORT', 6379)),
            db=0,
            decode_responses=True,
            max_connections=10,  # Add connection pooling
            socket_timeout=5
        )
        
        # Add semaphore for concurrent processing
        self._semaphore = asyncio.Semaphore(3)  # Limit concurrent processing
        
        # Redis keys
        self.processing_key = 'meetings:processing'
        self.indexing_key = 'indexing:status'
        self.current_meeting_key = 'indexing:current_meeting'
        
        # Add lock timeout for indexing
        self._lock_timeout = 3600  # 1 hour timeout for indexing lock
        
        # Add dead letter queue key
        self.dead_letter_key = 'meetings:dead_letter'
        self.max_retries = 3  # Maximum number of retry attempts

    @property
    def status(self):
        return {
            "is_indexing": bool(self.redis.get(self.indexing_key)),
            "current_meeting": self.redis.get(self.current_meeting_key),
            "processing_meetings": list(self.redis.smembers(self.processing_key))
        }

    async def process_single_meeting(self, meeting, user_id):
        meeting_id = meeting['id']
        
        async with self._semaphore:
            # Check if meeting is in dead letter queue
            if self.redis.sismember(self.dead_letter_key, meeting_id):
                print(f"Meeting {meeting_id} in dead letter queue, skipping...")
                return

            # Get retry count
            retry_key = f"meeting:retry:{meeting_id}"
            retry_count = int(self.redis.get(retry_key) or 0)
            
            if retry_count >= self.max_retries:
                print(f"Meeting {meeting_id} exceeded max retries, moving to dead letter queue")
                self.redis.sadd(self.dead_letter_key, meeting_id)
                self.redis.delete(retry_key)
                return

            if self.redis.sismember(self.processing_key, meeting_id):
                print(f"Meeting {meeting_id} already being processed, skipping...")
                return
            
            try:
                if not await check_item_exists(meeting_id):
                    self.redis.sadd(self.processing_key, meeting_id)
                    self.redis.set(self.current_meeting_key, meeting_id)
                    
                    transcription = await self.vexa.get_transcription(
                        meeting_session_id=meeting_id, 
                        use_index=True
                    )
                    
                    if transcription:   
                        df, formatted_input, start_datetime, speakers, transcript = transcription
                        final_df = await asyncio.wait_for(
                            process_meeting_data(formatted_input, df),
                            timeout=60
                        )
                        await save_meeting_data_to_db(
                            final_df, 
                            meeting_id, 
                            transcript, 
                            start_datetime, 
                            user_id
                        )
                        async with get_session() as session:
                            await self.qdrant.sync_meeting(meeting_id, session)
            except Exception as e:
                print(f"Error processing meeting {meeting_id}: {e}")
                # Increment retry counter
                self.redis.incr(retry_key)
                # Set expiry on retry key to avoid orphaned counters
                self.redis.expire(retry_key, 86400)  # 24 hours
            finally:
                self.redis.srem(self.processing_key, meeting_id)
                self.redis.delete(self.current_meeting_key)

    async def index_meetings(self, num_meetings: int = 110):
        num_meetings = 120 #temp
        await self.vexa.get_user_info()
        user_id = self.vexa.user_id
        indexing_lock_key = f"indexing:lock:{user_id}"
        
        if not self.redis.set(indexing_lock_key, 'true', nx=True, ex=self._lock_timeout):
            raise ValueError("Indexing already in progress for this user")

        try:
            await self.qdrant.sync_from_postgres(get_session)
            
            meetings = await self.vexa.get_meetings()
            meetings = meetings[-num_meetings:]

            # Process meetings concurrently
            tasks = [
                asyncio.create_task(self.process_single_meeting(meeting, user_id))
                for meeting in reversed(meetings)
            ]
            await asyncio.gather(*tasks)
            
        except Exception as e:
            print(f"Error in index_meetings: {e}")
            raise
        finally:
            self.redis.delete(indexing_lock_key)
            self.redis.delete(self.indexing_key)
