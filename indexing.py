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
from psql_models import Speaker, Meeting, DiscussionPoint, engine, UserMeeting, AccessLevel
from psql_helpers import get_session


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

async def save_meeting_data_to_db(final_df, meeting_id, transcript, meeting_datetime, user_id):  # Added user_id parameter
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
                await session.flush()

                # Create UserMeeting relationship with owner access
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
                new_meeting = existing_meeting

            for _, row in final_df.iterrows():
                speaker_query = await session.execute(
                    select(Speaker).where(Speaker.name == row['speaker'])
                )
                speaker = speaker_query.scalar_one_or_none()
                
                if not speaker:
                    speaker = Speaker(name=row['speaker'])
                    session.add(speaker)
                    await session.flush()

                new_discussion_point = DiscussionPoint(
                    summary_index=row['summary_index'],
                    summary=row['summary'],
                    details=row['details'],
                    referenced_text=row['referenced_text'],
                    meeting_id=new_meeting.meeting_id,
                    speaker_id=speaker.id,
                    topic_name=row['topic_name'],
                    topic_type=row['topic_type'],
                    model=row['model']
                )
                session.add(new_discussion_point)
                await session.flush()

            await session.commit()
            print("Meeting data and discussion points saved successfully to the database.")
        except Exception as e:
            await session.rollback()
            print(f"Error saving to database: {e}")
            raise

class Indexing:
    def __init__(self, token: str):
        self.vexa = VexaAPI(token=token)
        
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

    @property
    def status(self):
        return {
            "is_indexing": bool(self.redis.get(self.indexing_key)),
            "current_meeting": self.redis.get(self.current_meeting_key),
            "processing_meetings": list(self.redis.smembers(self.processing_key))
        }

    async def index_meetings(self, num_meetings: int = 400):
        await self.vexa.get_user_info()
        user_id = self.vexa.user_id
        # Add user-specific lock key
        indexing_lock_key = f"indexing:lock:{user_id}"
        
        # Try to acquire lock with timeout
        if not self.redis.set(indexing_lock_key, 'true', nx=True, ex=self._lock_timeout):
            raise ValueError("Indexing already in progress for this user")

        try:
            meetings = await self.vexa.get_meetings()
            meetings = meetings[-num_meetings:]

            for meeting in reversed(meetings):
                # Add delay between processing meetings
                await asyncio.sleep(0.1)  
                
                meeting_id = meeting['id']
                
                # Skip if already being processed
                if self.redis.sismember(self.processing_key, meeting_id):
                    print(f"Meeting {meeting_id} already being processed, skipping...")
                    continue
                
                try:
                    if not await check_item_exists(meeting_id):
                        # Add to processing set and set current meeting
                        self.redis.sadd(self.processing_key, meeting_id)
                        self.redis.set(self.current_meeting_key, meeting_id)
                        print(f"Processing meeting {meeting_id}")
                        
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
                            print(f"Successfully processed meeting {meeting_id}")
                except Exception as e:
                    print(f"Error processing meeting {meeting_id}: {e}")
                finally:
                    # Clean up meeting state
                    self.redis.srem(self.processing_key, meeting_id)
                    self.redis.delete(self.current_meeting_key)
        except Exception as e:
            print(f"Error in index_meetings: {e}")
            raise
        finally:
            # Clean up indexing state
            self.redis.delete(indexing_lock_key)
            self.redis.delete(self.indexing_key)
