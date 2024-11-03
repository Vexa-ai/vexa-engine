import pandas as pd
import datetime
import os
import asyncio
from sqlalchemy import func, exists, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.orm import selectinload
from redis import Redis
from uuid import UUID

from vexa import VexaAPI
from core import system_msg, user_msg
from prompts import Prompts
from pydantic_models import MeetingExtraction, EntityExtraction, MeetingNameAndSummary
from psql_models import Speaker, Meeting, DiscussionPoint, engine, UserMeeting, AccessLevel, DefaultAccess,User, UserToken
from psql_helpers import get_session
from qdrant_search import QdrantSearchEngine

from sqlalchemy import select, exists,distinct


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
    points_df = pd.concat([discussion_points_df, topics_df]).reset_index(drop=True)
    points_df = points_df.reset_index().rename(columns={'index': 'summary_index'})
    
    try:
        final_df = points_df.groupby('summary_index').agg({
            'topic_name': 'first',
            'topic_type': 'first',
            'summary': 'first',
            'details': 'first',
            'speaker': 'first',
            'model': 'first'
        }).reset_index()
        
        meeting_name_and_summary = await MeetingNameAndSummary.extract(formatted_input, final_df.drop(columns=['model']).to_markdown(index=False))

        return final_df, meeting_name_and_summary
    except Exception as e:
        print(f"Error processing meeting data: {e}")
        return pd.DataFrame()

async def save_meeting_data_to_db(final_df, meeting_id, transcript, meeting_datetime, user_id, meeting_name_and_summary):
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
                # Update existing meeting with name and summary
                existing_meeting.meeting_name = meeting_name_and_summary.meeting_name
                existing_meeting.meeting_summary = meeting_name_and_summary.summary
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
                        final_df, meeting_name_and_summary = await asyncio.wait_for(
                            process_meeting_data(formatted_input, df),
                            timeout=60
                        )
                        await save_meeting_data_to_db(
                            final_df, 
                            meeting_id, 
                            transcript, 
                            start_datetime, 
                            user_id,
                            meeting_name_and_summary
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

    async def index_meetings(self, num_meetings: int = 20):
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

    def show_redis_queues(self):
        """Display contents of all Redis queues related to indexing"""
        queues_info = {
            "Processing Queue": list(self.redis.smembers(self.processing_key)),
            "Dead Letter Queue": list(self.redis.smembers(self.dead_letter_key)),
            "Current Meeting": self.redis.get(self.current_meeting_key),
            "Is Indexing": bool(self.redis.get(self.indexing_key))
        }
        
        # Get all retry counters
        retry_keys = self.redis.keys("meeting:retry:*")
        retry_counts = {}
        for key in retry_keys:
            meeting_id = key.split(":")[-1]
            count = self.redis.get(key)
            retry_counts[meeting_id] = count
            
        queues_info["Retry Counts"] = retry_counts
        
        return queues_info

    def reset_redis_queues(self):
        """Reset all Redis queues related to indexing"""
        # Delete all keys
        self.redis.delete(self.processing_key)
        self.redis.delete(self.indexing_key)
        self.redis.delete(self.current_meeting_key)
        self.redis.delete(self.dead_letter_key)
        
        # Delete all retry counters
        retry_keys = self.redis.keys("meeting:retry:*")
        if retry_keys:
            self.redis.delete(*retry_keys)
            
        # Delete any indexing locks
        lock_keys = self.redis.keys("indexing:lock:*")
        if lock_keys:
            self.redis.delete(*lock_keys)
            
        return {
            "status": "success",
            "message": "All indexing queues have been reset"
        }

async def get_indexing_stats(session: AsyncSession) -> pd.DataFrame:
    # Get users with their tokens and meeting counts
    users_query = (
        select(
            User.id.label('user_id'),
            User.email,
            UserToken.token,
            func.max(Meeting.timestamp).label('last_meeting_date')
        )
        .join(UserToken, User.id == UserToken.user_id)
        .join(UserMeeting, User.id == UserMeeting.user_id)
        .join(Meeting)
        .where(UserMeeting.is_owner == True)
        .group_by(User.id, User.email, UserToken.token)
    )
    
    result = await session.execute(users_query)
    users_data = result.mappings().all()
    df = pd.DataFrame(users_data)
    
    # Get total meetings count
    total_meetings_query = (
        select(
            UserMeeting.user_id,
            func.count(distinct(Meeting.meeting_id)).label('total_meetings')
        )
        .join(Meeting)
        .where(UserMeeting.is_owner == True)
        .group_by(UserMeeting.user_id)
    )
    
    result = await session.execute(total_meetings_query)
    total_meetings_data = result.mappings().all()
    total_df = pd.DataFrame(total_meetings_data)
    
    # Print debugging information
    print("df columns:", df.columns.tolist())
    print("total_df columns:", total_df.columns.tolist())
    print("\ndf head:\n", df.head())
    print("\ntotal_df head:\n", total_df.head())
    
    # Convert UUID to string in both DataFrames before merge
    if 'user_id' in df.columns:
        df['user_id'] = df['user_id'].astype(str)
    if 'user_id' in total_df.columns:
        total_df['user_id'] = total_df['user_id'].astype(str)
    
    # Merge with total meetings
    df = pd.merge(df, total_df, on='user_id', how='left')
    
    # Get indexed meetings count
    indexed_query = (
        select(
            UserMeeting.user_id,
            func.count(distinct(Meeting.meeting_id)).label('indexed_meetings')
        )
        .join(Meeting)
        .join(DiscussionPoint, Meeting.meeting_id == DiscussionPoint.meeting_id)
        .where(UserMeeting.is_owner == True)
        .group_by(UserMeeting.user_id)
    )
    
    result = await session.execute(indexed_query)
    indexed_data = result.mappings().all()
    indexed_df = pd.DataFrame(indexed_data)
    
    if 'user_id' in indexed_df.columns:
        indexed_df['user_id'] = indexed_df['user_id'].astype(str)
    
    # Merge with indexed meetings
    df = pd.merge(df, indexed_df, on='user_id', how='left')
    
    # Calculate additional metrics
    df['remaining_meetings'] = df['total_meetings'] - df['indexed_meetings']
    df['progress_percentage'] = round(
        (df['indexed_meetings'] / df['total_meetings'] * 100).fillna(0), 2
    )
    
    # Format timestamp
    df['last_meeting_date'] = pd.to_datetime(df['last_meeting_date']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Remove token from final output and reorder columns
    df = df[[
        'user_id', 
        'email',
        'total_meetings',
        'indexed_meetings',
        'remaining_meetings',
        'progress_percentage',
        'last_meeting_date'
    ]]
    
    return df

async def start_indexing_for_users(session: AsyncSession, emails: list[str] = None, num_meetings: int = 10) -> dict:
    """
    Start indexing process for selected users by email
    
    Args:
        session: AsyncSession - Database session
        emails: Optional[list[str]] - List of user emails to index. If None, indexes all users
        num_meetings: int - Number of most recent meetings to index per user
    Returns:
        dict with status of indexing initiation for each user
    """
    
    # Query to get users and their tokens
    users_query = (
        select(
            User.id,
            User.email,
            UserToken.token
        )
        .join(UserToken, User.id == UserToken.user_id)
    )
    
    if emails:
        users_query = users_query.where(User.email.in_(emails))
    
    users_result = await session.execute(users_query)
    users_data = users_result.all()
    
    # Track which emails were not found
    if emails:
        found_emails = {user.email for user in users_data}
        not_found = set(emails) - found_emails
        results = {
            email: {
                "email": email,
                "status": "failed",
                "error": "User not found"
            } for email in not_found
        }
    else:
        results = {}
    
    tasks = []
    
    for user_id, email, token in users_data:
        if not token:
            results[email] = {
                "email": email,
                "status": "failed",
                "error": "No token found"
            }
            continue
            
        try:
            # Create indexer instance for each user
            indexer = Indexing(token)
            
            # Create task for each user's indexing process
            task = asyncio.create_task(
                indexer.index_meetings(num_meetings=num_meetings)
            )
            tasks.append((email, task))
            
            results[email] = {
                "email": email,
                "status": "initiated"
            }
            
        except Exception as e:
            results[email] = {
                "email": email,
                "status": "failed",
                "error": str(e)
            }
    
    # Wait for all indexing tasks to complete or timeout
    if tasks:
        done, pending = await asyncio.wait(
            [task for _, task in tasks],
            timeout=10.0  # Timeout for waiting on task initiation
        )
        
        # Update results based on task completion
        for email, task in tasks:
            if task in pending:
                results[email]["status"] = "running"
            elif task in done:
                try:
                    await task
                    results[email]["status"] = "completed"
                except Exception as e:
                    results[email]["status"] = "failed"
                    results[email]["error"] = str(e)
    
    return results
