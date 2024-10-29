from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, Float, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, joinedload
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy import create_engine, text
import uuid
from datetime import datetime
import hashlib
from uuid import UUID
from typing import List, Optional
from datetime import timezone
from sqlalchemy import or_
from sqlalchemy import func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import select
from contextlib import asynccontextmanager
from sqlalchemy import and_
from enum import Enum
from sqlalchemy import Index, UniqueConstraint, CheckConstraint
from sqlalchemy import Table, Column, Integer, String, Text, Float, Boolean, ForeignKey

#docker run --name dima_entities -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres

from psql_models import Base, Meeting, Speaker, User, DiscussionPoint, AccessLevel, UserMeeting, DefaultAccess,engine,async_session,DATABASE_URL


# Async function to fetch context
async def fetch_context(session: AsyncSession, speaker_names: List[str], reference_date: datetime, user_id: UUID):
    print(f"Fetching context for speakers: {speaker_names}, reference_date: {reference_date}")

    if reference_date.tzinfo is None:
        reference_date = reference_date.replace(tzinfo=timezone.utc)

    # Use execute() for raw SQL queries
    result = await session.execute(select(Speaker).filter(Speaker.name.in_(speaker_names)))
    existing_speakers = result.scalars().all()

    if not existing_speakers:
        print(f"No speakers found with names: {speaker_names}")
        return []

    existing_speaker_names = [speaker.name for speaker in existing_speakers]
    print(f"Found existing speakers: {existing_speaker_names}")

    # Main query with date filter
    items_query = select(DiscussionPoint).join(Meeting).join(Speaker).filter(
        Speaker.name.in_(existing_speaker_names),
        Meeting.timestamp <= reference_date,
        or_(
            Meeting.owner_id == user_id,
            Meeting.meeting_id == user_id,
            Meeting.access_level.in_([AccessLevel.SEARCH.value, AccessLevel.TRANSCRIPT.value])
        )
    ).order_by(Meeting.timestamp.desc())

    result = await session.execute(items_query)
    discussion_points = result.scalars().all()
    print(f"Found {len(discussion_points)} discussion points after applying date filter")

    context = []
    for dp in discussion_points:
        context.append({
            'topic_name': dp.topic_name,
            'topic_type': dp.topic_type,
            'summary': dp.summary,
            'details': dp.details,
            'speaker': dp.speaker.name,
            'timestamp': dp.meeting.timestamp,
            'meeting_id': dp.meeting.meeting_id,
            'model': dp.model
        })

    print(f"Returning context with {len(context)} entries")

    # Debug: Check database statistics
    speakers_count = await session.scalar(select(func.count(Speaker.id)))
    meetings_count = await session.scalar(select(func.count(Meeting.id)))
    discussion_points_count = await session.scalar(select(func.count(DiscussionPoint.id)))
    print(f"Database statistics: Speakers: {speakers_count}, Meetings: {meetings_count}, Discussion Points: {discussion_points_count}")

    return context

async def execute_query(query):
    async with engine.connect() as conn:
        await conn.execute(text(query))

from sqlalchemy.schema import DropTable
from sqlalchemy.ext.compiler import compiles

@compiles(DropTable, "postgresql")
def _compile_drop_table(element, compiler, **kwargs):
    return compiler.visit_drop_table(element) + " CASCADE"

async def init_db():
    engine = create_async_engine(DATABASE_URL)
    
    async with engine.begin() as conn:
        # Drop all tables with CASCADE
        await conn.run_sync(Base.metadata.drop_all)
        
        # Create all tables
        await conn.run_sync(Base.metadata.create_all)
    
    print("Database initialized successfully.")
    
@asynccontextmanager
async def get_session():
    async with async_session() as session:
        try:
            yield session
            await session.commit()
        except:
            await session.rollback()
            raise


import pandas as pd
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.asyncio import AsyncSession

async def read_table_async(table_name):
    async with AsyncSession(engine) as session:
        result = await session.execute(select(table_name))
        rows = result.fetchall()
        
        # Extract attributes from the objects
        data = []
        for row in rows:
            obj = row[0]  # Assuming the object is the first (and only) item in each row
            data.append({column.name: getattr(obj, column.name) for column in table_name.__table__.columns})
        
        return pd.DataFrame(data)
    
    
async def fetch_joined_data():
    async with async_session() as session:
        # Original query remains the same
        query = select(DiscussionPoint, Meeting, Speaker).join(
            Meeting, DiscussionPoint.meeting_id == Meeting.meeting_id
        ).join(
            Speaker, DiscussionPoint.speaker_id == Speaker.id
        )
        
        result = await session.execute(query)
        rows = result.fetchall()
        
        # Create a dictionary to store all speakers per meeting
        meeting_speakers = {}
        for dp, meeting, speaker in rows:
            if meeting.meeting_id not in meeting_speakers:
                meeting_speakers[meeting.meeting_id] = set()
            meeting_speakers[meeting.meeting_id].add(speaker.name)
        
        # Convert the result to a list of dictionaries with other speakers
        data = []
        for dp, meeting, speaker in rows:
            # Get all speakers except the current one
            other_speakers = list(meeting_speakers[meeting.meeting_id] - {speaker.name})
            
            data.append({
                # Original fields remain the same
                'summary_index': dp.summary_index,
                'summary': dp.summary,
                'details': dp.details,
                'referenced_text': dp.referenced_text,
                'topic_name': dp.topic_name,
                'topic_type': dp.topic_type,
                'meeting_id': meeting.meeting_id,
                'meeting_timestamp': meeting.timestamp,
                'speaker_name': speaker.name,
                # Add new field
                'other_speakers': other_speakers
            })

        df = pd.DataFrame(data)
        return df

# Helper functions for access control
async def get_user_meeting_access(session: AsyncSession, user_id: UUID, meeting_id: UUID) -> AccessLevel:
    """Get user's access level for a meeting"""
    result = await session.execute(
        select(UserMeeting)
        .filter_by(meeting_id=meeting_id, user_id=user_id)
    )
    user_meeting = result.scalar_one_or_none()
    
    if user_meeting:
        if user_meeting.is_owner:
            return AccessLevel.OWNER
        return AccessLevel(user_meeting.access_level)
    return AccessLevel.REMOVED

async def can_access_transcript(session: AsyncSession, user_id: UUID, meeting_id: UUID) -> bool:
    """Check if user has transcript-level access or higher"""
    access = await get_user_meeting_access(session, user_id, meeting_id)
    return access in [AccessLevel.TRANSCRIPT, AccessLevel.OWNER]

async def can_access_search(session: AsyncSession, user_id: UUID, meeting_id: UUID) -> bool:
    """Check if user has search-level access or higher"""
    access = await get_user_meeting_access(session, user_id, meeting_id)
    return access in [AccessLevel.SEARCH, AccessLevel.TRANSCRIPT, AccessLevel.OWNER]

async def is_meeting_owner(session: AsyncSession, user_id: UUID, meeting_id: UUID) -> bool:
    """Check if user is the owner of the meeting"""
    access = await get_user_meeting_access(session, user_id, meeting_id)
    return access == AccessLevel.OWNER



# Add these new methods to the file

async def get_first_meeting_timestamp(session: AsyncSession, user_id: str) -> Optional[str]:
    query = select(Meeting.timestamp)\
        .join(UserMeeting, Meeting.meeting_id == UserMeeting.meeting_id)\
        .where(and_(
            UserMeeting.user_id == user_id,
            UserMeeting.is_owner == True
        ))\
        .order_by(Meeting.timestamp.asc())\
        .limit(1)
    
    result = await session.execute(query)
    timestamp = result.scalar()
    return timestamp.isoformat() if timestamp else None

async def get_last_meeting_timestamp(session: AsyncSession, user_id: str) -> Optional[str]:
    query = select(Meeting.timestamp)\
        .join(UserMeeting, Meeting.meeting_id == UserMeeting.meeting_id)\
        .where(and_(
            UserMeeting.user_id == user_id,
            UserMeeting.is_owner == True
        ))\
        .order_by(Meeting.timestamp.desc())\
        .limit(1)
    
    result = await session.execute(query)
    timestamp = result.scalar()
    return timestamp.isoformat() if timestamp else None

async def get_accessible_meetings(
    session: AsyncSession,
    user_id: UUID,
    access_level: Optional[AccessLevel] = None,
    limit: int = 100,
    offset: int = 0
) -> tuple[List[Meeting], int]:
    """
    Get all meetings accessible to a user with optional access level filter.
    Returns tuple of (meetings list, total count).
    
    Args:
        session: AsyncSession
        user_id: User UUID
        access_level: Optional minimum AccessLevel required (e.g., TRANSCRIPT will return TRANSCRIPT and OWNER access)
        limit: Maximum number of meetings to return
        offset: Number of meetings to skip
    """
    # Build base query
    query = (
        select(Meeting)
        .distinct()
        .join(UserMeeting, Meeting.meeting_id == UserMeeting.meeting_id)
        .where(
            and_(
                UserMeeting.user_id == user_id,
                UserMeeting.access_level != AccessLevel.REMOVED.value
            )
        )
    )

    # Add access level filter if specified
    if access_level:
        query = query.where(
            or_(
                Meeting.owner_id == user_id,  # Owner always has full access
                and_(
                    UserMeeting.user_id == user_id,
                    UserMeeting.access_level.in_([
                        level.value for level in AccessLevel
                        if level.value >= access_level.value
                    ])
                )
            )
        )

    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total_count = await session.scalar(count_query)

    # Add ordering and pagination
    query = (
        query
        .order_by(Meeting.timestamp.desc())
        .offset(offset)
        .limit(limit)
    )

    # Execute query
    result = await session.execute(query)
    meetings = result.scalars().unique().all()

    return meetings, total_count

# Helper method to get meetings with additional metadata
async def get_meetings_with_metadata(
    session: AsyncSession,
    user_id: UUID,
    access_level: Optional[AccessLevel] = None,
    limit: int = 100,
    offset: int = 0
) -> tuple[List[dict], int]:
    """
    Get meetings with additional metadata like access level and share info.
    Returns tuple of (meetings with metadata list, total count).
    """
    meetings, total_count = await get_accessible_meetings(
        session, user_id, access_level, limit, offset
    )
    
    # Fetch all access levels for these meetings in one query
    meeting_ids = [m.meeting_id for m in meetings]
    shares_query = await session.execute(
        select(UserMeeting)
        .where(UserMeeting.meeting_id.in_(meeting_ids))
    )
    shares_by_meeting = {}
    for share in shares_query.scalars():
        if share.meeting_id not in shares_by_meeting:
            shares_by_meeting[share.meeting_id] = []
        shares_by_meeting[share.meeting_id].append(share)

    # Build response with metadata
    result = []
    for meeting in meetings:
        meeting_data = {
            "meeting_id": meeting.meeting_id,
            "timestamp": meeting.timestamp,
            "transcript": meeting.transcript,
            "owner_id": meeting.owner_id,
            "is_owner": meeting.owner_id == user_id,
            "access_level": AccessLevel.OWNER.value if meeting.owner_id == user_id else 
                          next((
                              share.access_level 
                              for share in shares_by_meeting.get(meeting.meeting_id, [])
                              if share.user_id == user_id
                          ), AccessLevel.REMOVED.value),
            "shares": [
                {
                    "user_id": share.user_id,
                    "access_level": share.access_level,
                    "created_at": share.created_at,
                    "created_by": share.created_by
                }
                for share in shares_by_meeting.get(meeting.meeting_id, [])
            ]
        }
        result.append(meeting_data)

    return result, total_count

# Example usage:
"""
async def get_user_meetings(user_id: UUID):
    async with async_session() as session:
        # Get all meetings
        meetings, total = await get_accessible_meetings(session, user_id)
        
        # Get only meetings with transcript access
        transcript_meetings, count = await get_accessible_meetings(
            session, 
            user_id, 
            access_level=AccessLevel.TRANSCRIPT
        )
        
        # Get meetings with metadata
        meetings_with_meta, total = await get_meetings_with_metadata(
            session,
            user_id,
            limit=10,
            offset=0
        )
"""

async def get_schema_info():
    async with async_session() as session:
        # Query to get table information
        tables_query = """
        SELECT 
            t.table_name,
            array_agg(
                c.column_name || ' ' || 
                c.data_type || 
                CASE WHEN c.is_nullable = 'NO' THEN ' NOT NULL' ELSE '' END ||
                CASE WHEN c.column_default IS NOT NULL THEN ' DEFAULT ' || c.column_default ELSE '' END
            ) as columns,
            array_agg(
                CASE 
                    WHEN tc.constraint_type = 'PRIMARY KEY' THEN 'PK: ' || kcu.column_name
                    WHEN tc.constraint_type = 'FOREIGN KEY' THEN 
                        'FK: ' || kcu.column_name || ' -> ' || ccu.table_name || '.' || ccu.column_name
                    WHEN tc.constraint_type = 'UNIQUE' THEN 'UQ: ' || kcu.column_name
                    WHEN tc.constraint_type = 'CHECK' THEN 'CHECK: ' || tc.constraint_name
                END
            ) FILTER (WHERE tc.constraint_type IS NOT NULL) as constraints
        FROM 
            information_schema.tables t
        LEFT JOIN 
            information_schema.columns c ON t.table_name = c.table_name
        LEFT JOIN 
            information_schema.table_constraints tc ON t.table_name = tc.table_name
        LEFT JOIN 
            information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
        LEFT JOIN 
            information_schema.constraint_column_usage ccu ON tc.constraint_name = ccu.constraint_name
        WHERE 
            t.table_schema = 'public'
            AND t.table_type = 'BASE TABLE'
        GROUP BY 
            t.table_name
        ORDER BY 
            t.table_name;
        """
        
        result = await session.execute(text(tables_query))
        schema_info = result.fetchall()
        
        return schema_info

# Example usage:

async def set_default_access(
    session: AsyncSession, 
    owner_user_id: UUID, 
    granted_user_id: UUID, 
    access_level: AccessLevel
) -> DefaultAccess:

    # First check if both users exist
    users_query = select(User).filter(
        User.id.in_([owner_user_id, granted_user_id])
    )
    result = await session.execute(users_query)
    existing_users = result.scalars().all()
    
    if len(existing_users) != 2:
        raise ValueError("Both owner and granted users must exist in the users table")

    # Check if default access already exists
    existing = await session.execute(
        select(DefaultAccess)
        .filter_by(
            owner_user_id=owner_user_id,
            granted_user_id=granted_user_id
        )
    )
    default_access = existing.scalar_one_or_none()
    
    if default_access:
        # Update existing
        default_access.access_level = access_level.value
    else:
        # Create new
        default_access = DefaultAccess(
            owner_user_id=owner_user_id,
            granted_user_id=granted_user_id,
            access_level=access_level.value
        )
        session.add(default_access)
    
    await session.commit()
    return default_access

async def create_user(
    session: AsyncSession, 
    user_id: UUID, 
    email: str,
    username: str = None,
    first_name: str = None,
    last_name: str = None,
    image: str = None
) -> User:

    user = User(
        id=user_id,
        email=email,
        username=username,
        first_name=first_name,
        last_name=last_name,
        image=image
    )
    session.add(user)
    await session.commit()
    return user


