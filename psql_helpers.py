from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, Float, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, joinedload
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy import create_engine, text
import uuid
from datetime import datetime
import hashlib
from uuid import UUID
from typing import List, Optional, Union
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
from sqlalchemy import distinct
from psql_models import meeting_speaker_association

#docker run --name dima_entities -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres

from psql_models import Base, Meeting, Speaker, User, DiscussionPoint, AccessLevel, UserMeeting, DefaultAccess,engine,async_session,DATABASE_URL,ShareLink,UserToken

import secrets

from datetime import timedelta

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

async def setup_database():
    async with engine.begin() as conn:
        # Create all tables
        await conn.run_sync(Base.metadata.create_all)
    
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

async def read_table_async(table_or_query):
    async with AsyncSession(engine) as session:
        # Convert table class to select query if needed
        if hasattr(table_or_query, '__table__'):
            query = select(table_or_query)
        else:
            query = table_or_query
            
        result = await session.execute(query)
        rows = result.fetchall()
        
        # If it's a simple table query
        if hasattr(table_or_query, '__table__'):
            data = []
            for row in rows:
                obj = row[0]  # Assuming the object is the first item in each row
                data.append({column.name: getattr(obj, column.name) for column in table_or_query.__table__.columns})
        # If it's a complex query with multiple entities
        else:
            data = []
            for row in rows:
                row_data = {}
                for entity, obj in zip(query.selected_columns, row):
                    if hasattr(obj, '__table__'):
                        # If it's an ORM object
                        for column in obj.__table__.columns:
                            row_data[f"{obj.__table__.name}_{column.name}"] = getattr(obj, column.name)
                    else:
                        # If it's a single column
                        row_data[entity.name] = obj
                data.append(row_data)
        
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

async def get_first_meeting_timestamp(session: AsyncSession, user_id: UUID) -> Optional[str]:
    """Get the timestamp of the user's earliest meeting"""
    query = (
        select(Meeting.timestamp)
        .join(UserMeeting)
        .where(
            and_(
                UserMeeting.user_id == user_id,
                or_(
                    UserMeeting.is_owner == True,
                    UserMeeting.access_level != AccessLevel.REMOVED.value
                )
            )
        )
        .order_by(Meeting.timestamp.asc())
        .limit(1)
    )
    
    result = await session.execute(query)
    timestamp = result.scalar_one_or_none()
    return timestamp.isoformat() if timestamp else None

async def get_last_meeting_timestamp(session: AsyncSession, user_id: UUID) -> Optional[str]:
    """Get the timestamp of the user's most recent meeting"""
    query = (
        select(Meeting.timestamp)
        .join(UserMeeting)
        .where(
            and_(
                UserMeeting.user_id == user_id,
                or_(
                    UserMeeting.is_owner == True,
                    UserMeeting.access_level != AccessLevel.REMOVED.value
                )
            )
        )
        .order_by(Meeting.timestamp.desc())
        .limit(1)
    )
    
    result = await session.execute(query)
    timestamp = result.scalar_one_or_none()
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

async def update_user_meetings_access(
    session: AsyncSession,
    owner_id: UUID,
    granted_user_id: UUID,
    access_level: AccessLevel
) -> None:
    """
    Update UserMeeting records for all meetings owned by owner_id to grant 
    specified access level to granted_user_id
    """
    
    # Get all meetings where the owner has owner access
    owner_meetings_query = await session.execute(
        select(Meeting.meeting_id)
        .join(UserMeeting)
        .filter(
            and_(
                UserMeeting.user_id == owner_id,
                UserMeeting.is_owner == True
            )
        )
    )
    owner_meeting_ids = owner_meetings_query.scalars().all()
    
    for meeting_id in owner_meeting_ids:
        # Check if UserMeeting already exists for this user and meeting
        existing_query = await session.execute(
            select(UserMeeting)
            .filter(
                and_(
                    UserMeeting.meeting_id == meeting_id,
                    UserMeeting.user_id == granted_user_id
                )
            )
        )
        existing_access = existing_query.scalar_one_or_none()
        
        if existing_access:
            # Update existing access level
            existing_access.access_level = access_level.value
        else:
            # Create new UserMeeting record
            new_access = UserMeeting(
                meeting_id=meeting_id,
                user_id=granted_user_id,
                access_level=access_level.value,
                created_by=owner_id,
                is_owner=False
            )
            session.add(new_access)
    
    await session.commit()

async def create_share_link(
    session: AsyncSession,
    owner_id: UUID,
    access_level: AccessLevel,
    meeting_ids: Optional[List[UUID]] = None,
    target_email: Optional[str] = None,
    expiration_hours: Optional[int] = 24
) -> str:
    """Create a sharing link with specified access level and optional target email"""
    # Verify ownership of meetings if specific meetings are being shared
    if meeting_ids:
        for meeting_id in meeting_ids:
            user_meeting = await session.execute(
                select(UserMeeting)
                .where(
                    and_(
                        UserMeeting.meeting_id == meeting_id,
                        UserMeeting.user_id == owner_id,
                        UserMeeting.is_owner == True
                    )
                )
            )
            if not user_meeting.scalar_one_or_none():
                raise ValueError(f"User {owner_id} is not the owner of meeting {meeting_id}")

    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=expiration_hours) if expiration_hours else None
    
    share_link = ShareLink(
        token=token,
        owner_id=owner_id,
        target_email=target_email.lower() if target_email else None,
        access_level=access_level.value,
        expires_at=expires_at,
        shared_meetings=meeting_ids
    )
    
    session.add(share_link)
    await session.commit()
    
    return token

async def accept_share_link(
    session: AsyncSession,
    token: str,
    accepting_user_id: UUID,
    accepting_email: Optional[str] = None
) -> bool:
    """Accept a share link and grant access to the accepting user"""
    
    # Get the share link
    share_link_query = select(ShareLink).where(ShareLink.token == token)
    result = await session.execute(share_link_query)
    share_link = result.scalar_one_or_none()
    
    if not share_link:
        return False
        
    # Check if link is expired
    if share_link.expires_at and share_link.expires_at < datetime.now(timezone.utc):
        return False
        
    # Check if link is already used
    if share_link.is_used:
        return False
        
    # Check if target email matches if specified
    if share_link.target_email and accepting_email:
        if share_link.target_email.lower() != accepting_email.lower():
            return False
            
    # Mark link as used
    share_link.is_used = True
    share_link.accepted_by = accepting_user_id
    share_link.accepted_at = datetime.now(timezone.utc)
    
    # Get meetings to share
    meetings = share_link.shared_meetings or []
    
    # Grant access to each meeting
    for meeting_id in meetings:
        # Check if user already has access
        existing_access = await session.execute(
            select(UserMeeting)
            .where(
                and_(
                    UserMeeting.meeting_id == meeting_id,
                    UserMeeting.user_id == accepting_user_id
                )
            )
        )
        existing = existing_access.scalar_one_or_none()
        
        if existing:
            # Update access level if new level is higher
            if AccessLevel(share_link.access_level).value > AccessLevel(existing.access_level).value:
                existing.access_level = share_link.access_level
                existing.created_by = share_link.owner_id
                existing.created_at = datetime.now(timezone.utc)
        else:
            # Create new access
            user_meeting = UserMeeting(
                meeting_id=meeting_id,
                user_id=accepting_user_id,
                access_level=share_link.access_level,
                created_at=datetime.now(timezone.utc),
                created_by=share_link.owner_id,
                is_owner=False
            )
            session.add(user_meeting)
    
    await session.commit()
    return True

# ... existing code ...

async def get_meeting_by_id(session: AsyncSession, meeting_id: UUID) -> Optional[Meeting]:

    query = (
        select(Meeting)
        .options(
            joinedload(Meeting.discussion_points).joinedload(DiscussionPoint.speaker),
            joinedload(Meeting.user_meetings)
        )
        .where(Meeting.meeting_id == meeting_id)
    )
    result = await session.execute(query)
    return result.unique().scalar_one_or_none()

# ... rest of the code ...

# # Update access for all existing meetings
# async with async_session() as session:
#     await update_user_meetings_access(
#         session,
#         owner_id=owner_uuid,
#         granted_user_id=user_uuid,
#         access_level=AccessLevel.SEARCH
#     )


# # Accept share link without updating existing meetings (default behavior)
# success = await accept_share_link(
#     session,
#     token="received_token",
#     accepting_user_id=user_uuid,
#     accepting_email="user@example.com"
# )

# # Accept share link and update existing meetings
# success = await accept_share_link(
#     session,
#     token="received_token",
#     accepting_user_id=user_uuid,
#     accepting_email="user@example.com",
#     update_existing_meetings=True
# )

async def get_meetings_by_user_id(
    user_id: UUID,
    include_transcript: bool = False,
    include_summary: bool = False,
    include_speakers: bool = True,
    limit: int = 100,
    offset: int = 0
) -> tuple[List[dict], int]:
    """
    Get meetings for a user with optional transcript, summary, and speaker fields.
    Returns tuple of (meetings list, total count).
    
    Args:
        user_id: User UUID
        include_transcript: Whether to include meeting transcripts
        include_summary: Whether to include meeting summaries
        include_speakers: Whether to include speaker information
        limit: Maximum number of meetings to return
        offset: Number of meetings to skip
    """
    async with async_session() as session:
        # Select specific columns based on include flags
        columns = [
            Meeting.meeting_id,
            Meeting.timestamp,
            Meeting.meeting_name,
            UserMeeting.access_level,
            UserMeeting.is_owner,
        ]
        
        if include_transcript:
            columns.append(Meeting.transcript)
        if include_summary:
            columns.append(Meeting.meeting_summary)

        # Build base query
        query = (
            select(*columns)
            .join(UserMeeting, Meeting.meeting_id == UserMeeting.meeting_id)
            .where(
                and_(
                    UserMeeting.user_id == user_id,
                    UserMeeting.access_level != AccessLevel.REMOVED.value
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
        rows = result.all()

        # Convert rows to dictionaries
        meetings = []
        for row in rows:
            meeting_dict = {
                "meeting_id": row.meeting_id,
                "timestamp": row.timestamp,
                "meeting_name": row.meeting_name,
                "access_level": row.access_level,
                "is_owner": row.is_owner,
            }
            
            if include_transcript:
                meeting_dict["transcript"] = row.transcript
            if include_summary:
                meeting_dict["meeting_summary"] = row.meeting_summary

            # Add speakers if requested
            if include_speakers:
                # Query to get speakers for this meeting
                speakers_query = (
                    select(Speaker.name)
                    .join(DiscussionPoint, Speaker.id == DiscussionPoint.speaker_id)
                    .where(DiscussionPoint.meeting_id == row.meeting_id)
                    .distinct()
                )
                speakers_result = await session.execute(speakers_query)
                speakers = [speaker[0] for speaker in speakers_result.fetchall()]
                meeting_dict["speakers"] = speakers
                
            meetings.append(meeting_dict)

        return meetings, total_count

async def has_meeting_access(session: AsyncSession, user_id: UUID, meeting_id: UUID) -> bool:
    result = await session.execute(
        select(UserMeeting)
        .where(and_(
            UserMeeting.user_id == user_id,
            UserMeeting.meeting_id == meeting_id,
            UserMeeting.access_level != AccessLevel.REMOVED.value
        ))
    )
    return result.scalar_one_or_none() is not None

async def get_meeting_token(meeting_id: Union[UUID, str], session: AsyncSession = None) -> Optional[str]:
    # Convert string to UUID if needed
    if isinstance(meeting_id, str):
        meeting_id = UUID(meeting_id)
    
    async with (session or get_session()) as session:
        query = select(UserToken.token)\
            .join(UserMeeting, UserToken.user_id == UserMeeting.user_id)\
            .where(
                and_(
                    UserMeeting.meeting_id == meeting_id,
                    UserMeeting.is_owner == True
                )
            )
        result = await session.execute(query)
        return result.scalars().first()

async def get_token_by_email(email: str, session: AsyncSession = None) -> Optional[tuple[str, dict]]:
    async with (session or get_session()) as session:
        query = select(UserToken, User)\
            .join(User, UserToken.user_id == User.id)\
            .where(User.email == email)\
            .order_by(UserToken.last_used_at.desc())\
            .limit(1)
        
        result = await session.execute(query)
        row = result.first()
        if not row:
            return None
        
        token, user = row
        user_data = {
            'id': user.id,
            'email': user.email,
            'username': user.username,
            'first_name': user.first_name,
            'last_name': user.last_name,
            'image': user.image
        }
        return token.token, user_data

async def get_meetings_by_ids(
    meeting_ids: List[Union[UUID, str]], 
    user_id: Union[UUID, str],
    session: AsyncSession = None
) -> dict:
    """Get meetings by list of meeting IDs with associated data
    
    Args:
        meeting_ids: List of meeting UUIDs or strings
        user_id: User UUID or string
        session: Optional AsyncSession
        
    Returns:
        Dict with total count and list of meeting data
    """
    # Convert string IDs to UUID if needed
    if isinstance(user_id, str):
        user_id = UUID(user_id)
    meeting_ids = [UUID(mid) if isinstance(mid, str) else mid for mid in meeting_ids]
    
    async with (session or get_session()) as session:
        # Build query for meetings with speakers
        query = (
            select(
                Meeting.meeting_id,
                Meeting.meeting_name, 
                Meeting.timestamp,
                Meeting.is_indexed,
                Meeting.meeting_summary,
                UserMeeting.access_level,
                UserMeeting.is_owner,
                func.array_agg(distinct(Speaker.name)).label('speakers')
            )
            .join(UserMeeting, Meeting.meeting_id == UserMeeting.meeting_id)
            .join(meeting_speaker_association, Meeting.id == meeting_speaker_association.c.meeting_id)
            .join(Speaker, meeting_speaker_association.c.speaker_id == Speaker.id)
            .where(
                and_(
                    UserMeeting.user_id == user_id,
                    UserMeeting.access_level != AccessLevel.REMOVED.value,
                    Meeting.meeting_id.in_(meeting_ids)
                )
            )
            .group_by(
                Meeting.meeting_id,
                Meeting.meeting_name,
                Meeting.timestamp,
                Meeting.is_indexed,
                Meeting.meeting_summary,
                UserMeeting.access_level,
                UserMeeting.is_owner
            )
        )

        result = await session.execute(query)
        meetings = result.all()
        
        # Create a mapping of meeting_id to meeting data
        meetings_dict = {
            str(meeting.meeting_id): {
                "meeting_id": str(meeting.meeting_id),
                "meeting_name": meeting.meeting_name,
                "timestamp": meeting.timestamp.astimezone(timezone.utc).replace(tzinfo=None),
                "is_indexed": meeting.is_indexed,
                "meeting_summary": meeting.meeting_summary,
                "access_level": meeting.access_level,
                "is_owner": meeting.is_owner,
                "speakers": [s for s in meeting.speakers if s and s != 'TBD']
            }
            for meeting in meetings
        }
        
        # Maintain input order
        meetings_list = [
            meetings_dict[str(mid)]
            for mid in meeting_ids
            if str(mid) in meetings_dict
        ]
        
        return {
            "total": len(meetings_list),
            "meetings": meetings_list
        }

async def clean_meeting_postgres_data(meeting_id: Union[UUID, str], session: AsyncSession = None) -> bool:
    """Clean all PostgreSQL data for a meeting (discussion points, name, summary)
    
    Args:
        meeting_id: Meeting UUID or string
        session: Optional AsyncSession
        
    Returns:
        bool: True if meeting was updated, False if meeting not found
    """
    if isinstance(meeting_id, str):
        meeting_id = UUID(meeting_id)
        
    async with (session or get_session()) as session:
        # Get meeting
        query = select(Meeting).where(Meeting.meeting_id == meeting_id)
        result = await session.execute(query)
        meeting = result.scalar_one_or_none()
        
        if not meeting:
            return False
            
        # Delete discussion points using delete statement
        from sqlalchemy import delete
        delete_stmt = delete(DiscussionPoint).where(DiscussionPoint.meeting_id == meeting_id)
        await session.execute(delete_stmt)
        
        # Clean meeting data
        meeting.meeting_name = None
        meeting.meeting_summary = None
        meeting.is_indexed = False
        
        await session.commit()
        return True
