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
#docker run --name dima_entities -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres


DATABASE_URL = 'postgresql+asyncpg://postgres:mysecretpassword@localhost:5432/postgres'
# Create async engine
engine = create_async_engine(DATABASE_URL)

# Create async session
async_session = sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

Base = declarative_base()

# Association table for the many-to-many relationship between meetings and speakers
meeting_speaker_association = Table('meeting_speaker', Base.metadata,
    Column('meeting_id', Integer, ForeignKey('meetings.id')),
    Column('speaker_id', Integer, ForeignKey('speakers.id'))
)

class Speaker(Base):
    __tablename__ = 'speakers'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    
    discussion_points = relationship('DiscussionPoint', back_populates='speaker')

class Meeting(Base):
    __tablename__ = 'meetings'

    id = Column(Integer, primary_key=True)
    meeting_id = Column(PostgresUUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4)
    transcript = Column(Text)
    timestamp = Column(DateTime, default=datetime.utcnow)
    owner_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=False)  # Changed to nullable=False

    discussion_points = relationship('DiscussionPoint', back_populates='meeting')
    shares = relationship('MeetingShare', back_populates='meeting')
    owner = relationship('User')

class DiscussionPoint(Base):
    __tablename__ = 'discussion_points'

    id = Column(Integer, primary_key=True)
    summary_index = Column(Integer)
    summary = Column(Text)
    details = Column(Text)
    referenced_text = Column(Text)
    meeting_id = Column(PostgresUUID(as_uuid=True), ForeignKey('meetings.meeting_id'))
    speaker_id = Column(Integer, ForeignKey('speakers.id'))
    topic_name = Column(String(255))
    topic_type = Column(String(50))
    model = Column(String(50))

    meeting = relationship('Meeting', back_populates='discussion_points')
    speaker = relationship('Speaker', back_populates='discussion_points')

class Output(Base):
    __tablename__ = 'outputs'

    id = Column(Integer, primary_key=True)
    input_text = Column(Text, nullable=False)
    output_text = Column(Text)
    model = Column(String(100), nullable=False)
    temperature = Column(Float, nullable=False)
    max_tokens = Column(Integer, nullable=False)
    cache_key = Column(String(64), unique=True, nullable=False)

    def __repr__(self):
        return f"<Output(id={self.id}, model='{self.model}', cache_key='{self.cache_key}')>"

    @staticmethod
    def generate_cache_key(input_text: str, model: str, temperature: float, max_tokens: int) -> str:
        key = f"{input_text}_{model}_{temperature}_{max_tokens}"
        return hashlib.md5(key.encode()).hexdigest()

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
            MeetingShare.user_id == user_id,
            MeetingShare.access_level.in_([AccessLevel.SEARCH.value, AccessLevel.TRANSCRIPT.value])
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

# Add these new imports
from enum import Enum
from sqlalchemy import Index, UniqueConstraint, CheckConstraint

class AccessLevel(str, Enum):
    REMOVED = 'removed'
    SEARCH = 'search'
    TRANSCRIPT = 'transcript'

class User(Base):
    __tablename__ = 'users'

    id = Column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String(100), nullable=False)
    email = Column(String(255), unique=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

class UserToken(Base):
    __tablename__ = 'user_tokens'

    token = Column(String, primary_key=True)
    user_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    user_name = Column(String, nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    last_used_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    user = relationship('User', backref='tokens')
    
    __table_args__ = (
        Index('idx_user_tokens_user_id', 'user_id'),
    )

class MeetingShare(Base):
    __tablename__ = 'meeting_shares'

    id = Column(Integer, primary_key=True)
    meeting_id = Column(PostgresUUID(as_uuid=True), ForeignKey('meetings.meeting_id'))
    user_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'))
    access_level = Column(
        String(20),
        nullable=False,
        default=AccessLevel.SEARCH.value,
        server_default=AccessLevel.SEARCH.value
    )
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    created_by = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'))

    meeting = relationship('Meeting', back_populates='shares')
    user = relationship('User', foreign_keys=[user_id])
    creator = relationship('User', foreign_keys=[created_by])

    __table_args__ = (
        UniqueConstraint('meeting_id', 'user_id', name='uq_meeting_user_share'),
        CheckConstraint(
            access_level.in_([e.value for e in AccessLevel]),
            name='valid_access_level'
        )
    )

# Helper functions for access control
async def get_user_meeting_access(session: AsyncSession, user_id: UUID, meeting_id: UUID) -> AccessLevel:
    """Get user's access level for a meeting"""
    # First check if user is the owner
    meeting = await session.execute(
        select(Meeting).filter_by(meeting_id=meeting_id, owner_id=user_id)
    )
    if meeting.scalar_one_or_none():
        return AccessLevel.TRANSCRIPT

    # Then check shared access
    result = await session.execute(
        select(MeetingShare.access_level)
        .filter_by(meeting_id=meeting_id, user_id=user_id)
    )
    access = result.scalar_one_or_none()
    return AccessLevel(access) if access else AccessLevel.REMOVED

async def can_access_transcript(session: AsyncSession, user_id: UUID, meeting_id: UUID) -> bool:
    """Check if user has transcript-level access"""
    access = await get_user_meeting_access(session, user_id, meeting_id)
    return access == AccessLevel.TRANSCRIPT

async def can_access_search(session: AsyncSession, user_id: UUID, meeting_id: UUID) -> bool:
    """Check if user has search-level access"""
    access = await get_user_meeting_access(session, user_id, meeting_id)
    return access in [AccessLevel.SEARCH, AccessLevel.TRANSCRIPT]

class Thread(Base):
    __tablename__ = 'threads'

    thread_id = Column(String, primary_key=True)
    user_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    thread_name = Column(String(255))
    messages = Column(Text)  # We'll store JSON serialized messages
    timestamp = Column(DateTime(timezone=True), default=datetime.utcnow)

    user = relationship('User')

# Add these new methods to the file

async def get_first_meeting_timestamp(session: AsyncSession, user_id: UUID) -> Optional[str]:
    """Get the timestamp of the user's earliest meeting"""
    subquery = (
        select(MeetingShare.meeting_id)
        .where(
            and_(
                MeetingShare.user_id == user_id,
                MeetingShare.access_level != 'removed'
            )
        )
        .scalar_subquery()
    )

    query = (
        select(Meeting.timestamp)
        .where(
            or_(
                Meeting.owner_id == user_id,
                Meeting.meeting_id.in_(subquery)  # Changed from Meeting.id to Meeting.meeting_id
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
    subquery = (
        select(MeetingShare.meeting_id)
        .where(
            and_(
                MeetingShare.user_id == user_id,
                MeetingShare.access_level != 'removed'
            )
        )
        .scalar_subquery()
    )

    query = (
        select(Meeting.timestamp)
        .where(
            or_(
                Meeting.owner_id == user_id,
                Meeting.meeting_id.in_(subquery)  # Changed from Meeting.id to Meeting.meeting_id
            )
        )
        .order_by(Meeting.timestamp.desc())
        .limit(1)
    )
    
    result = await session.execute(query)
    timestamp = result.scalar_one_or_none()
    return timestamp.isoformat() if timestamp else None
