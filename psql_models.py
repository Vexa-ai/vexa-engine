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
import os
from sqlalchemy.pool import AsyncAdaptedQueuePool

POSTGRES_HOST = os.getenv('POSTGRES_HOST', '127.0.0.1')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'mysecretpassword')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'postgres')

DATABASE_URL = f"postgresql+asyncpg://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

#DATABASE_URL = 'postgresql+asyncpg://postgres:mysecretpassword@localhost:5432/postgres'

# Create async engine with connection pooling
engine = create_async_engine(
    DATABASE_URL,
    poolclass=AsyncAdaptedQueuePool,
    max_overflow=10,
    pool_size=20,
)

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
class AccessLevel(str, Enum):
    REMOVED = 'removed'
    SEARCH = 'search'
    TRANSCRIPT = 'transcript'
    OWNER = 'owner'
    
class User(Base):
    __tablename__ = 'users'

    id = Column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    email = Column(String(255), unique=True, nullable=False)
    username = Column(String(255), nullable=True)
    first_name = Column(String(100), nullable=True)
    last_name = Column(String(100), nullable=True)
    image = Column(String(1024), nullable=True)
    created_timestamp = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_timestamp = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)
    is_indexed = Column(Boolean, default=False, nullable=False, server_default='false')
    
    # Relationships
    default_access_granted = relationship("DefaultAccess", foreign_keys="DefaultAccess.granted_user_id", back_populates="granted_user")
    default_access_owner = relationship("DefaultAccess", foreign_keys="DefaultAccess.owner_user_id", back_populates="owner")

class UserToken(Base):
    __tablename__ = 'user_tokens'

    token = Column(String, primary_key=True)
    user_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    last_used_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    user = relationship('User', backref='tokens')
    
    __table_args__ = (
        Index('idx_user_tokens_user_id', 'user_id'),
    )

class Meeting(Base):
    __tablename__ = 'meetings'

    id = Column(Integer, primary_key=True)
    meeting_id = Column(PostgresUUID(as_uuid=True), unique=True, nullable=False, default=uuid.uuid4)
    transcript = Column(Text)
    timestamp = Column(DateTime, default=datetime.utcnow)
    meeting_name = Column(String(100), nullable=True)
    meeting_summary = Column(Text, nullable=True)
    is_indexed = Column(Boolean, default=False, nullable=False, server_default='false')

    discussion_points = relationship('DiscussionPoint', back_populates='meeting')
    user_meetings = relationship('UserMeeting', back_populates='meeting')
    speakers = relationship('Speaker', secondary=meeting_speaker_association, back_populates='meetings')

class UserMeeting(Base):
    __tablename__ = 'user_meetings'

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
    is_owner = Column(Boolean, nullable=False, default=False)

    meeting = relationship('Meeting', back_populates='user_meetings')
    user = relationship('User', foreign_keys=[user_id])
    creator = relationship('User', foreign_keys=[created_by])

    __table_args__ = (
        UniqueConstraint('meeting_id', 'user_id', name='uq_user_meeting'),
        CheckConstraint(
            access_level.in_([e.value for e in AccessLevel]),
            name='valid_access_level'
        )
    )

class Speaker(Base):
    __tablename__ = 'speakers'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)
    
    discussion_points = relationship('DiscussionPoint', back_populates='speaker')
    meetings = relationship('Meeting', secondary=meeting_speaker_association, back_populates='speakers')


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
    
class Thread(Base):
    __tablename__ = 'threads'

    thread_id = Column(String, primary_key=True)
    user_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    meeting_id = Column(PostgresUUID(as_uuid=True), ForeignKey('meetings.meeting_id'), nullable=True)
    thread_name = Column(String(255))
    messages = Column(Text)  # We'll store JSON serialized messages
    timestamp = Column(DateTime(timezone=True), default=datetime.utcnow)

    user = relationship('User')
    meeting = relationship('Meeting')

    __table_args__ = (
        Index('idx_thread_user_meeting', 'user_id', 'meeting_id'),
    )

class DefaultAccess(Base):
    __tablename__ = 'default_access'

    owner_user_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), primary_key=True)
    granted_user_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), primary_key=True)
    access_level = Column(
        String(20),
        nullable=False,
        default=AccessLevel.SEARCH.value,
        server_default=AccessLevel.SEARCH.value
    )
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    owner = relationship('User', foreign_keys=[owner_user_id])
    granted_user = relationship('User', foreign_keys=[granted_user_id])

    __table_args__ = (
        CheckConstraint(
            access_level.in_([e.value for e in AccessLevel]),
            name='valid_default_access_level'
        ),
    )


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

class ShareLink(Base):
    __tablename__ = 'share_links'

    token = Column(String(64), primary_key=True)
    owner_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    target_email = Column(String(255), nullable=True)  # Optional email for specific recipient
    access_level = Column(
        String(20),
        nullable=False,
        default=AccessLevel.SEARCH.value
    )
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    expires_at = Column(DateTime(timezone=True), nullable=False)
    is_used = Column(Boolean, default=False)
    accepted_by = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=True)
    accepted_at = Column(DateTime(timezone=True), nullable=True)

    owner = relationship('User', foreign_keys=[owner_id])
    accepted_user = relationship('User', foreign_keys=[accepted_by])

    __table_args__ = (
        CheckConstraint(
            access_level.in_([e.value for e in AccessLevel]),
            name='valid_share_link_access_level'
        ),
    )

class UTMParams(Base):
    __tablename__ = 'utm_params'

    id = Column(Integer, primary_key=True)
    user_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    utm_source = Column(String(255), nullable=True)
    utm_medium = Column(String(255), nullable=True)
    utm_campaign = Column(String(255), nullable=True)
    utm_term = Column(String(255), nullable=True)
    utm_content = Column(String(255), nullable=True)
    ref = Column(String(255), nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    user = relationship('User', backref='utm_params')

    __table_args__ = (
        Index('idx_utm_params_user_id', 'user_id'),
    )

# The following code should be placed inside an async function to avoid the "async" not allowed outside of async function error.

