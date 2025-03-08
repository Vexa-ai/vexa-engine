from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, Float, Table, Boolean, Enum as SAEnum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, joinedload
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID, ARRAY, JSONB, JSON
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
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID
from sqlalchemy.dialects.postgresql import ENUM as PostgresENUM

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


class AccessLevel(str, Enum):
    OWNER = 'owner'
    SHARED = 'shared'
    REMOVED = 'removed'
    


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
    content_access = relationship('ContentAccess', foreign_keys='ContentAccess.user_id', back_populates='user')
    transcript_access = relationship('TranscriptAccess', foreign_keys='TranscriptAccess.user_id', back_populates='user')

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
    

class ContentType(str, Enum):
    MEETING = 'meeting'
    TITLE = 'title'
    SUMMARY = 'summary'
    NOTE = 'note'

class ExternalIDType(str, Enum):
    GOOGLE_MEET = 'google_meet'
    NONE = 'none'

class Content(Base):
    __tablename__ = 'content'

    id = Column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    type = Column(String)
    text = Column(String)
    timestamp = Column(DateTime(timezone=True))
    external_id = Column(String, nullable=True)
    external_id_type = Column(String, nullable=True, server_default=ExternalIDType.NONE.value)
    last_update = Column(DateTime(timezone=True))
    parent_id = Column(PostgresUUID(as_uuid=True), ForeignKey('content.id'), nullable=True)
    is_indexed = Column(Boolean, default=False)
    content_metadata = Column(JSON, nullable=True)

    # Self-referential relationship
    parent = relationship('Content', remote_side=[id], backref='children')
    
    # Existing relationships
    user_content = relationship('UserContent', back_populates='content')
    transcripts = relationship('Transcript', back_populates='content', cascade='all, delete-orphan')
    user_access = relationship('ContentAccess', back_populates='content', cascade='all, delete-orphan')

    __table_args__ = (
        Index('idx_content_parent_id', 'parent_id'),
        Index('idx_content_type', 'type'),
        Index('idx_content_external_id', 'external_id'),
        Index('idx_content_external_id_type', 'external_id_type'),
        UniqueConstraint('external_id', 'external_id_type', name='uq_content_external_id'),
        CheckConstraint(
            type.in_([e.value for e in ContentType]),
            name='valid_content_type'
        ),
        CheckConstraint(
            external_id_type.in_([e.value for e in ExternalIDType]),
            name='valid_external_id_type'
        ),
    )


class UserContent(Base):
    __tablename__ = 'user_content'

    id = Column(Integer, primary_key=True)
    content_id = Column(PostgresUUID(as_uuid=True), ForeignKey('content.id'))
    user_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'))
    access_level = Column(
        String(20),
        nullable=False,
        default=AccessLevel.SHARED.value,
        server_default=AccessLevel.SHARED.value
    )
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    created_by = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'))
    is_owner = Column(Boolean, nullable=False, default=False)

    content = relationship('Content', back_populates='user_content')
    user = relationship('User', foreign_keys=[user_id])
    creator = relationship('User', foreign_keys=[created_by])

    __table_args__ = (
        UniqueConstraint('content_id', 'user_id', name='uq_user_content'),
        CheckConstraint(
            access_level.in_([e.value for e in AccessLevel]),
            name='valid_access_level'
        )
    )

class Transcript(Base):
    __tablename__ = 'transcript'
    id = Column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    content_id = Column(PostgresUUID(as_uuid=True), ForeignKey('content.id'), nullable=False)
    text_content = Column(String, nullable=True)
    speaker = Column(String, nullable=True)
    start_timestamp = Column(DateTime(timezone=True), nullable=False)
    end_timestamp = Column(DateTime(timezone=True), nullable=False)
    confidence = Column(Float, nullable=False)
    word_timing_data = Column(JSON, nullable=True)
    segment_metadata = Column(JSON, nullable=True)
    # Relationships
    content = relationship('Content', back_populates='transcripts')
    user_access = relationship('TranscriptAccess', back_populates='transcript', cascade='all, delete-orphan')

class ContentAccess(Base):
    __tablename__ = 'content_access'
    id = Column(Integer, primary_key=True)
    content_id = Column(PostgresUUID(as_uuid=True), ForeignKey('content.id'), nullable=False)
    user_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    access_level = Column(PostgresENUM('owner', 'shared', 'removed', name='accesslevelenum'), nullable=False)
    granted_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    granted_by = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    # Relationships
    content = relationship('Content', back_populates='user_access')
    user = relationship('User', foreign_keys=[user_id])
    granter = relationship('User', foreign_keys=[granted_by])

class TranscriptAccess(Base):
    __tablename__ = 'transcript_access'
    id = Column(Integer, primary_key=True)
    transcript_id = Column(PostgresUUID(as_uuid=True), ForeignKey('transcript.id'), nullable=False)
    user_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    access_level = Column(PostgresENUM('owner', 'shared', 'removed', name='accesslevelenum'), nullable=False)
    granted_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    granted_by = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    # Relationships
    transcript = relationship('Transcript', back_populates='user_access')
    user = relationship('User', foreign_keys=[user_id])
    granter = relationship('User', foreign_keys=[granted_by])