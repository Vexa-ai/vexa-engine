from sqlalchemy import Column, Integer, String, Text, ForeignKey, DateTime, Float, Table, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, joinedload
from sqlalchemy.dialects.postgresql import UUID as PostgresUUID, ARRAY, JSONB
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
    REMOVED = 'removed'
    SEARCH = 'search'
    TRANSCRIPT = 'transcript'
    OWNER = 'owner'
    


# Rename association tables
content_entity_association = Table('content_entity', Base.metadata,
    Column('content_id', PostgresUUID(as_uuid=True), ForeignKey('content.id')),
    Column('entity_id', Integer, ForeignKey('entities.id'))
)

thread_entity_association = Table('thread_entity', Base.metadata,
    Column('thread_id', String, ForeignKey('threads.thread_id')),
    Column('entity_id', Integer, ForeignKey('entities.id'))
)

    
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
    

class EntityType(str, Enum):
    SPEAKER = 'speaker'


class Entity(Base):
    __tablename__ = 'entities'

    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    type = Column(String(50), nullable=False)
    
    content = relationship('Content', secondary=content_entity_association, back_populates='entities')
    threads = relationship('Thread', secondary=thread_entity_association, back_populates='entities')


class ContentType(str, Enum):
    MEETING = 'meeting'
    DOCUMENT = 'document'
    TITLE = 'title'
    SUMMARY = 'summary'
    CHUNK = 'chunk'

class Content(Base):
    __tablename__ = 'content'

    id = Column(PostgresUUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    content_id = Column(PostgresUUID(as_uuid=True))
    type = Column(String)
    text = Column(String)
    timestamp = Column(DateTime)
    parent_id = Column(PostgresUUID(as_uuid=True), ForeignKey('content.id'), nullable=True)
    is_indexed = Column(Boolean, default=False)
    last_update = Column(DateTime)

    # Self-referential relationship
    parent = relationship('Content', remote_side=[id], backref='children')
    
    # Existing relationships
    user_content = relationship('UserContent', back_populates='content')
    entities = relationship('Entity', secondary=content_entity_association, back_populates='content')

    __table_args__ = (
        Index('idx_content_parent_id', 'parent_id'),
        Index('idx_content_type', 'type'),
    )


class UserContent(Base):
    __tablename__ = 'user_content'

    id = Column(Integer, primary_key=True)
    content_id = Column(PostgresUUID(as_uuid=True), ForeignKey('content.id'))
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


    
class Thread(Base):
    __tablename__ = 'threads'

    thread_id = Column(String, primary_key=True)
    user_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    content_id = Column(PostgresUUID(as_uuid=True), ForeignKey('content.id'), nullable=True)
    prompt_id = Column(Integer, ForeignKey('prompts.id'), nullable=True)
    thread_name = Column(String(255))
    messages = Column(Text)
    timestamp = Column(DateTime(timezone=True), default=datetime.utcnow)

    user = relationship('User')
    content = relationship('Content')
    entities = relationship('Entity', secondary=thread_entity_association, back_populates='threads')
    prompt = relationship('Prompt', back_populates='threads')

    __table_args__ = (
        Index('idx_thread_user_content', 'user_id', 'content_id'),
        Index('idx_thread_prompt', 'prompt_id'),
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

class ShareLink(Base):
    __tablename__ = 'share_links'

    token = Column(String(64), primary_key=True)
    owner_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    target_email = Column(String(255), nullable=True)
    access_level = Column(
        String(20),
        nullable=False,
        default=AccessLevel.SEARCH.value
    )
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    expires_at = Column(DateTime(timezone=True), nullable=True)
    is_used = Column(Boolean, default=False)
    accepted_by = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=True)
    accepted_at = Column(DateTime(timezone=True), nullable=True)
    shared_content = Column(ARRAY(PostgresUUID), nullable=True)  # Array of content IDs

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

class Prompt(Base):
    __tablename__ = 'prompts'

    id = Column(Integer, primary_key=True)
    prompt = Column(Text, nullable=False)
    alias = Column(String(255), nullable=True)
    user_id = Column(PostgresUUID(as_uuid=True), ForeignKey('users.id'), nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    
    user = relationship('User')
    threads = relationship('Thread', back_populates='prompt')

    __table_args__ = (
        Index('idx_prompts_user_id', 'user_id'),
    )


# The following code should be placed inside an async function to avoid the "async" not allowed outside of async function error.

