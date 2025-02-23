import pytest
import pytest_asyncio
import asyncio
import os
import httpx
from datetime import datetime, timezone
from uuid import uuid4
from sqlalchemy import delete, select, or_
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker
from typing import Generator, AsyncGenerator
from fastapi.testclient import TestClient

# Add project root to Python path
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Now import app and models after adding to path
from main import app
from psql_helpers import get_session
from psql_models import (
    Base, User, UserToken, UserContent, ShareLink, TranscriptAccess,
    ContentAccess, Entity, Content, content_entity_association, UTMParams, 
    DefaultAccess, Thread, UserEntityAccess, Prompt
)

# Test database URL
TEST_DATABASE_URL = "postgresql+asyncpg://postgres:mysecretpassword_dev@postgres/test_db"

@pytest.fixture(scope="session")
def event_loop() -> Generator:
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture(scope="session")
async def test_engine():
    engine = create_async_engine(TEST_DATABASE_URL, echo=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()

@pytest.fixture
async def test_session(test_engine) -> AsyncGenerator[AsyncSession, None]:
    async_session = sessionmaker(test_engine, class_=AsyncSession, expire_on_commit=False)
    async with async_session() as session:
        yield session

@pytest.fixture
async def client() -> AsyncGenerator[AsyncClient, None]:
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client

@pytest_asyncio.fixture
async def setup_test_users(test_session):
    """Create test users with tokens"""
    test_users = [
        ("test1@example.com", "Test User 1"),
        ("test2@example.com", "Test User 2"),
        ("test3@example.com", "Test User 3")
    ]
    
    created_users = []
    try:
        # Delete all existing users and their related data
        for email, _ in test_users:
            # Get user id
            user_result = await test_session.execute(
                select(User.id).where(User.email == email)
            )
            user_id = user_result.scalar_one_or_none()
            
            if user_id:
                # Delete in correct order respecting foreign key constraints
                await test_session.execute(delete(content_entity_association).where(content_entity_association.c.created_by == user_id))
                await test_session.execute(delete(UTMParams).where(UTMParams.user_id == user_id))
                await test_session.execute(delete(ShareLink).where(or_(ShareLink.owner_id == user_id, ShareLink.accepted_by == user_id)))
                await test_session.execute(delete(DefaultAccess).where(or_(DefaultAccess.owner_user_id == user_id, DefaultAccess.granted_user_id == user_id)))
                await test_session.execute(delete(Thread).where(Thread.user_id == user_id))
                await test_session.execute(delete(UserEntityAccess).where(or_(
                    UserEntityAccess.owner_user_id == user_id,
                    UserEntityAccess.granted_user_id == user_id,
                    UserEntityAccess.granted_by == user_id
                )))
                await test_session.execute(delete(TranscriptAccess).where(or_(
                    TranscriptAccess.user_id == user_id,
                    TranscriptAccess.granted_by == user_id
                )))
                await test_session.execute(delete(ContentAccess).where(or_(
                    ContentAccess.user_id == user_id,
                    ContentAccess.granted_by == user_id
                )))
                await test_session.execute(delete(UserContent).where(or_(
                    UserContent.user_id == user_id,
                    UserContent.created_by == user_id
                )))
                await test_session.execute(delete(Entity).where(Entity.user_id == user_id))
                await test_session.execute(delete(Prompt).where(Prompt.user_id == user_id))
                await test_session.execute(delete(UserToken).where(UserToken.user_id == user_id))
                await test_session.execute(delete(User).where(User.id == user_id))
        
        # Create new test users
        for email, username in test_users:
            user = User(
                id=uuid4(),
                email=email,
                username=username,
                created_timestamp=datetime.now(timezone.utc)
            )
            test_session.add(user)
            await test_session.flush()
            
            token = UserToken(
                token=str(uuid4()),
                user_id=user.id,
                created_at=datetime.now(timezone.utc)
            )
            test_session.add(token)
            created_users.append((user, token))
        
        await test_session.commit()
        return created_users
    except Exception as e:
        await test_session.rollback()
        raise e

@pytest_asyncio.fixture(autouse=True)
async def setup_test_env(setup_test_users):
    """Automatically setup test environment"""
    return setup_test_users  # Return directly instead of yield 

@pytest_asyncio.fixture
async def client() -> AsyncClient:
    """Create async client for testing endpoints"""
    async with AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
        yield ac

@pytest_asyncio.fixture
async def test_session() -> AsyncSession:
    """Create test database session"""
    async with get_session() as session:
        yield session 