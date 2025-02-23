import pytest
import pytest_asyncio
import asyncio
import os
import httpx
from datetime import datetime, timezone
from uuid import uuid4
from sqlalchemy import delete, select, or_
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import FastAPI
from main import app
from psql_helpers import get_session

# Add project root to Python path
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from psql_models import (
    User, UserToken, async_session, UserContent, ShareLink, TranscriptAccess, 
    ContentAccess, Entity, Content, content_entity_association, UTMParams, DefaultAccess, Thread, UserEntityAccess, Prompt
)

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture
async def setup_test_users():
    """Create test users with tokens"""
    test_users = [
        ("test1@example.com", "Test User 1"),
        ("test2@example.com", "Test User 2"),
        ("test3@example.com", "Test User 3")
    ]
    
    created_users = []
    async with async_session() as session:
        # Delete all existing users and their related data
        for email, _ in test_users:
            # Get user id
            user_result = await session.execute(
                select(User.id).where(User.email == email)
            )
            user_id = user_result.scalar_one_or_none()
            
            if user_id:
                # Delete in correct order respecting foreign key constraints
                # First delete content_entity associations
                await session.execute(delete(content_entity_association).where(content_entity_association.c.created_by == user_id))
                # Delete UTM params
                await session.execute(delete(UTMParams).where(UTMParams.user_id == user_id))
                # Delete share links
                await session.execute(delete(ShareLink).where(or_(ShareLink.owner_id == user_id, ShareLink.accepted_by == user_id)))
                # Delete default access
                await session.execute(delete(DefaultAccess).where(or_(DefaultAccess.owner_user_id == user_id, DefaultAccess.granted_user_id == user_id)))
                # Delete threads
                await session.execute(delete(Thread).where(Thread.user_id == user_id))
                # Delete user entity access
                await session.execute(delete(UserEntityAccess).where(or_(
                    UserEntityAccess.owner_user_id == user_id,
                    UserEntityAccess.granted_user_id == user_id,
                    UserEntityAccess.granted_by == user_id
                )))
                # Delete transcript access
                await session.execute(delete(TranscriptAccess).where(or_(
                    TranscriptAccess.user_id == user_id,
                    TranscriptAccess.granted_by == user_id
                )))
                # Delete content access
                await session.execute(delete(ContentAccess).where(or_(
                    ContentAccess.user_id == user_id,
                    ContentAccess.granted_by == user_id
                )))
                # Delete user content
                await session.execute(delete(UserContent).where(or_(
                    UserContent.user_id == user_id,
                    UserContent.created_by == user_id
                )))
                # Delete entities owned by user
                await session.execute(delete(Entity).where(Entity.user_id == user_id))
                # Delete prompts
                await session.execute(delete(Prompt).where(Prompt.user_id == user_id))
                # Delete user tokens
                await session.execute(delete(UserToken).where(UserToken.user_id == user_id))
                # Finally delete the user
                await session.execute(delete(User).where(User.id == user_id))
        await session.commit()
        
        # Create new test users
        for email, username in test_users:
            # Create user
            user = User(
                id=uuid4(),
                email=email,
                username=username,
                created_timestamp=datetime.now(timezone.utc)
            )
            session.add(user)
            await session.flush()  # Flush to get the user.id
            
            # Create token
            token = UserToken(
                token=str(uuid4()),
                user_id=user.id,
                created_at=datetime.now(timezone.utc)
            )
            session.add(token)
            created_users.append((user, token))
        
        await session.commit()
        return created_users

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