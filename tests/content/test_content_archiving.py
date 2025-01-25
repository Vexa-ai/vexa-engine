import pytest
import pytest_asyncio
import asyncio
from uuid import uuid4, UUID
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_
from psql_models import Content, UserContent, ContentType, AccessLevel, User
from psql_helpers import get_session
from psql_access import mark_content_deleted, cleanup_search_indices, get_accessible_content, archive_content, get_archived_content, restore_content
from unittest.mock import patch, AsyncMock

@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest_asyncio.fixture
async def test_session():
    async with get_session() as session:
        await session.begin()
        yield session
        await session.rollback()

@pytest.fixture
def test_user_ids():
    return [UUID(str(uuid4())) for _ in range(2)]

@pytest_asyncio.fixture
async def test_users(test_user_ids, test_session):
    for i, user_id in enumerate(test_user_ids):
        user = User(
            id=user_id,
            email=f"{user_id}@test.com",
            username=f"Test User {i}",
            first_name=f"Test{i}",
            last_name="User"
        )
        test_session.add(user)
    await test_session.commit()
    return test_user_ids

@pytest_asyncio.fixture
async def test_content(test_users, test_session):
    content_id = UUID(str(uuid4()))
    
    # Create test content
    content = Content(
        id=content_id,
        content_id=content_id,
        type=ContentType.NOTE.value,
        timestamp=datetime.now(),
        is_indexed=True
    )
    test_session.add(content)
    await test_session.flush()
    
    # Add user access
    for i, user_id in enumerate(test_users):
        user_content = UserContent(
            content_id=content_id,
            user_id=user_id,
            access_level=AccessLevel.OWNER.value if i == 0 else AccessLevel.TRANSCRIPT.value
        )
        test_session.add(user_content)
    
    await test_session.commit()
    return content_id

@pytest.mark.asyncio
async def test_basic_archiving(test_users, test_content, test_session):
    owner_id = test_users[0]
    
    # Check initial state
    result = await test_session.execute(
        select(UserContent)
        .where(UserContent.content_id == test_content)
    )
    initial_access = result.scalars().all()
    print("\nInitial access levels:")
    for access in initial_access:
        print(f"User {access.user_id}: {access.access_level}")
    
    # Check content state
    result = await test_session.execute(
        select(Content).where(Content.id == test_content)
    )
    content = result.scalar_one()
    print(f"\nContent state before deletion:")
    print(f"ID: {content.id}")
    print(f"Type: {content.type}")
    print(f"Is indexed: {content.is_indexed}")
    
    # Archive content for owner
    result = await mark_content_deleted(test_session, owner_id, test_content)
    assert result is True
    
    # Check state after deletion
    result = await test_session.execute(
        select(UserContent)
        .where(UserContent.content_id == test_content)
    )
    after_delete = result.scalars().all()
    print("\nAccess levels after deletion:")
    for access in after_delete:
        print(f"User {access.user_id}: {access.access_level}")
    
    # Check content state after deletion
    result = await test_session.execute(
        select(Content).where(Content.id == test_content)
    )
    content = result.scalar_one()
    print(f"\nContent state after deletion:")
    print(f"ID: {content.id}")
    print(f"Type: {content.type}")
    print(f"Is indexed: {content.is_indexed}")
    
    # Verify content is archived for owner
    contents, _ = await get_accessible_content(owner_id, session=test_session)
    assert not any(c['content_id'] == str(test_content) for c in contents)
    
    # Debug query for other user's content
    result = await test_session.execute(
        select(Content, UserContent.access_level)
        .join(UserContent, or_(
            UserContent.content_id == Content.id,
            UserContent.content_id == Content.content_id
        ))
        .where(and_(
            UserContent.user_id == test_users[1],
            Content.type == content.type,
            UserContent.access_level != AccessLevel.REMOVED.value
        ))
    )
    debug_rows = result.all()
    print("\nDebug query results:")
    for row in debug_rows:
        print(f"Content ID: {row.Content.id}")
        print(f"Content Type: {row.Content.type}")
        print(f"Access Level: {row.access_level}")
    
    # Verify content still accessible for other user
    other_user_contents, _ = await get_accessible_content(test_users[1], content_type=ContentType.NOTE, session=test_session)
    print("\nOther user's accessible content:")
    for c in other_user_contents:
        print(f"Content ID: {c['content_id']}")
        print(f"Type: {c['type']}")
        print(f"Access Level: {c['access_level']}")
    
    assert any(c['content_id'] == str(test_content) for c in other_user_contents)

@pytest.mark.asyncio
async def test_search_cleanup(test_users, test_content, test_session):
    owner_id = test_users[0]
    
    # Mock search engine cleanup
    with patch('psql_access.qdrant_engine.delete_points', new_callable=AsyncMock) as mock_qdrant, \
         patch('psql_access.es_engine.delete_documents') as mock_es:
        
        mock_qdrant.return_value = True
        mock_es.return_value = True
        
        # Archive content
        await mark_content_deleted(test_session, owner_id, test_content)
        
        # Clean up search indices
        result = await cleanup_search_indices(test_session, test_content)
        assert result is True
        
        # Verify search engines were called
        mock_qdrant.assert_called_once()
        mock_es.assert_called_once()

@pytest.mark.asyncio
async def test_error_cases(test_users, test_session):
    non_existent_content = UUID(str(uuid4()))
    
    # Try archiving non-existent content
    result = await mark_content_deleted(test_session, test_users[0], non_existent_content)
    assert result is False 

@pytest.mark.asyncio
async def test_archive_management(test_users, test_content, test_session):
    owner_id = test_users[0]
    
    # Archive content
    result = await archive_content(test_session, owner_id, test_content, cleanup_search=False)
    assert result is True
    
    # Get archived content
    archived_contents, count = await get_archived_content(owner_id, session=test_session)
    assert count == 1
    assert any(c['content_id'] == str(test_content) for c in archived_contents)
    
    # Get archived content with type filter
    archived_contents, count = await get_archived_content(
        owner_id,
        content_type=ContentType.NOTE,
        session=test_session
    )
    assert count == 1
    assert any(c['content_id'] == str(test_content) for c in archived_contents)
    
    # Get archived content with wrong type filter
    archived_contents, count = await get_archived_content(
        owner_id,
        content_type=ContentType.MEETING,
        session=test_session
    )
    assert count == 0
    assert len(archived_contents) == 0
    
    # Restore content
    result = await restore_content(
        test_session,
        owner_id,
        test_content,
        restore_access_level=AccessLevel.TRANSCRIPT
    )
    assert result is True
    
    # Verify content is accessible again
    contents, _ = await get_accessible_content(
        owner_id,
        content_type=ContentType.NOTE,
        session=test_session
    )
    assert any(c['content_id'] == str(test_content) for c in contents)
    
    # Verify content is no longer in archive
    archived_contents, count = await get_archived_content(owner_id, session=test_session)
    assert count == 0
    assert not any(c['content_id'] == str(test_content) for c in archived_contents) 