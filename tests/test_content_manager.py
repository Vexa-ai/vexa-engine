import pytest
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4
from typing import List, Dict
from content_manager import ContentManager, ContentData
from psql_models import (
    Content, UserContent, Entity, ContentType, 
    EntityType, AccessLevel, async_session
)
from sqlalchemy import select, and_
from unittest.mock import patch, MagicMock
from indexing.redis_keys import RedisKeys

@pytest.mark.asyncio
async def test_add_content_basic(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Test creating new content
    content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.NOTE.value,
        text="Test content",
        entities=[{"name": "Test Entity", "type": EntityType.SPEAKER.value}]
    )
    
    assert content_id is not None
    
    # Verify in database
    async with async_session() as session:
        result = await session.execute(
            select(Content)
            .join(UserContent)
            .where(Content.id == UUID(content_id))
        )
        content = result.scalar_one()
        assert content.text == "Test content"
        assert content.type == ContentType.NOTE.value
        
        # Verify user content record
        user_content = await session.execute(
            select(UserContent).where(
                and_(
                    UserContent.content_id == content.id,
                    UserContent.user_id == test_user.id
                )
            )
        )
        uc = user_content.scalar_one()
        assert uc.is_owner is True
        assert uc.access_level == AccessLevel.OWNER.value

@pytest.mark.asyncio
async def test_add_content_with_entities(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    entities = [
        {"name": "Speaker 1", "type": EntityType.SPEAKER.value},
        {"name": "Tag 1", "type": EntityType.TAG.value}
    ]
    
    content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.NOTE.value,
        text="Test with entities",
        entities=entities
    )
    
    # Verify entities in database
    async with async_session() as session:
        result = await session.execute(
            select(Entity)
            .join(Content.entities)
            .where(Content.id == UUID(content_id))
        )
        db_entities = result.scalars().all()
        assert len(db_entities) == 2
        entity_names = {e.name for e in db_entities}
        entity_types = {e.type for e in db_entities}
        assert "Speaker 1" in entity_names
        assert "Tag 1" in entity_names
        assert EntityType.SPEAKER.value in entity_types
        assert EntityType.TAG.value in entity_types

@pytest.mark.asyncio
async def test_get_contents_pagination(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Create multiple contents
    content_ids = []
    for i in range(5):
        content_id = await manager.add_content(
            user_id=str(test_user.id),
            type=ContentType.NOTE.value,
            text=f"Test content {i}"
        )
        content_ids.append(content_id)
    
    # Test pagination
    page1 = await manager.get_contents(
        user_id=str(test_user.id),
        limit=2,
        offset=0
    )
    page2 = await manager.get_contents(
        user_id=str(test_user.id),
        limit=2,
        offset=2
    )
    
    assert len(page1["contents"]) == 2
    assert len(page2["contents"]) == 2
    assert page1["contents"][0]["content_id"] != page2["contents"][0]["content_id"]
    assert page1["total"] >= 5

@pytest.mark.asyncio
async def test_get_contents_filters(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Create content with specific type and entities
    content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.NOTE.value,
        text="Test content",
        entities=[{"name": "Test Speaker", "type": EntityType.SPEAKER.value}]
    )
    
    # Test type filter
    type_filtered = await manager.get_contents(
        user_id=str(test_user.id),
        content_type=ContentType.NOTE.value
    )
    assert len(type_filtered["contents"]) > 0
    assert all(c["type"] == ContentType.NOTE.value for c in type_filtered["contents"])
    
    # Test entity filter
    entity_filtered = await manager.get_contents(
        user_id=str(test_user.id),
        filters=[{
            "type": "speakers",
            "values": ["Test Speaker"]
        }]
    )
    assert len(entity_filtered["contents"]) > 0
    assert any(
        any(e["name"] == "Test Speaker" for e in c["entities"])
        for c in entity_filtered["contents"]
    )

@pytest.mark.asyncio
async def test_modify_content(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Create initial content
    content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.NOTE.value,
        text="Initial text",
        entities=[{"name": "Initial Entity", "type": EntityType.SPEAKER.value}]
    )
    
    # Modify content
    success = await manager.modify_content(
        user_id=str(test_user.id),
        content_id=UUID(content_id),
        text="Modified text",
        entities=[{"name": "Modified Entity", "type": EntityType.SPEAKER.value}]
    )
    assert success is True
    
    # Verify modifications
    content = await manager.get_content(
        user_id=str(test_user.id),
        content_id=UUID(content_id)
    )
    assert content is not None
    assert content.text == "Modified text"
    assert len(content.entities) == 1
    assert content.entities[0]["name"] == "Modified Entity"

@pytest.mark.asyncio
async def test_delete_restore_content(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Create content
    content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.NOTE.value,
        text="Test content"
    )
    
    # Delete content
    success = await manager.delete_content(
        user_id=str(test_user.id),
        content_id=UUID(content_id)
    )
    assert success is True
    
    # Verify content is not accessible
    content = await manager.get_content(
        user_id=str(test_user.id),
        content_id=UUID(content_id)
    )
    assert content is None
    
    # Restore content
    success = await manager.restore_content(
        user_id=str(test_user.id),
        content_id=UUID(content_id)
    )
    assert success is True
    
    # Verify content is accessible again
    content = await manager.get_content(
        user_id=str(test_user.id),
        content_id=UUID(content_id)
    )
    assert content is not None

@pytest.mark.asyncio
async def test_ownership_filters(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    other_user, other_token = users[1]
    
    manager = await ContentManager.create()
    
    # Create content for test user
    own_content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.NOTE.value,
        text="Own content"
    )
    
    # Create content for other user and share with test user
    shared_content_id = await manager.add_content(
        user_id=str(other_user.id),
        type=ContentType.NOTE.value,
        text="Shared content"
    )
    
    # Add sharing logic here when implemented
    
    # Test "my" ownership filter
    my_contents = await manager.get_contents(
        user_id=str(test_user.id),
        ownership="my"
    )
    assert len(my_contents["contents"]) > 0
    assert all(c["is_owner"] for c in my_contents["contents"])
    
    # Test "shared" ownership filter
    shared_contents = await manager.get_contents(
        user_id=str(test_user.id),
        ownership="shared"
    )
    assert all(not c["is_owner"] for c in shared_contents["contents"])

@pytest.mark.asyncio
async def test_get_content_not_found(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Test with non-existent content ID
    content = await manager.get_content(
        user_id=str(test_user.id),
        content_id=uuid4()
    )
    assert content is None

@pytest.mark.asyncio
async def test_modify_content_no_access(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    other_user, other_token = users[1]
    
    manager = await ContentManager.create()
    
    # Create content for other user
    content_id = await manager.add_content(
        user_id=str(other_user.id),
        type=ContentType.NOTE.value,
        text="Other user's content"
    )
    
    # Try to modify as test user
    success = await manager.modify_content(
        user_id=str(test_user.id),
        content_id=UUID(content_id),
        text="Modified text"
    )
    assert success is False

@pytest.mark.asyncio
async def test_create_share_link(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Create content to share
    content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.NOTE.value,
        text="Test content"
    )
    
    # Create share link
    token = await manager.create_share_link(
        owner_id=str(test_user.id),
        access_level=AccessLevel.TRANSCRIPT,
        content_ids=[UUID(content_id)]
    )
    
    assert token is not None
    assert isinstance(token, str)

@pytest.mark.asyncio
async def test_accept_share_link(setup_test_users):
    users = setup_test_users
    owner, owner_token = users[0]
    accepter, accepter_token = users[1]
    
    manager = await ContentManager.create()
    
    # Create content and share link
    content_id = await manager.add_content(
        user_id=str(owner.id),
        type=ContentType.NOTE.value,
        text="Test content"
    )
    
    token = await manager.create_share_link(
        owner_id=str(owner.id),
        access_level=AccessLevel.TRANSCRIPT,
        content_ids=[UUID(content_id)]
    )
    
    # Accept share link
    success = await manager.accept_share_link(
        token=token,
        accepting_user_id=str(accepter.id)
    )
    
    assert success is True
    
    # Verify access
    content = await manager.get_content(
        user_id=str(accepter.id),
        content_id=UUID(content_id)
    )
    assert content is not None

@pytest.mark.asyncio
async def test_queue_content_indexing(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Create content
    content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.NOTE.value,
        text="Test content"
    )
    
    with patch('content_manager.redis_client') as mock_redis:
        # Mock Redis responses
        mock_redis.sismember.return_value = False
        mock_redis.zscore.return_value = None
        mock_redis.hget.return_value = None
        
        # Queue content for indexing
        result = await manager.queue_content_indexing(
            user_id=str(test_user.id),
            content_id=UUID(content_id)
        )
        
        assert result["status"] == "queued"
        assert "message" in result
        
        # Verify Redis calls
        mock_redis.sismember.assert_called_once_with(RedisKeys.PROCESSING_SET, str(content_id))
        mock_redis.zscore.assert_called_once_with(RedisKeys.INDEXING_QUEUE, str(content_id))
        mock_redis.zadd.assert_called_once()

@pytest.mark.asyncio
async def test_queue_content_indexing_already_processing(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Create content
    content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.NOTE.value,
        text="Test content"
    )
    
    with patch('content_manager.redis_client') as mock_redis:
        # Mock Redis to indicate content is being processed
        mock_redis.sismember.return_value = True
        
        # Attempt to queue content
        result = await manager.queue_content_indexing(
            user_id=str(test_user.id),
            content_id=UUID(content_id)
        )
        
        assert result["status"] == "already_processing"
        assert "message" in result
        
        # Verify only sismember was called
        mock_redis.sismember.assert_called_once_with(RedisKeys.PROCESSING_SET, str(content_id))
        mock_redis.zscore.assert_not_called()
        mock_redis.zadd.assert_not_called()

@pytest.mark.asyncio
async def test_queue_content_indexing_no_access(setup_test_users):
    users = setup_test_users
    owner, owner_token = users[0]
    other_user, other_token = users[1]
    
    manager = await ContentManager.create()
    
    # Create content
    content_id = await manager.add_content(
        user_id=str(owner.id),
        type=ContentType.NOTE.value,
        text="Test content"
    )
    
    # Try to queue content as other user
    with pytest.raises(ValueError, match="No access to content"):
        await manager.queue_content_indexing(
            user_id=str(other_user.id),
            content_id=UUID(content_id)
        ) 