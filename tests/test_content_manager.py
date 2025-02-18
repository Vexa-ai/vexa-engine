import pytest
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4
from typing import List, Dict
from content_manager import ContentManager, ContentData
from psql_models import (
    Content, UserContent, Entity, ContentType, 
    EntityType, AccessLevel, async_session, content_entity_association
)
from sqlalchemy import select, and_
from unittest.mock import patch, MagicMock, AsyncMock
from indexing.redis_keys import RedisKeys
from vexa import VexaAPI, VexaAPIError

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
            "type": EntityType.SPEAKER.value,
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

@pytest.mark.asyncio
async def test_physical_delete_content(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    manager = await ContentManager.create()
    # Create content
    content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.NOTE.value,
        text="Test content"
    )
    # Try to delete non-archived content - should fail
    success = await manager.delete_content(
        user_id=str(test_user.id),
        content_id=UUID(content_id),
        physical_delete=True
    )
    assert success is False
    # Verify content still exists
    async with async_session() as session:
        result = await session.execute(
            select(Content).where(Content.id == UUID(content_id))
        )
        content = result.scalar_one()
        assert content is not None
    # Archive content first
    await manager.archive_content(
        user_id=str(test_user.id),
        content_id=UUID(content_id)
    )
    # Now physical delete should work
    success = await manager.delete_content(
        user_id=str(test_user.id),
        content_id=UUID(content_id),
        physical_delete=True
    )
    assert success is True
    # Verify content is physically deleted
    async with async_session() as session:
        result = await session.execute(
            select(Content).where(Content.id == UUID(content_id))
        )
        content = result.scalar_one_or_none()
        assert content is None

@pytest.mark.asyncio
async def test_add_content_invalid_type(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Test invalid content type
    with pytest.raises(ValueError, match="Invalid content type"):
        await manager.add_content(
            user_id=str(test_user.id),
            type="invalid_type",
            text="Test content"
        )

@pytest.mark.asyncio
async def test_add_content_invalid_entity_type(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Test invalid entity type
    with pytest.raises(ValueError, match="Invalid entity type"):
        await manager.add_content(
            user_id=str(test_user.id),
            type=ContentType.NOTE.value,
            text="Test content",
            entities=[{"name": "Test Entity", "type": "invalid_type"}]
        )
    
    # Test missing entity name
    with pytest.raises(ValueError, match="Entity name is required"):
        await manager.add_content(
            user_id=str(test_user.id),
            type=ContentType.NOTE.value,
            text="Test content",
            entities=[{"type": EntityType.SPEAKER.value}]
        )

@pytest.mark.asyncio
async def test_add_content_valid_entity(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Test valid entity types
    content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.NOTE.value,
        text="Test content",
        entities=[
            {"name": "Test Speaker", "type": EntityType.SPEAKER.value},
            {"name": "Test Tag", "type": EntityType.TAG.value}
        ]
    )
    
    # Verify entities in database
    async with async_session() as session:
        result = await session.execute(
            select(Entity)
            .join(content_entity_association)
            .where(content_entity_association.c.content_id == UUID(content_id))
        )
        entities = result.scalars().all()
        assert len(entities) == 2
        entity_types = {e.type for e in entities}
        assert EntityType.SPEAKER.value in entity_types
        assert EntityType.TAG.value in entity_types

@pytest.mark.asyncio
async def test_get_contents_archived(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Create content
    content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.NOTE.value,
        text="Test content"
    )
    
    # Archive content
    await manager.archive_content(
        user_id=str(test_user.id),
        content_id=UUID(content_id)
    )
    
    # Get only archived contents
    archived_contents = await manager.get_contents(
        user_id=str(test_user.id),
        only_archived=True
    )
    
    assert len(archived_contents["contents"]) > 0
    assert any(c["content_id"] == content_id for c in archived_contents["contents"])
    
    # Get non-archived contents
    active_contents = await manager.get_contents(
        user_id=str(test_user.id),
        only_archived=False
    )
    
    # Verify the content is not in active contents
    content_ids = [c["content_id"] for c in active_contents["contents"]]
    assert content_id not in content_ids 

@pytest.mark.asyncio
async def test_get_content_meeting_type(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Create meeting content with no text
    content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.MEETING.value,
        text=""
    )
    
    mock_transcription = (
        None,  # df
        "Test meeting transcription",  # formatted_input
        datetime.now(),  # start_time
        None,  # _
        "Test transcript"  # transcript
    )
    
    with patch('content_manager.get_meeting_token', new_callable=AsyncMock) as mock_token, \
         patch('content_manager.VexaAPI') as mock_vexa:
        # Setup mocks
        mock_token.return_value = "test_token"
        mock_vexa_instance = MagicMock()
        mock_vexa_instance.get_transcription = AsyncMock(return_value=mock_transcription)
        mock_vexa.return_value = mock_vexa_instance
        
        # Get content
        content = await manager.get_content(
            user_id=str(test_user.id),
            content_id=UUID(content_id)
        )
        
        assert content is not None
        assert content.text == "Test meeting transcription"
        assert content.type == ContentType.MEETING.value
        mock_token.assert_called_once_with(UUID(content_id))
        mock_vexa_instance.get_transcription.assert_called_once_with(
            meeting_session_id=UUID(content_id),
            use_index=True
        )

@pytest.mark.asyncio
async def test_get_content_meeting_type_no_transcription(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Create meeting content with no text
    content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.MEETING.value,
        text=""
    )
    
    with patch('content_manager.get_meeting_token', new_callable=AsyncMock) as mock_token, \
         patch('content_manager.VexaAPI') as mock_vexa:
        # Setup mocks
        mock_token.return_value = "test_token"
        mock_vexa_instance = MagicMock()
        mock_vexa_instance.get_transcription = AsyncMock(return_value=None)
        mock_vexa.return_value = mock_vexa_instance
        
        # Get content
        content = await manager.get_content(
            user_id=str(test_user.id),
            content_id=UUID(content_id)
        )
        
        assert content is not None
        assert content.text == ""
        assert content.type == ContentType.MEETING.value

@pytest.mark.asyncio
async def test_get_content_meeting_type_with_text(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Create meeting content with existing text
    existing_text = "Existing meeting text"
    content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.MEETING.value,
        text=existing_text
    )
    
    with patch('content_manager.get_meeting_token') as mock_token, \
         patch('content_manager.VexaAPI') as mock_vexa:
        # Get content - should not call VexaAPI since text exists
        content = await manager.get_content(
            user_id=str(test_user.id),
            content_id=UUID(content_id)
        )
        
        assert content is not None
        assert content.text == existing_text
        assert content.type == ContentType.MEETING.value
        mock_token.assert_not_called()
        mock_vexa.assert_not_called()

@pytest.mark.asyncio
async def test_get_content_meeting_type_api_error(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    manager = await ContentManager.create()
    
    # Create meeting content with no text
    content_id = await manager.add_content(
        user_id=str(test_user.id),
        type=ContentType.MEETING.value,
        text=""
    )
    
    with patch('content_manager.get_meeting_token', new_callable=AsyncMock) as mock_token, \
         patch('content_manager.VexaAPI') as mock_vexa:
        # Setup mocks
        mock_token.return_value = "test_token"
        mock_vexa_instance = MagicMock()
        mock_vexa_instance.get_transcription = AsyncMock(side_effect=VexaAPIError("API Error"))
        mock_vexa.return_value = mock_vexa_instance
        
        # Get content - should handle API error gracefully
        content = await manager.get_content(
            user_id=str(test_user.id),
            content_id=UUID(content_id)
        )
        
        assert content is not None
        assert content.text == ""
        assert content.type == ContentType.MEETING.value 