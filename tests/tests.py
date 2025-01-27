import pytest
import os
import sys
from sqlalchemy import select
from datetime import datetime, timezone
from uuid import UUID

# Add project root to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api_client import APIClient, MeetingOwnership
from psql_models import User, Content, Entity, async_session, EntityType

@pytest.mark.asyncio
async def test_get_available_emails(setup_test_users):
    """Get all available test emails from database"""
    users = setup_test_users  # No need to await, fixture returns directly
    
    async with async_session() as session:
        query = (
            select(User.email)
            .where(User.created_timestamp <= datetime.now(timezone.utc))
            .order_by(User.created_timestamp.desc())
        )
        
        result = await session.execute(query)
        emails = result.scalars().all()
        
        print("\nAvailable test emails:")
        for email in emails:
            print(f"- {email}")
        
        assert len(emails) >= 3, "Expected at least 3 test emails in database"
        return emails

@pytest.mark.asyncio
async def test_get_contents_basic(setup_test_users):
    """Test basic content retrieval"""
    users = setup_test_users  # No need to await
    test_user, test_token = users[0]
    
    client = await APIClient.create(email=test_user.email)
    response = await client.get_contents()
    assert isinstance(response, dict)
    assert "total" in response
    assert "contents" in response
    assert isinstance(response["contents"], list)

@pytest.mark.asyncio
async def test_get_contents_with_filters(setup_test_users):
    """Test content retrieval with filters"""
    users = setup_test_users  # No need to await
    test_user, test_token = users[0]
    
    client = await APIClient.create(email=test_user.email)
    filters = [{"type": "speakers", "values": ["John Doe"]}]
    response = await client.get_contents(
        content_type="meeting",
        filters=filters,
        limit=5
    )
    
    assert isinstance(response, dict)
    assert "total" in response
    assert "contents" in response
    assert len(response["contents"]) <= 5

@pytest.mark.asyncio
async def test_add_content_basic(setup_test_users):
    """Test adding basic content without entities"""
    users = setup_test_users
    test_user, test_token = users[0]
    
    client = await APIClient.create(email=test_user.email)
    response = await client.add_content(
        body="Test note content",
        content_type="note"
    )
    
    assert isinstance(response, dict)
    assert "content_id" in response
    assert "timestamp" in response
    assert "type" in response
    assert response["type"] == "note"
    assert response["parent_id"] is None

@pytest.mark.asyncio
async def test_add_content_with_entities(setup_test_users):
    """Test adding content with entities"""
    users = setup_test_users
    test_user, test_token = users[0]
    
    client = await APIClient.create(email=test_user.email)
    
    # Add content with entities
    entities = [
        {"type": "tag", "name": "test-tag-1"},
        {"type": "tag", "name": "test-tag-2"}
    ]
    
    response = await client.add_content(
        body="Test note with entities",
        content_type="note",
        entities=entities
    )
    
    assert response["content_id"] is not None
    assert response["type"] == "note"
    assert response["parent_id"] is None
    
    # Verify entities were created and associated
    async with async_session() as session:
        content_id = UUID(response["content_id"])
        
        # Check content exists
        content_query = select(Content).where(Content.id == content_id)
        content_result = await session.execute(content_query)
        content = content_result.scalar_one()
        assert content.text == "Test note with entities"
        
        # Check entities exist and are associated
        entity_query = select(Entity).where(
            Entity.name.in_([e["name"] for e in entities])
        )
        entity_result = await session.execute(entity_query)
        entities = entity_result.scalars().all()
        assert len(entities) == 2
        
        # Verify entity types
        entity_types = {e.name: e.type for e in entities}
        assert entity_types["test-tag-1"] == EntityType.TAG
        assert entity_types["test-tag-2"] == EntityType.TAG

@pytest.mark.asyncio
async def test_add_content_with_parent(setup_test_users):
    """Test adding content with parent reference"""
    users = setup_test_users
    test_user, test_token = users[0]
    
    client = await APIClient.create(email=test_user.email)
    
    # Create parent content
    parent_response = await client.add_content(
        body="Parent note",
        content_type="note"
    )
    
    # Create child content
    child_response = await client.add_content(
        body="Child note",
        content_type="note",
        parent_id=UUID(parent_response["content_id"])
    )
    
    assert isinstance(child_response, dict)
    assert child_response["parent_id"] == parent_response["content_id"]

@pytest.mark.asyncio
async def test_modify_content(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    client = await APIClient.create(email=test_user.email)
    
    # First create content to modify
    response = await client.add_content(
        body="Initial content",
        content_type="note",
        entities=[{"type": "tag", "name": "test-tag-1"}]
    )
    content_id = UUID(response["content_id"])
    
    # Modify the content
    modified = await client.modify_content(
        content_id=content_id,
        body="Modified content",
        entities=[
            {"type": "tag", "name": "test-tag-2"},
            {"type": "tag", "name": "test-tag-3"}
        ]
    )
    
    assert modified["content_id"] == str(content_id)
    assert "type" in modified
    assert "timestamp" in modified
    assert "parent_id" in modified

@pytest.mark.asyncio
async def test_archive_restore_content(setup_test_users):
    users = setup_test_users
    test_user, test_token = users[0]
    
    client = await APIClient.create(email=test_user.email)
    
    # Create content to archive
    response = await client.add_content(
        body="Content to archive",
        content_type="note"
    )
    content_id = UUID(response["content_id"])
    
    # Archive the content
    success = await client.archive_content(content_id)
    assert success is True
    
    # Verify content is not returned in normal queries
    contents = await client.get_contents()
    assert not any(c["content_id"] == str(content_id) for c in contents["contents"])
    
    # Restore the content
    success = await client.restore_content(content_id)
    assert success is True
    
    # Verify content is returned again
    contents = await client.get_contents()
    assert any(c["content_id"] == str(content_id) for c in contents["contents"])

@pytest.mark.asyncio
async def test_get_content_note(setup_test_users):
    """Test getting note content"""
    users = setup_test_users
    test_user, test_token = users[0]
    
    client = await APIClient.create(email=test_user.email)
    
    # Create note with entities
    response = await client.add_content(
        body="Test note content",
        content_type="note",
        entities=[{"type": "tag", "name": "test-tag"}]
    )
    content_id = UUID(response["content_id"])
    
    # Get content
    content = await client.get_content(content_id)
    
    assert content["content_id"] == str(content_id)
    assert content["type"] == "note"
    assert content["text"] == "Test note content"
    assert content["parent_id"] is None
    assert len(content["entities"]) == 1
    assert content["entities"][0]["name"] == "test-tag"
    assert content["entities"][0]["type"] == "tag"
    assert "children" in content
    assert isinstance(content["children"], list)

@pytest.mark.asyncio
async def test_get_content_with_children(setup_test_users):
    """Test getting content with children"""
    users = setup_test_users
    test_user, test_token = users[0]
    
    client = await APIClient.create(email=test_user.email)
    
    # Create parent note
    parent = await client.add_content(
        body="Parent note",
        content_type="note"
    )
    parent_id = UUID(parent["content_id"])
    
    # Create child notes
    child1 = await client.add_content(
        body="Child note 1",
        content_type="note",
        parent_id=parent_id
    )
    child2 = await client.add_content(
        body="Child note 2",
        content_type="note",
        parent_id=parent_id
    )
    
    # Get parent content
    content = await client.get_content(parent_id)
    
    assert content["content_id"] == str(parent_id)
    assert len(content["children"]) == 2
    assert {c["content_id"] for c in content["children"]} == {child1["content_id"], child2["content_id"]}
    
@pytest.mark.asyncio
async def test_get_nonexistent_content(setup_test_users):
    """Test getting content that doesn't exist"""
    users = setup_test_users
    test_user, test_token = users[0]
    
    client = await APIClient.create(email=test_user.email)
    
    with pytest.raises(ValueError, match="Content not found or no access"):
        await client.get_content(UUID('00000000-0000-0000-0000-000000000000')) 

@pytest.mark.asyncio
async def test_get_entities_basic(setup_test_users):
    """Test basic entity retrieval"""
    users = setup_test_users
    test_user, test_token = users[0]
    
    client = await APIClient.create(email=test_user.email)
    
    # First create some content with entities
    entities = [
        {"type": "tag", "name": "test-tag-1"},
        {"type": "tag", "name": "test-tag-2"}
    ]
    
    await client.add_content(
        body="Test note with entities",
        content_type="note",
        entities=entities
    )
    
    # Get entities
    response = await client.get_entities("tag")
    
    assert isinstance(response, dict)
    assert "total" in response
    assert "entities" in response
    assert isinstance(response["entities"], list)
    assert len(response["entities"]) >= 2
    
    # Verify entity structure
    entity = response["entities"][0]
    assert "name" in entity
    assert "last_seen" in entity
    assert "content_count" in entity
    assert entity["content_count"] >= 1

@pytest.mark.asyncio
async def test_get_entities_pagination(setup_test_users):
    """Test entity retrieval with pagination"""
    users = setup_test_users
    test_user, test_token = users[0]
    
    client = await APIClient.create(email=test_user.email)
    
    # Create multiple content items with entities
    for i in range(5):
        await client.add_content(
            body=f"Test note {i}",
            content_type="note",
            entities=[{"type": "tag", "name": f"test-tag-{i}"}]
        )
    
    # Get entities with limit
    response = await client.get_entities("tag", limit=2)
    assert len(response["entities"]) <= 2
    
    # Get next page
    response2 = await client.get_entities("tag", offset=2, limit=2)
    assert len(response2["entities"]) <= 2
    
    # Verify different entities
    first_page_names = {e["name"] for e in response["entities"]}
    second_page_names = {e["name"] for e in response2["entities"]}
    assert not first_page_names.intersection(second_page_names)

@pytest.mark.asyncio
async def test_get_entities_invalid_type(setup_test_users):
    """Test error handling for invalid entity type"""
    users = setup_test_users
    test_user, test_token = users[0]
    
    client = await APIClient.create(email=test_user.email)
    
    with pytest.raises(ValueError) as exc:
        await client.get_entities("invalid_type")
    assert "Invalid entity type" in str(exc.value)

@pytest.mark.asyncio
async def test_chat_with_content(setup_test_users):
    """Test chat with content association"""
    users = setup_test_users
    test_user, test_token = users[0]

    client = await APIClient.create(email=test_user.email)

    # Create content first
    content_response = await client.add_content(
        body="Test content for chat",
        content_type="note"
    )
    content_id = UUID(content_response["content_id"])

    # Test 1: Chat with single content ID
    final_response = None
    async for response in client.chat(
        query="Test query",
        content_id=content_id,
        meta={"test_key": "test_value"}
    ):
        if 'chunk' in response:
            continue
        final_response = response

    assert final_response is not None
    assert final_response['content_id'] == str(content_id)
    assert final_response['output'] is not None
    assert final_response['thread_id'] is not None
    assert final_response['service_content']['meta']['content_ids'] == [str(content_id)]

    # Test 2: Chat with multiple content IDs
    content_response2 = await client.add_content(
        body="Another test content",
        content_type="note"
    )
    content_id2 = UUID(content_response2["content_id"])
    
    final_response = None
    async for response in client.chat(
        query="Test query with multiple contents",
        content_ids=[content_id, content_id2]
    ):
        if 'chunk' in response:
            continue
        final_response = response

    assert final_response is not None
    assert final_response['content_ids'] == [str(content_id), str(content_id2)]
    assert final_response['output'] is not None
    assert final_response['thread_id'] is not None
    assert final_response['service_content']['meta']['content_ids'] == [str(content_id), str(content_id2)]

@pytest.mark.asyncio
async def test_chat_with_entity(setup_test_users):
    """Test chat with entity association"""
    users = setup_test_users
    test_user, test_token = users[0]

    client = await APIClient.create(email=test_user.email)

    # Create content with entity first
    content_response = await client.add_content(
        body="Test content",
        content_type="note",
        entities=[{"type": "tag", "name": "test-tag"}]
    )

    # Get entity ID
    entities = await client.get_entities("tag")
    entity = next(e for e in entities["entities"] if e["name"] == "test-tag")

    # Test 1: Chat with single entity ID
    final_response = None
    async for response in client.chat(
        query="Test query",
        entity_id=entity["id"],
        meta={"test_key": "test_value"}
    ):
        if 'chunk' in response:
            continue
        final_response = response

    assert final_response is not None
    assert final_response['entity_id'] == entity["id"]
    assert final_response['output'] is not None
    assert final_response['thread_id'] is not None
    assert final_response['service_content']['meta']['entity_ids'] == [str(entity["id"])]

    # Test 2: Chat with multiple entity IDs
    content_response2 = await client.add_content(
        body="Another test content",
        content_type="note",
        entities=[{"type": "tag", "name": "test-tag-2"}]
    )
    entities = await client.get_entities("tag")
    entity2 = next(e for e in entities["entities"] if e["name"] == "test-tag-2")

    final_response = None
    async for response in client.chat(
        query="Test query with multiple entities",
        entity_ids=[entity["id"], entity2["id"]]
    ):
        if 'chunk' in response:
            continue
        final_response = response

    assert final_response is not None
    assert final_response['entity_ids'] == [entity["id"], entity2["id"]]
    assert final_response['output'] is not None
    assert final_response['thread_id'] is not None
    assert final_response['service_content']['meta']['entity_ids'] == [str(entity["id"]), str(entity2["id"])]

@pytest.mark.asyncio
async def test_chat_invalid_combinations(setup_test_users):
    """Test chat with invalid ID combinations"""
    users = setup_test_users
    test_user, test_token = users[0]

    client = await APIClient.create(email=test_user.email)

    # Create test content and entity
    content_response = await client.add_content(
        body="Test content",
        content_type="note",
        entities=[{"type": "tag", "name": "test-tag"}]
    )
    content_id = UUID(content_response["content_id"])

    entities = await client.get_entities("tag")
    entity = next(e for e in entities["entities"] if e["name"] == "test-tag")

    # Test 1: Cannot specify both content_id and entity_id
    with pytest.raises(ValueError, match="Cannot specify both content_id and entity_id"):
        async for _ in client.chat(
            query="Test query",
            content_id=content_id,
            entity_id=entity["id"]
        ):
            pass

    # Test 2: Cannot specify both content_id and content_ids
    with pytest.raises(ValueError, match="Cannot specify both single and multiple IDs of the same type"):
        async for _ in client.chat(
            query="Test query",
            content_id=content_id,
            content_ids=[content_id]
        ):
            pass

    # Test 3: Cannot specify both entity_id and entity_ids
    with pytest.raises(ValueError, match="Cannot specify both single and multiple IDs of the same type"):
        async for _ in client.chat(
            query="Test query",
            entity_id=entity["id"],
            entity_ids=[entity["id"]]
        ):
            pass

    # Test 4: Cannot specify both content_ids and entity_ids
    with pytest.raises(ValueError, match="Cannot specify both content_ids and entity_ids"):
        async for _ in client.chat(
            query="Test query",
            content_ids=[content_id],
            entity_ids=[entity["id"]]
        ):
            pass

@pytest.mark.asyncio
async def test_chat_thread_continuation(setup_test_users):
    """Test chat thread continuation"""
    users = setup_test_users
    test_user, test_token = users[0]

    client = await APIClient.create(email=test_user.email)

    # Create content first
    content_response = await client.add_content(
        body="Test content for chat",
        content_type="note"
    )
    content_id = UUID(content_response["content_id"])

    # Start a chat thread
    thread_id = None
    async for response in client.chat(
        query="Initial query",
        content_id=content_id
    ):
        if 'thread_id' in response:
            thread_id = response['thread_id']
            break

    assert thread_id is not None

    # Continue the thread
    final_response = None
    async for response in client.chat(
        query="Follow-up query",
        thread_id=thread_id
    ):
        if 'chunk' in response:
            continue
        final_response = response

    assert final_response is not None
    assert final_response['thread_id'] == thread_id
    assert final_response['output'] is not None

@pytest.mark.asyncio
async def test_get_threads_by_content(setup_test_users):
    """Test getting threads by content"""
    users = setup_test_users
    test_user, test_token = users[0]

    client = await APIClient.create(email=test_user.email)

    # Create content and chat with it
    content_response = await client.add_content(
        body="Test content for threads",
        content_type="note"
    )
    content_id = UUID(content_response["content_id"])

    # Test 1: Chat with single content ID for thread mapping
    final_response = None
    async for response in client.chat(
        query="Test query",
        content_id=content_id
    ):
        if 'chunk' in response:
            continue
        final_response = response

    assert final_response is not None
    thread_id = final_response['thread_id']

    # Test 2: Get threads by content
    threads = await client.get_threads(content_id=content_id)
    assert len(threads) > 0
    assert any(t["thread_id"] == thread_id for t in threads)

@pytest.mark.asyncio
async def test_get_threads_by_entity(setup_test_users):
    """Test getting threads by entity"""
    users = setup_test_users
    test_user, test_token = users[0]

    client = await APIClient.create(email=test_user.email)

    # Create content with entity and chat
    content_response = await client.add_content(
        body="Test content",
        content_type="note",
        entities=[{"type": "tag", "name": "test-tag-threads"}]
    )

    # Get entity ID
    entities = await client.get_entities("tag")
    entity = next(e for e in entities["entities"] if e["name"] == "test-tag-threads")

    # Test 1: Chat with single entity ID for thread mapping
    final_response = None
    async for response in client.chat(
        query="Test query",
        entity_id=entity["id"]
    ):
        if 'chunk' in response:
            continue
        final_response = response

    assert final_response is not None
    thread_id = final_response['thread_id']

    # Test 2: Get threads by entity
    threads = await client.get_threads(entity_id=entity["id"])
    assert len(threads) > 0
    assert any(t["thread_id"] == thread_id for t in threads) 