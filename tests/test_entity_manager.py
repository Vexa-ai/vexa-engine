import pytest
from uuid import uuid4
from datetime import datetime, timezone, timedelta
from entity_manager import EntityManager
from psql_models import User, UserToken, Entity, Content, UserContent, EntityType, ContentType, AccessLevel, content_entity_association, Thread
from sqlalchemy import select, delete
from psql_helpers import async_session
import pytest_asyncio
import uuid

@pytest_asyncio.fixture
async def setup_test_users():
    async with async_session() as session:
        # Clean up any existing test data in correct order
        await session.execute(delete(content_entity_association))
        await session.execute(delete(Thread))  # Delete threads before entities
        await session.execute(delete(Entity))
        await session.execute(delete(UserContent))
        await session.execute(delete(Content))
        await session.execute(delete(UserToken))
        await session.execute(delete(User))
        
        # Create test users
        test_users = []
        test_tokens = []
        for i in range(2):
            user = User(id=uuid.uuid4(), email=f"test{i}@example.com")
            token = UserToken(user_id=user.id, token=f"test_token_{i}")
            session.add(user)
            session.add(token)
            test_users.append(user)
            test_tokens.append(token)
        
        # Create test content with timestamps
        base_time = datetime.now()
        for i in range(3):
            content = Content(
                id=uuid.uuid4(),
                type=ContentType.NOTE.value,
                text=f"Test content {i}",
                timestamp=base_time + timedelta(minutes=i)
            )
            session.add(content)
            
            # Create entity for content
            entity = Entity(name=f"Test Entity {i}", type=EntityType.SPEAKER.value)
            session.add(entity)
            
            # Associate content with entity
            await session.flush()  # Get IDs
            await session.execute(
                content_entity_association.insert().values(
                    content_id=content.id,
                    entity_id=entity.id
                )
            )
            
            # Create user content association
            user_content = UserContent(
                user_id=test_users[0].id,
                content_id=content.id,
                access_level=AccessLevel.OWNER.value,
                is_owner=True
            )
            session.add(user_content)
        
        await session.commit()
        return test_users, test_tokens

@pytest.mark.asyncio
async def test_get_entities_basic(setup_test_users):
    users, tokens = setup_test_users
    manager = await EntityManager.create()
    
    # Test basic entity retrieval
    entities = await manager.get_entities(str(users[0].id), EntityType.SPEAKER.value)
    assert len(entities) == 3
    assert all(e.type == EntityType.SPEAKER.value for e in entities)

@pytest.mark.asyncio
async def test_get_entities_pagination(setup_test_users):
    users, tokens = setup_test_users
    manager = await EntityManager.create()
    
    # Test pagination
    entities = await manager.get_entities(str(users[0].id), EntityType.SPEAKER.value, limit=2)
    assert len(entities) == 2

@pytest.mark.asyncio
async def test_get_entities_invalid_type(setup_test_users):
    users, tokens = setup_test_users
    manager = await EntityManager.create()
    
    # Test invalid entity type
    entities = await manager.get_entities(str(users[0].id), "invalid_type")
    assert len(entities) == 0

@pytest.mark.asyncio
async def test_get_entities_access_control(setup_test_users):
    users, tokens = setup_test_users
    manager = await EntityManager.create()
    
    # Test access control - user[1] should not see entities
    entities = await manager.get_entities(str(users[1].id), EntityType.SPEAKER.value)
    assert len(entities) == 0

@pytest.mark.asyncio
async def test_get_entities_timestamp_ordering(setup_test_users):
    users, tokens = setup_test_users
    manager = await EntityManager.create()
    
    # Test ordering by content timestamp
    entities = await manager.get_entities(str(users[0].id), EntityType.SPEAKER.value)
    assert len(entities) == 3
    
    # Get associated content timestamps
    async with async_session() as session:
        content_timestamps = []
        for entity in entities:
            # Get content through association
            result = await session.execute(
                select(Content)
                .join(content_entity_association)
                .where(content_entity_association.c.entity_id == entity.id)
            )
            content = result.scalar_one()
            content_timestamps.append(content.timestamp)
            
        # Verify timestamps are in descending order
        assert all(content_timestamps[i] >= content_timestamps[i+1] for i in range(len(content_timestamps)-1)) 