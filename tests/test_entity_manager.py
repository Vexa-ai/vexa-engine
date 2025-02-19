import pytest
from uuid import uuid4
from datetime import datetime, timezone, timedelta
from entity_manager import EntityManager
from psql_models import User, UserToken, Entity, Content, UserContent, EntityType, ContentType, AccessLevel, content_entity_association, Thread, async_session, Transcript, ContentAccess, TranscriptAccess
from sqlalchemy import select, delete, text
import pytest_asyncio
import uuid

@pytest_asyncio.fixture
async def setup_test_users():
    async with async_session() as session:
        # Clean up any existing test data in correct order
        await session.execute(delete(content_entity_association))
        await session.execute(delete(Thread))  # Delete threads before entities
        await session.execute(delete(ContentAccess))  # Delete content access records
        await session.execute(delete(TranscriptAccess))  # Delete transcript access records
        await session.execute(delete(UserContent))  # Delete user content records
        await session.execute(delete(Entity))  # Delete entities
        await session.execute(delete(Transcript))  # Delete transcripts before content
        await session.execute(delete(Content))  # Delete content
        await session.execute(text("DELETE FROM prompts"))  # Delete prompts before users
        await session.execute(delete(UserToken))  # Delete user tokens
        await session.execute(delete(User))  # Finally delete users
        
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
        
        await session.flush()  # Flush to get IDs
        
        # Create test content with timestamps
        base_time = datetime.now(timezone.utc)
        for i in range(3):
            content = Content(
                id=uuid.uuid4(),
                type=ContentType.NOTE.value,
                text=f"Test content {i}",
                timestamp=base_time + timedelta(minutes=i)
            )
            session.add(content)
            await session.flush()  # Flush to get content ID
            
            # Create entity for content
            entity = Entity(name=f"Test Entity {i}", type=EntityType.SPEAKER.value)
            session.add(entity)
            await session.flush()  # Flush to get entity ID
            
            # Associate content with entity
            await session.execute(
                content_entity_association.insert().values(
                    content_id=content.id,
                    entity_id=entity.id,
                    created_by=test_users[0].id  # Set owner
                )
            )
            
            # Create user content association
            user_content = UserContent(
                user_id=test_users[0].id,
                content_id=content.id,
                access_level=AccessLevel.OWNER.value,
                is_owner=True,
                created_by=test_users[0].id
            )
            session.add(user_content)
            
            # Create content access record
            content_access = ContentAccess(
                content_id=content.id,
                user_id=test_users[0].id,
                access_level=AccessLevel.OWNER.value,
                granted_by=test_users[0].id
            )
            session.add(content_access)
        
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