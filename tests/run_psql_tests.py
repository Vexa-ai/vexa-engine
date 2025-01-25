import asyncio
from uuid import uuid4
from app.models.psql_models import (
    User, UserToken, Content, Entity, UserContent,
    content_entity_association
)
from app.utils.db import get_session
from app.models.schema.content import ContentType, EntityType, AccessLevel, EntityRef
from sqlalchemy import select, and_
from datetime import datetime, timezone
from app.services.content.management import create_content as create_content_service, update_content, archive_content

async def setup_test_user():
    async with get_session() as session:
        try:
            # First cleanup any existing test user
            result = await session.execute(
                select(User).where(User.email == "test@example.com")
            )
            existing_user = result.scalar_one_or_none()
            if existing_user:
                # Delete associated tokens
                await session.execute(
                    select(UserToken).where(UserToken.user_id == existing_user.id)
                )
                tokens = (await session.execute(
                    select(UserToken).where(UserToken.user_id == existing_user.id)
                )).scalars().all()
                for token in tokens:
                    await session.delete(token)
                
                # Delete user content associations
                user_contents = (await session.execute(
                    select(UserContent).where(UserContent.user_id == existing_user.id)
                )).scalars().all()
                for user_content in user_contents:
                    await session.delete(user_content)
                
                # Delete user
                await session.delete(existing_user)
                await session.commit()
            
            # Create new test user
            user = User(
                id=uuid4(),
                email="test@example.com",
                username="test_user",
                first_name="Test",
                last_name="User"
            )
            session.add(user)
            
            # Create user token
            token = UserToken(
                user_id=user.id,
                token=str(uuid4())
            )
            session.add(token)
            await session.commit()
            return user, token
            
        except Exception as e:
            await session.rollback()
            raise Exception(f"Failed to setup test user: {str(e)}")

async def cleanup_test_user(user, token):
    async with get_session() as session:
        # First delete all user_content associations
        await session.execute(
            select(UserContent).where(UserContent.user_id == user.id)
        )
        user_contents = (await session.execute(
            select(UserContent).where(UserContent.user_id == user.id)
        )).scalars().all()
        
        for user_content in user_contents:
            await session.delete(user_content)
            
        # Then delete token
        await session.delete(token)
        
        # Finally delete user
        await session.delete(user)
        await session.commit()

async def cleanup_test_content(session, user_id):
    # Delete user content associations
    user_contents = (await session.execute(
        select(UserContent).where(UserContent.user_id == user_id)
    )).scalars().all()
    
    for user_content in user_contents:
        content_id = user_content.content_id
        
        # Delete content-entity associations
        await session.execute(
            content_entity_association.delete().where(
                content_entity_association.c.content_id == content_id
            )
        )
        
        # Get and delete entities that are only associated with this content
        entities = (await session.execute(
            select(Entity)
            .join(content_entity_association)
            .where(content_entity_association.c.content_id == content_id)
        )).scalars().all()
        
        for entity in entities:
            # Check if entity is used by other content
            other_uses = await session.execute(
                select(content_entity_association)
                .where(
                    and_(
                        content_entity_association.c.entity_id == entity.id,
                        content_entity_association.c.content_id != content_id
                    )
                )
            )
            if not other_uses.first():
                await session.delete(entity)
        
        # Delete content
        content = (await session.execute(
            select(Content).where(Content.id == content_id)
        )).scalar_one_or_none()
        if content:
            await session.delete(content)
            
        # Delete user content
        await session.delete(user_content)
    
    await session.commit()

async def test_content_lifecycle():
    user, token = await setup_test_user()
    
    try:
        async with get_session() as session:
            # Create content using service
            new_content, user_content = await create_content_service(
                session=session,
                user_id=user.id,
                content_type=ContentType.NOTE,
                text="Test note content",
                entities=[
                    EntityRef(name="John Doe", type=EntityType.SPEAKER),
                    EntityRef(name="Project X", type=EntityType.TAG)
                ]
            )
            
            # Verify content
            result = await session.execute(
                select(Content).where(Content.id == new_content.id)
            )
            saved_content = result.scalar_one()
            assert saved_content.text == "Test note content"
            
            # Verify entities using the relationship
            result = await session.execute(
                select(Entity)
                .join(content_entity_association)
                .where(content_entity_association.c.content_id == new_content.id)
            )
            saved_entities = result.scalars().all()
            assert len(saved_entities) == 2
            print("✓ Content verification passed")
            
            # Update content
            saved_content.text = "Updated test note"
            saved_content.last_update = datetime.now(timezone.utc)
            await session.commit()
            
            # Verify update
            result = await session.execute(
                select(Content).where(Content.id == new_content.id)
            )
            updated_content = result.scalar_one()
            assert updated_content.text == "Updated test note"
            print("✓ Content updated successfully")
            
            # Archive content
            saved_content.is_archived = True
            await session.commit()
            
            # Verify archive
            result = await session.execute(
                select(Content).where(Content.id == new_content.id)
            )
            archived_content = result.scalar_one()
            assert archived_content.is_archived == True
            print("✓ Content archived successfully")
            
    finally:
        await cleanup_test_user(user, token)

async def test_content_filtering():
    user, token = await setup_test_user()
    
    try:
        async with get_session() as session:
            # Clean up any existing test content
            await cleanup_test_content(session, user.id)
            await session.commit()
            
            # Create multiple contents with different tags
            contents = []
            for i in range(3):
                now = datetime.now(timezone.utc)
                content = Content(
                    id=uuid4(),
                    type=ContentType.NOTE.value,
                    text=f"Test note {i}",
                    timestamp=now,
                    last_update=now,
                    is_indexed=False
                )
                session.add(content)
                await session.flush()
                
                # Create user content association
                user_content = UserContent(
                    content_id=content.id,
                    user_id=user.id,
                    access_level=AccessLevel.OWNER.value,
                    is_owner=True,
                    created_by=user.id
                )
                session.add(user_content)
                
                # Create and associate entity
                entity = Entity(
                    name=f"Tag {i}",
                    type=EntityType.TAG.value
                )
                session.add(entity)
                await session.flush()
                
                # Link entity to content
                await session.execute(
                    content_entity_association.insert().values(
                        content_id=content.id,
                        entity_id=entity.id
                    )
                )
                contents.append(content)
            
            await session.commit()
            
            # Test filtering by tag
            result = await session.execute(
                select(Content)
                .join(UserContent, UserContent.content_id == Content.id)
                .join(content_entity_association)
                .join(Entity)
                .where(and_(
                    Entity.type == EntityType.TAG.value,
                    Entity.name == "Tag 1",
                    UserContent.user_id == user.id,
                    UserContent.access_level != AccessLevel.REMOVED.value
                ))
            )
            filtered_contents = result.scalars().all()
            assert len(filtered_contents) == 1, f"Expected 1 content, got {len(filtered_contents)}"
            print("✓ Content filtering test passed")
            
    finally:
        await cleanup_test_user(user, token)

async def test_content_update():
    user, token = await setup_test_user()
    
    try:
        async with get_session() as session:
            # First create content
            new_content, user_content = await create_content_service(
                session=session,
                user_id=user.id,
                content_type=ContentType.NOTE,
                text="Initial text",
                entities=[EntityRef(name="Initial", type=EntityType.TAG)]
            )
            
            # Update content
            updated_content, _ = await update_content(
                session=session,
                content_id=new_content.id,
                user_id=user.id,
                text="Updated text",
                entities=[
                    EntityRef(name="Updated", type=EntityType.TAG),
                    EntityRef(name="New", type=EntityType.TAG)
                ]
            )
            
            # Verify update
            assert updated_content.text == "Updated text"
            
            # Verify entities
            result = await session.execute(
                select(Entity)
                .join(content_entity_association)
                .where(content_entity_association.c.content_id == updated_content.id)
            )
            entities = result.scalars().all()
            assert len(entities) == 2
            entity_names = {e.name for e in entities}
            assert "Updated" in entity_names
            assert "New" in entity_names
            print("✓ Content update test passed")
            
    finally:
        await cleanup_test_user(user, token)

async def test_content_archive():
    user, token = await setup_test_user()
    
    try:
        async with get_session() as session:
            # Create parent content
            parent_content, parent_user_content = await create_content_service(
                session=session,
                user_id=user.id,
                content_type=ContentType.NOTE,
                text="Parent note"
            )
            
            # Create child content
            child_content, child_user_content = await create_content_service(
                session=session,
                user_id=user.id,
                content_type=ContentType.NOTE,
                text="Child note",
                parent_id=parent_content.id
            )
            
            # Archive parent with children
            await archive_content(
                session=session,
                content_id=parent_content.id,
                user_id=user.id,
                archive_children=True
            )
            
            # Verify both contents are archived
            result = await session.execute(
                select(UserContent)
                .where(UserContent.content_id.in_([parent_content.id, child_content.id]))
            )
            archived_contents = result.scalars().all()
            assert all(content.access_level == AccessLevel.REMOVED.value for content in archived_contents)
            print("✓ Content archive test passed")
            
    finally:
        await cleanup_test_user(user, token)

async def run_tests():
    print("\nRunning database tests...")
    
    print("\n1. Testing content lifecycle")
    await test_content_lifecycle()
    
    print("\n2. Testing content filtering")
    await test_content_filtering()
    
    print("\n3. Testing content update")
    await test_content_update()
    
    print("\n4. Testing content archive")
    await test_content_archive()
    
    print("\nAll tests completed successfully! ✨")

if __name__ == "__main__":
    asyncio.run(run_tests()) 