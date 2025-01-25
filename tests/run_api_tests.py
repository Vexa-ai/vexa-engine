import asyncio
from uuid import uuid4, UUID
from app.models.psql_models import (
    User, UserToken, Content, Entity, UserContent,
    content_entity_association
)
from app.utils.db import get_session
from app.models.schema.content import ContentType, EntityType, AccessLevel
from sqlalchemy import select, and_
from datetime import datetime, timezone
from api_client import APIClient

async def setup_test_user():
    async with get_session() as session:
        try:
            # First cleanup any existing test user
            await cleanup_test_user()
            
            # Create new test user
            user = User(
                id=uuid4(),
                email="test@example.com",
                username="test_user",
                first_name="Test",
                last_name="User"
            )
            session.add(user)
            await session.commit()
            
            # Create initial token for the user
            token = UserToken(
                token=str(uuid4()),
                user_id=user.id
            )
            session.add(token)
            await session.commit()
            
            return user
            
        except Exception as e:
            await session.rollback()
            print(f"Failed to setup test user: {str(e)}")
            raise

async def cleanup_test_user():
    async with get_session() as session:
        # Delete test user and all associated data
        result = await session.execute(
            select(User).where(User.email == "test@example.com")
        )
        user = result.scalar_one_or_none()
        if user:
            # Delete user tokens
            await session.execute(
                select(UserToken).where(UserToken.user_id == user.id)
            )
            tokens = (await session.execute(
                select(UserToken).where(UserToken.user_id == user.id)
            )).scalars().all()
            for token in tokens:
                await session.delete(token)
            
            # Delete user content
            user_contents = (await session.execute(
                select(UserContent).where(UserContent.user_id == user.id)
            )).scalars().all()
            for uc in user_contents:
                await session.delete(uc)
            
            # Delete user
            await session.delete(user)
            await session.commit()

async def test_note_api():
    try:
        # Setup test user and wait for it to be created
        user = await setup_test_user()
        
        # Create API client
        client = await APIClient.create(email="test@example.com")
        
        # Create a note
        note = await client.create_content(
            content_type=ContentType.NOTE,
            text="Test API note",
            entities=[{"name": "API Test", "type": EntityType.TAG}]
        )
        assert note.type == ContentType.NOTE.value
        print("✓ Note creation passed")
        
        # Wait a moment for content to be indexed
        await asyncio.sleep(1)
        
        # List contents
        contents = await client.list_contents(content_type=ContentType.NOTE)
        print(f"Found {len(contents['contents'])} contents")  # Debug print
        print(f"Looking for content_id: {note.content_id}")   # Debug print
        assert any(c["content_id"] == note.content_id for c in contents["contents"])
        print("✓ Content listing passed")
        
        # Update content
        updated = await client.update_content(
            content_id=UUID(note["id"]),
            text="Updated API note",
            entities=[
                {"name": "API Test", "type": EntityType.TAG},
                {"name": "Updated", "type": EntityType.TAG}
            ]
        )
        assert updated["text"] == "Updated API note"
        print("✓ Content update passed")
        
        # Archive content
        result = await client.archive_content(UUID(note["id"]))
        assert "archived successfully" in result["message"].lower()
        print("✓ Content archiving passed")
        
    except Exception as e:
        print(f"Test failed: {str(e)}")
        raise
    finally:
        await cleanup_test_user()

async def main():
    await test_note_api()

if __name__ == "__main__":
    asyncio.run(main()) 