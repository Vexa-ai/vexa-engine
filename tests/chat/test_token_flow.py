# Test script to explore note token flow
import asyncio
from sqlalchemy import select
from datetime import datetime
from uuid import UUID

from psql_models import Content, UserContent, ContentType, UserToken
from psql_helpers import async_session
from psql_access import get_content_token, get_user_token

async def explore_token_flow(note_id: str):
    """Explore token retrieval flow for a note"""
    print(f"\n=== Exploring token flow for note {note_id} ===")
    
    async with async_session() as session:
        # 1. Check if note exists
        note_query = select(Content).where(Content.id == UUID(note_id))
        note_result = await session.execute(note_query)
        note = note_result.scalar_one_or_none()
        
        if not note:
            print("❌ Note not found")
            return
            
        print(f"✅ Found note: {note.id}")
        print(f"Type: {note.type}")
        print(f"Created at: {note.timestamp}")
        
        # 2. Get user content record
        user_content_query = select(UserContent).where(UserContent.content_id == note.id)
        user_content_result = await session.execute(user_content_query)
        user_content = user_content_result.scalar_one_or_none()
        
        if not user_content:
            print("❌ No user content record found")
            return
            
        print(f"✅ Found user content record")
        print(f"User ID: {user_content.user_id}")
        print(f"Access Level: {user_content.access_level}")
        
        # 3. Check user tokens
        token_query = select(UserToken).where(UserToken.user_id == user_content.user_id)
        token_result = await session.execute(token_query)
        tokens = token_result.scalars().all()
        
        if not tokens:
            print("❌ No tokens found for user")
        else:
            print(f"✅ Found {len(tokens)} tokens for user")
            for token in tokens:
                print(f"Token: {token.token[:10]}...")
                print(f"Last used: {token.last_used_at}")
        
        # 4. Try getting content token
        try:
            content_token = await get_content_token(note.id)
            print(f"\n✅ Got content token: {content_token[:10] if content_token else 'None'}...")
            
            # 5. If content token is a user ID, try getting user token
            if content_token and note.type == ContentType.NOTE.value:
                print("\nTrying to get user token...")
                user_token = await get_user_token(UUID(content_token))
                if user_token:
                    print(f"✅ Got user token: {user_token[:10]}...")
                else:
                    print("❌ No user token found")
                
        except Exception as e:
            print(f"\n❌ Error getting content token: {str(e)}")

# Run the exploration
async def main():
    note_id = "f10cd91b-79f0-4cb9-b332-16f56baa7b10"  # The note ID from logs
    await explore_token_flow(note_id)

if __name__ == "__main__":
    asyncio.run(main()) 