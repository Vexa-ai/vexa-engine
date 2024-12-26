"""Script to fix missing tokens for existing notes."""
import asyncio
from sqlalchemy import select, and_
from datetime import datetime
from uuid import UUID

from psql_models import Content, UserContent, ContentType, UserToken
from psql_helpers import async_session
from token_manager import TokenManager

async def fix_note_tokens():
    """Find notes without tokens and add them."""
    print("\n=== Fixing missing tokens for notes ===")
    
    async with async_session() as session:
        # Get all notes
        notes_query = select(Content, UserContent).join(
            UserContent,
            and_(
                Content.id == UserContent.content_id,
                UserContent.is_owner == True
            )
        ).where(Content.type == ContentType.NOTE.value)
        
        notes_result = await session.execute(notes_query)
        notes = notes_result.all()
        
        print(f"Found {len(notes)} notes to check")
        fixed_count = 0
        
        for note, user_content in notes:
            # Check if user has any tokens
            token_query = select(UserToken).where(
                UserToken.user_id == user_content.user_id
            ).order_by(UserToken.last_used_at.desc())
            token_result = await session.execute(token_query)
            token = token_result.scalar_one_or_none()
            
            if not token:
                print(f"\nNote {note.id}:")
                print(f"❌ No token found for user {user_content.user_id}")
                continue
                
            print(f"\nNote {note.id}:")
            print(f"✅ Found token for user {user_content.user_id}")
            print(f"Token: {token.token[:10]}...")
            print(f"Last used: {token.last_used_at}")
            
            # Update last_used_at to ensure token is current
            token.last_used_at = datetime.now()
            fixed_count += 1
            
        await session.commit()
        print(f"\nFixed {fixed_count} out of {len(notes)} notes")

if __name__ == "__main__":
    asyncio.run(fix_note_tokens()) 