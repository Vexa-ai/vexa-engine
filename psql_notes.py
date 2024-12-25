from datetime import datetime, timezone
from typing import Optional, List, Union
from uuid import UUID
from sqlalchemy import select, and_, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
import logging

from psql_models import (
    Content, UserContent, ContentType, AccessLevel,
    User, content_entity_association
)
from psql_helpers import get_session

# Set up logging
logger = logging.getLogger(__name__)

async def create_note(
    session: AsyncSession,
    user_id: int,
    text: str,
    parent_id: Optional[UUID] = None
) -> Content:
    """Create a new note in the database."""
    logger.info(f"Creating note for user {user_id}")
    logger.debug(f"Note text: {text}, parent_id: {parent_id}")
    
    try:
        # Create the content entry
        logger.debug("Creating Content entry")
        content = Content(
            text=text,
            type=ContentType.NOTE,
            parent_id=parent_id,
            timestamp=datetime.utcnow()
        )
        session.add(content)
        await session.flush()  # Get the content.id
        logger.debug(f"Content created with ID: {content.id}")
        
        # Create the user_content entry
        logger.debug(f"Creating UserContent entry for user {user_id} and content {content.id}")
        user_content = UserContent(
            user_id=user_id,
            content_id=content.id,
            access_level=AccessLevel.OWNER
        )
        session.add(user_content)
        
        # Commit the transaction
        logger.debug("Committing transaction")
        await session.commit()
        logger.info(f"Note creation completed successfully for content ID: {content.id}")
        
        return content
        
    except Exception as e:
        logger.error(f"Error creating note: {str(e)}", exc_info=True)
        await session.rollback()
        raise

async def get_notes_by_user(
    session: AsyncSession,
    user_id: UUID,
    parent_id: Optional[UUID] = None,
    limit: int = 100,
    offset: int = 0
) -> tuple[List[dict], int]:
    """Get notes for a user, optionally filtered by parent_id"""
    # Build base query
    query = (
        select(Content)
        .join(UserContent)
        .where(and_(
            UserContent.user_id == user_id,
            Content.type == ContentType.NOTE.value,
            UserContent.access_level != AccessLevel.REMOVED.value
        ))
    )
    
    if parent_id is not None:
        query = query.where(Content.parent_id == parent_id)
    
    # Get total count
    count_query = select(func.count()).select_from(query.subquery())
    total_count = await session.scalar(count_query)
    
    # Add ordering and pagination
    query = (
        query
        .order_by(Content.timestamp.desc())
        .offset(offset)
        .limit(limit)
    )
    
    result = await session.execute(query)
    notes = result.scalars().all()
    
    return [
        {
            "id": str(note.id),
            "text": note.text,
            "timestamp": note.timestamp,
            "parent_id": str(note.parent_id) if note.parent_id else None,
            "last_update": note.last_update
        }
        for note in notes
    ], total_count

async def update_note(
    session: AsyncSession,
    note_id: UUID,
    user_id: UUID,
    text: Optional[str] = None
) -> Optional[Content]:
    """Update an existing note"""
    # Check access
    access_query = select(UserContent).where(and_(
        UserContent.content_id == note_id,
        UserContent.user_id == user_id,
        UserContent.access_level == AccessLevel.OWNER.value
    ))
    result = await session.execute(access_query)
    if not result.scalar_one_or_none():
        return None
    
    # Get note
    note = await session.get(Content, note_id)
    if not note or note.type != ContentType.NOTE.value:
        return None
    
    # Update fields
    if text is not None:
        note.text = text
    note.last_update = datetime.now(timezone.utc)
    
    await session.commit()
    return note

async def delete_note(
    session: AsyncSession,
    note_id: UUID,
    user_id: UUID
) -> bool:
    """Delete a note (mark as removed)"""
    # Check access
    user_content = await session.scalar(
        select(UserContent).where(and_(
            UserContent.content_id == note_id,
            UserContent.user_id == user_id,
            UserContent.access_level == AccessLevel.OWNER.value
        ))
    )
    if not user_content:
        return False
    
    # Mark as removed
    user_content.access_level = AccessLevel.REMOVED.value
    await session.commit()
    return True 