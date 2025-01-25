from typing import Optional, List, Dict, Any
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from app.models.psql_models import Content, ContentType
import logging
import uuid

logger = logging.getLogger(__name__)

async def create_child_content(
    session: AsyncSession,
    parent_id: str,
    content_type: str,
    text_content: str,
    metadata: dict = None
) -> Optional[Content]:
    """Create child content and link to parent"""
    
    # Check if parent exists
    parent = await session.scalar(
        select(Content).where(Content.content_id == parent_id)
    )
    if not parent:
        logger.error(f"Parent content {parent_id} not found")
        return None
        
    # Create child content
    child = Content(
        content_id=str(uuid.uuid4()),
        parent_id=parent_id,
        type=content_type,
        text=text_content,
        metadata=metadata or {},
        is_indexed=False
    )
    session.add(child)
    await session.flush()
    
    return child

async def get_child_content(
    session: AsyncSession,
    parent_id: str,
    content_type: Optional[str] = None
) -> List[Content]:
    """Get all child content of specified type for parent"""
    
    query = select(Content).where(Content.parent_id == parent_id)
    if content_type:
        query = query.where(Content.type == content_type)
    
    result = await session.execute(query)
    return result.scalars().all()

async def update_child_content(
    session: AsyncSession,
    parent_id: str,
    child_id: str,
    child_type: ContentType
) -> None:
    """Update child content with parent relationship"""
    
    # Get parent content
    parent = await session.get(Content, parent_id)
    if not parent:
        raise ValueError(f"Parent content {parent_id} not found")
        
    # Get child content
    child = await session.get(Content, child_id)
    if not child:
        raise ValueError(f"Child content {child_id} not found")
        
    # Update child content
    child.parent_id = parent_id
    child.type = child_type
    await session.commit() 