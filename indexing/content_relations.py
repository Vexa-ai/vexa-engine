from typing import Optional, List
from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession
from psql_models import Content, ContentType
import logging

logger = logging.getLogger(__name__)

async def create_child_content(
    session: AsyncSession,
    parent_id: str,
    content_type: str,
    content_data: str,
    order: int = 0,
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
        parent_id=parent_id,
        type=content_type,
        content=content_data,
        order=order,
        metadata=metadata or {}
    )
    session.add(child)
    await session.flush()  # Get child ID
    
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
    query = query.order_by(Content.order)
    
    result = await session.execute(query)
    return result.scalars().all()

async def update_child_content(
    session: AsyncSession,
    parent_id: str,
    content_type: str,
    content_data: str,
    order: int = 0,
    metadata: dict = None
) -> Optional[Content]:
    """Update or create child content"""
    
    # Try to find existing child content
    existing = await session.scalar(
        select(Content).where(
            and_(
                Content.parent_id == parent_id,
                Content.type == content_type
            )
        )
    )
    
    if existing:
        existing.content = content_data
        existing.order = order
        if metadata:
            existing.metadata = metadata
        return existing
    
    # Create new if not exists
    return await create_child_content(
        session, parent_id, content_type, content_data, order, metadata
    ) 