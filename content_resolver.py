from typing import List, Optional, Set
from uuid import UUID
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from psql_models import content_entity_association

async def resolve_content_ids(
    session: AsyncSession,
    content_id: Optional[UUID] = None,
    entity_id: Optional[int] = None,
    content_ids: Optional[List[UUID]] = None,
    entity_ids: Optional[List[int]] = None
) -> List[UUID]:
    # Start with direct content_ids
    all_content_ids: Set[UUID] = set(content_ids or [])
    
    # Add single content_id if provided
    if content_id:
        all_content_ids.add(content_id)
        
    # Get content_ids from entity_ids
    if entity_ids or entity_id:
        ids_to_query = set(entity_ids or [])
        if entity_id:
            ids_to_query.add(entity_id)
            
        # Query content_entity_association
        query = select(content_entity_association.c.content_id).where(
            content_entity_association.c.entity_id.in_(ids_to_query)
        )
        result = await session.execute(query)
        entity_content_ids = {row[0] for row in result}
        all_content_ids.update(entity_content_ids)
        
    return list(all_content_ids) 