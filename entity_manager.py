from typing import List, Optional
from sqlalchemy import select, and_, func, desc
from sqlalchemy.ext.asyncio import AsyncSession
from psql_models import Entity, Content, UserContent, content_entity_association, EntityType, AccessLevel
from psql_helpers import get_session
import logging
from uuid import UUID

logger = logging.getLogger(__name__)

class EntityManager:
    def __init__(self):
        pass
    
    @classmethod
    async def create(cls):
        return cls()
    
    async def get_entities(
        self,
        user_id: str,
        entity_type: str,
        offset: int = 0,
        limit: int = 20,
        session: AsyncSession = None
    ) -> List[Entity]:
        async with (session or get_session()) as session:
            try:
                # Validate entity type
                if entity_type not in [e.value for e in EntityType]:
                    return []
                
                # Query to get entities ordered by their content timestamps
                query = (
                    select(Entity)
                    .select_from(Entity)
                    .join(content_entity_association, content_entity_association.c.entity_id == Entity.id)
                    .join(Content, Content.id == content_entity_association.c.content_id)
                    .join(UserContent, UserContent.content_id == Content.id)
                    .where(
                        and_(
                            UserContent.user_id == UUID(user_id),
                            UserContent.access_level != AccessLevel.REMOVED.value,
                            Entity.type == entity_type,
                            Entity.name != 'TBD',
                            Entity.name != None
                        )
                    )
                    .group_by(Entity.id, Content.timestamp)  # Include timestamp in group by
                    .order_by(desc(Content.timestamp))  # Order by timestamp directly
                    .offset(offset)
                    .limit(limit)
                )
                
                result = await session.execute(query)
                entities = result.scalars().unique().all()  # Use unique() to deduplicate entities
                
                return list(entities)
                
            except Exception as e:
                logger.error(f"Error getting entities: {str(e)}", exc_info=True)
                raise
            
            
#todo: add delete entity
#todo: add update entity
#todo: add create entity
