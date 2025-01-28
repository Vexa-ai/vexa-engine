import json
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Tuple
from uuid import UUID, uuid4
from pydantic import BaseModel, Field
from sqlalchemy import select, update, delete, and_, func, case
from sqlalchemy.ext.asyncio import AsyncSession
from psql_models import (
    Content, UserContent, Entity, content_entity_association,
    ContentType, AccessLevel, EntityType
)
from psql_helpers import get_session
import logging

logger = logging.getLogger(__name__)

class ContentData(BaseModel):
    content_id: str
    type: str
    text: str
    timestamp: datetime
    is_indexed: bool
    access_level: str
    is_owner: bool
    entities: List[Dict[str, str]] = []
    parent_id: Optional[str] = None
    meta: Optional[Dict] = None

class ContentManager:
    def __init__(self):
        pass
    
    @classmethod
    async def create(cls):
        return cls()
    
    async def get_contents(
        self,
        user_id: str,
        ownership: str = None,
        content_type: str = None,
        filters: List[Dict[str, Any]] = None,
        offset: int = 0,
        limit: int = 100,
        session: AsyncSession = None
    ) -> Dict[str, Any]:
        async with get_session() as session:
            try:
                # Base query for content with user access and entity info
                base_query = (
                    select(
                        Content.id.label('content_id'),
                        Content.timestamp,
                        Content.type,
                        Content.text,
                        Content.is_indexed,
                        UserContent.is_owner,
                        UserContent.access_level,
                        func.array_agg(func.distinct(Entity.name)).label('entities'),
                        func.array_agg(func.distinct(Entity.type)).label('entity_types')
                    )
                    .join(UserContent, Content.id == UserContent.content_id)
                    .outerjoin(content_entity_association, Content.id == content_entity_association.c.content_id)
                    .outerjoin(Entity, content_entity_association.c.entity_id == Entity.id)
                    .where(
                        and_(
                            UserContent.user_id == UUID(user_id),
                            UserContent.access_level != AccessLevel.REMOVED.value,
                            case(
                                (ownership == "my", UserContent.is_owner == True),
                                (ownership == "shared", UserContent.is_owner == False),
                                else_=True
                            ),
                            case(
                                (content_type is not None, Content.type == content_type),
                                else_=True
                            )
                        )
                    )
                )

                # Apply entity filters if provided
                if filters:
                    for filter_spec in filters:
                        if filter_spec["type"] == "speakers":
                            base_query = base_query.where(
                                and_(
                                    Entity.name.in_(filter_spec["values"]),
                                    Entity.type == EntityType.SPEAKER.value
                                )
                            )
                        elif filter_spec["type"] == "tags":
                            base_query = base_query.where(
                                and_(
                                    Entity.name.in_(filter_spec["values"]),
                                    Entity.type == EntityType.TAG.value
                                )
                            )

                base_query = base_query.group_by(Content.id, Content.timestamp, Content.type, Content.text, Content.is_indexed, UserContent.is_owner, UserContent.access_level)

                # Count total results using a simpler query
                count_query = (
                    select(func.count(func.distinct(Content.id)))
                    .select_from(Content)
                    .join(UserContent, Content.id == UserContent.content_id)
                    .outerjoin(content_entity_association, Content.id == content_entity_association.c.content_id)
                    .outerjoin(Entity, content_entity_association.c.entity_id == Entity.id)
                    .where(
                        and_(
                            UserContent.user_id == UUID(user_id),
                            UserContent.access_level != AccessLevel.REMOVED.value,
                            case(
                                (ownership == "my", UserContent.is_owner == True),
                                (ownership == "shared", UserContent.is_owner == False),
                                else_=True
                            ),
                            case(
                                (content_type is not None, Content.type == content_type),
                                else_=True
                            )
                        )
                    )
                )

                # Apply same entity filters to count query
                if filters:
                    for filter_spec in filters:
                        if filter_spec["type"] == "speakers":
                            count_query = count_query.where(
                                and_(
                                    Entity.name.in_(filter_spec["values"]),
                                    Entity.type == EntityType.SPEAKER.value
                                )
                            )
                        elif filter_spec["type"] == "tags":
                            count_query = count_query.where(
                                and_(
                                    Entity.name.in_(filter_spec["values"]),
                                    Entity.type == EntityType.TAG.value
                                )
                            )

                total = await session.scalar(count_query) or 0

                # Get paginated results
                results = await session.execute(
                    base_query.offset(offset).limit(limit)
                )
                
                contents = []
                for row in results.mappings():
                    contents.append({
                        "content_id": str(row['content_id']),
                        "type": row['type'],
                        "text": row['text'],
                        "timestamp": row['timestamp'],
                        "is_indexed": row['is_indexed'],
                        "is_owner": row['is_owner'],
                        "access_level": row['access_level'],
                        "entities": [
                            {"name": name, "type": type_}
                            for name, type_ in zip(row['entities'], row['entity_types'])
                            if name and name != 'TBD'
                        ] if row['entities'] and row['entities'][0] is not None else []
                    })

                return {
                    "total": total,
                    "contents": contents
                }

            except Exception as e:
                logger.error(f"Error getting contents: {str(e)}", exc_info=True)
                raise
    
    async def get_content(
        self,
        user_id: str,
        content_id: UUID,
        token: Optional[str] = None
    ) -> Optional[ContentData]:
        async with get_session() as session:
            query = (
                select(
                    Content,
                    UserContent.access_level,
                    UserContent.is_owner,
                    func.array_agg(func.distinct(Entity.name)).label('entities'),
                    func.array_agg(func.distinct(Entity.type)).label('entity_types')
                )
                .join(UserContent, Content.id == UserContent.content_id)
                .outerjoin(content_entity_association, Content.id == content_entity_association.c.content_id)
                .outerjoin(Entity, content_entity_association.c.entity_id == Entity.id)
                .where(
                    and_(
                        Content.id == content_id,
                        UserContent.user_id == UUID(user_id),
                        UserContent.access_level != AccessLevel.REMOVED.value
                    )
                )
                .group_by(Content.id, UserContent.access_level, UserContent.is_owner)
            )
            
            result = await session.execute(query)
            row = result.first()
            if not row:
                return None
                
            content, access_level, is_owner, entity_names, entity_types = row
            
            return ContentData(
                content_id=str(content.id),
                type=content.type,
                text=content.text,
                timestamp=content.timestamp,
                is_indexed=content.is_indexed,
                access_level=access_level,
                is_owner=is_owner,
                parent_id=str(content.parent_id) if content.parent_id else None,
                entities=[
                    {"name": name, "type": type_}
                    for name, type_ in zip(entity_names, entity_types)
                    if name and name != 'TBD'
                ]
            )
    
    async def add_content(
        self,
        user_id: str,
        type: str,
        text: str,
        parent_id: Optional[UUID] = None,
        entities: Optional[List[Dict[str, str]]] = None
    ) -> str:
        async with get_session() as session:
            async with session.begin():
                content = Content(
                    id=uuid4(),
                    type=type,
                    text=text,
                    timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
                    parent_id=parent_id,
                    is_indexed=False
                )
                session.add(content)
                await session.flush()
                
                # Create user content record
                user_content = UserContent(
                    content_id=content.id,
                    user_id=UUID(user_id),
                    created_by=UUID(user_id),
                    is_owner=True,
                    access_level=AccessLevel.OWNER.value
                )
                session.add(user_content)
                
                # Add entities
                if entities:
                    for entity_data in entities:
                        entity_type = EntityType(entity_data["type"])
                        # Check if entity exists
                        entity_query = select(Entity).where(
                            and_(
                                Entity.name == entity_data["name"],
                                Entity.type == entity_type.value
                            )
                        )
                        result = await session.execute(entity_query)
                        entity = result.scalar_one_or_none()
                        
                        if not entity:
                            entity = Entity(
                                name=entity_data["name"],
                                type=entity_type.value
                            )
                            session.add(entity)
                            await session.flush()
                        
                        await session.execute(
                            content_entity_association.insert().values(
                                content_id=content.id,
                                entity_id=entity.id
                            )
                        )
                
                await session.commit()
                return str(content.id)
    
    async def modify_content(
        self,
        user_id: str,
        content_id: UUID,
        text: str,
        entities: Optional[List[Dict[str, str]]] = None
    ) -> bool:
        async with get_session() as session:
            async with session.begin():
                # Check access
                content_query = (
                    select(Content)
                    .join(UserContent)
                    .where(
                        and_(
                            Content.id == content_id,
                            UserContent.user_id == UUID(user_id),
                            UserContent.access_level == AccessLevel.OWNER.value
                        )
                    )
                )
                result = await session.execute(content_query)
                content = result.scalar_one_or_none()
                
                if not content:
                    return False
                
                # Update content
                content.text = text
                content.last_update = datetime.now(timezone.utc).replace(tzinfo=None)
                
                # Update entities
                if entities is not None:
                    # Remove existing associations
                    await session.execute(
                        delete(content_entity_association)
                        .where(content_entity_association.c.content_id == content_id)
                    )
                    
                    # Add new entities
                    for entity_data in entities:
                        entity_type = EntityType(entity_data["type"])
                        entity_query = select(Entity).where(
                            and_(
                                Entity.name == entity_data["name"],
                                Entity.type == entity_type.value
                            )
                        )
                        result = await session.execute(entity_query)
                        entity = result.scalar_one_or_none()
                        
                        if not entity:
                            entity = Entity(
                                name=entity_data["name"],
                                type=entity_type.value
                            )
                            session.add(entity)
                            await session.flush()
                        
                        await session.execute(
                            content_entity_association.insert().values(
                                content_id=content_id,
                                entity_id=entity.id
                            )
                        )
                
                await session.commit()
                return True
    
    async def delete_content(self, user_id: str, content_id: UUID) -> bool:
        async with get_session() as session:
            async with session.begin():
                result = await session.execute(
                    update(UserContent)
                    .where(
                        and_(
                            UserContent.content_id == content_id,
                            UserContent.user_id == UUID(user_id)
                        )
                    )
                    .values(access_level=AccessLevel.REMOVED.value)
                )
                await session.commit()
                return result.rowcount > 0
    
    async def archive_content(self, user_id: str, content_id: UUID) -> bool:
        return await self.delete_content(user_id, content_id)
    
    async def restore_content(self, user_id: str, content_id: UUID) -> bool:
        async with get_session() as session:
            async with session.begin():
                result = await session.execute(
                    update(UserContent)
                    .where(
                        and_(
                            UserContent.content_id == content_id,
                            UserContent.user_id == UUID(user_id),
                            UserContent.access_level == AccessLevel.REMOVED.value
                        )
                    )
                    .values(access_level=AccessLevel.OWNER.value)
                )
                await session.commit()
                return result.rowcount > 0 