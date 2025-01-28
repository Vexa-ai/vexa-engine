import json
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Tuple
from uuid import UUID, uuid4
from pydantic import BaseModel, Field
from sqlalchemy import select, update, delete, and_, func, case, or_
from sqlalchemy.ext.asyncio import AsyncSession
from psql_models import (
    Content, UserContent, Entity, content_entity_association,
    ContentType, AccessLevel, EntityType
)
from psql_helpers import get_session
from redis import Redis
from indexing.redis_keys import RedisKeys
from psql_access import has_content_access
from psql_sharing import create_share_link, accept_share_link
import logging
import os

logger = logging.getLogger(__name__)

# Redis client initialization
REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT)

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
    
    async def delete_content(self, user_id: str, content_id: UUID, physical_delete: bool = False) -> bool:
        async with get_session() as session:
            async with session.begin():
                # Check if content exists and user has access
                user_content = await session.execute(
                    select(UserContent)
                    .where(
                        and_(
                            UserContent.content_id == content_id,
                            UserContent.user_id == UUID(user_id)
                        )
                    )
                )
                user_content = user_content.scalar_one_or_none()
                if not user_content:
                    return False
                # For physical delete, check if content is archived
                if physical_delete:
                    if user_content.access_level != AccessLevel.REMOVED.value:
                        return False
                    # First delete all UserContent records
                    await session.execute(
                        delete(UserContent)
                        .where(UserContent.content_id == content_id)
                    )
                    # Then delete content and its associations
                    await session.execute(
                        delete(content_entity_association)
                        .where(content_entity_association.c.content_id == content_id)
                    )
                    await session.execute(
                        delete(Content)
                        .where(Content.id == content_id)
                    )
                    await session.commit()
                    return True
                # Regular delete - just mark as removed
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

    async def create_share_link(
        self,
        owner_id: str,
        access_level: AccessLevel,
        content_ids: Optional[List[UUID]] = None,
        target_email: Optional[str] = None,
        expiration_hours: Optional[int] = None,
        session: AsyncSession = None
    ) -> str:
        async with (session or get_session()) as session:
            token = await create_share_link(
                session=session,
                owner_id=UUID(owner_id),
                access_level=access_level,
                content_ids=content_ids,
                target_email=target_email,
                expiration_hours=expiration_hours
            )
            return token

    async def accept_share_link(
        self,
        token: str,
        accepting_user_id: str,
        accepting_email: Optional[str] = None,
        session: AsyncSession = None
    ) -> bool:
        async with (session or get_session()) as session:
            return await accept_share_link(
                session=session,
                token=token,
                accepting_user_id=UUID(accepting_user_id),
                accepting_email=accepting_email
            )

    async def queue_content_indexing(
        self,
        user_id: str,
        content_id: UUID,
        session: AsyncSession = None
    ) -> Dict[str, str]:
        async with (session or get_session()) as session:
            # Verify access
            if not await has_content_access(session, UUID(user_id), content_id):
                raise ValueError("No access to content")
            
            content_id_str = str(content_id)
            
            # Check if content is already being processed
            if redis_client.sismember(RedisKeys.PROCESSING_SET, content_id_str):
                return {
                    "status": "already_processing",
                    "message": "Content is already being processed"
                }
            
            # Check if content is already in queue
            if redis_client.zscore(RedisKeys.INDEXING_QUEUE, content_id_str) is not None:
                return {
                    "status": "already_queued",
                    "message": "Content is already in indexing queue"
                }
            
            # Remove from failed set if present
            failed_info = redis_client.hget(RedisKeys.FAILED_SET, content_id_str)
            if failed_info:
                redis_client.hdel(RedisKeys.FAILED_SET, content_id_str)
            
            # Add to indexing queue
            redis_client.zadd(
                RedisKeys.INDEXING_QUEUE,
                {content_id_str: datetime.now().timestamp()}
            )
            
            return {
                "status": "queued",
                "message": "Content has been queued for indexing"
            } 