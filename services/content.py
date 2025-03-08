import json
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Tuple
from uuid import UUID, uuid4
from pydantic import BaseModel, Field
from sqlalchemy import select, update, delete, and_, func, case, or_
from sqlalchemy.ext.asyncio import AsyncSession
from psql_models import (
    Content, UserContent, Entity, content_entity_association,
    ContentType, AccessLevel, EntityType, ContentAccess, ExternalIDType,
    TranscriptAccess, Transcript
)
from services.psql_helpers import get_session
from redis import Redis
from indexing.redis_keys import RedisKeys
from psql_access import has_content_access, get_meeting_token
from psql_sharing import create_share_link, accept_share_link
from vexa import VexaAPI, VexaAPIError
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
    external_id: Optional[str] = None
    external_id_type: Optional[str] = None

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
        only_archived: bool = False,
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
                            case(
                                (only_archived, UserContent.access_level == AccessLevel.REMOVED.value),
                                else_=UserContent.access_level != AccessLevel.REMOVED.value
                            ),
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
                        filter_type = filter_spec["type"]
                        if filter_type == EntityType.SPEAKER.value:
                            base_query = base_query.where(
                                and_(
                                    Entity.name.in_(filter_spec["values"]),
                                    Entity.type == EntityType.SPEAKER.value
                                )
                            )
                        elif filter_type == EntityType.TAG.value:
                            base_query = base_query.where(
                                and_(
                                    Entity.name.in_(filter_spec["values"]),
                                    Entity.type == EntityType.TAG.value
                                )
                            )

                base_query = base_query.group_by(Content.id, Content.timestamp, Content.type, Content.text, Content.is_indexed, UserContent.is_owner, UserContent.access_level).order_by(Content.timestamp.desc())

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
                            case(
                                (only_archived, UserContent.access_level == AccessLevel.REMOVED.value),
                                else_=UserContent.access_level != AccessLevel.REMOVED.value
                            ),
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
                        filter_type = filter_spec["type"]
                        if filter_type == EntityType.SPEAKER.value:
                            count_query = count_query.where(
                                and_(
                                    Entity.name.in_(filter_spec["values"]),
                                    Entity.type == EntityType.SPEAKER.value
                                )
                            )
                        elif filter_type == EntityType.TAG.value:
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
            
            text = content.text
            if content.type == ContentType.MEETING.value and not text:
                try:
                    token = await get_meeting_token(content_id)
                    vexa_api = VexaAPI(token=token)
                    transcription = await vexa_api.get_transcription(meeting_session_id=content_id, use_index=True)
                    if transcription:
                        df, formatted_input, start_time, _, transcript = transcription
                        text = transcript
                    else:
                        logger.warning(f"No transcription found for meeting {content_id}")
                        text = ""
                except Exception as e:
                    logger.error(f"Error getting meeting transcription: {str(e)}")
                    text = ""
            
            if text is None:
                text = ""
            
            return ContentData(
                content_id=str(content.id),
                type=content.type,
                text=text,
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
        entities: Optional[List[Dict[str, Any]]] = None,
        external_id: Optional[str] = None,
        external_id_type: Optional[str] = None
    ) -> str:
        # Validate content type
        if type not in [e.value for e in ContentType]:
            raise ValueError(f"Invalid content type: {type}")
            
        # Validate external_id_type if provided
        if external_id_type and external_id_type not in [e.value for e in ExternalIDType]:
            raise ValueError(f"Invalid external_id_type: {external_id_type}")
            
        if external_id and not external_id_type:
            raise ValueError("external_id_type is required when external_id is provided")
            
        async with get_session() as session:
            try:
                # Check for duplicate external_id if provided
                if external_id and external_id_type:
                    existing = await session.execute(
                        select(Content)
                        .where(
                            and_(
                                Content.external_id == external_id,
                                Content.external_id_type == external_id_type,
                                Content.external_id.isnot(None)  # Ensure we only match non-null external_ids
                            )
                        )
                    )
                    if existing.scalar_one_or_none():
                        raise ValueError(f"Content with external_id {external_id} already exists")
                
                # Create content
                content = Content(
                    id=uuid4(),
                    type=type,
                    text=text,
                    timestamp=datetime.now(timezone.utc),
                    parent_id=parent_id,
                    external_id=external_id,
                    external_id_type=external_id_type
                )
                session.add(content)
                await session.flush()  # Ensure content is created before associations
                
                # Create user content association
                user_content = UserContent(
                    content_id=content.id,
                    user_id=UUID(user_id),
                    access_level=AccessLevel.OWNER.value,
                    created_by=UUID(user_id),
                    is_owner=True
                )
                session.add(user_content)
                
                # Create content access
                content_access = ContentAccess(
                    content_id=content.id,
                    user_id=UUID(user_id),
                    access_level=AccessLevel.OWNER.value,
                    granted_by=UUID(user_id)
                )
                session.add(content_access)
                
                # Add entities if provided
                if entities:
                    for entity_data in entities:
                        if "name" not in entity_data:
                            raise ValueError("Entity name is required")
                        if "type" not in entity_data or entity_data["type"] not in [e.value for e in EntityType]:
                            raise ValueError("Invalid entity type")
                            
                        entity_name = entity_data["name"]
                        entity_type = entity_data["type"]
                        
                        # Check if entity exists
                        entity_query = select(Entity).where(
                            and_(
                                Entity.name == entity_name,
                                Entity.type == entity_type
                            )
                        )
                        result = await session.execute(entity_query)
                        entity = result.scalar_one_or_none()
                        
                        if not entity:
                            entity = Entity(
                                name=entity_name,
                                type=entity_type
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
                
            except Exception as e:
                await session.rollback()
                if "duplicate" in str(e).lower():
                    raise ValueError(f"Content with external_id {external_id} already exists")
                raise
    
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
                        try:
                            # Handle EntityRequest objects
                            entity_name = entity_data.name
                            entity_type = entity_data.type
                        except AttributeError:
                            # Handle dict input for backward compatibility
                            entity_name = entity_data["name"]
                            entity_type = EntityType(entity_data["type"])
                            
                        # Check if entity exists
                        entity_query = select(Entity).where(
                            and_(
                                Entity.name == entity_name,
                                Entity.type == entity_type
                            )
                        )
                        result = await session.execute(entity_query)
                        entity = result.scalar_one_or_none()
                        
                        if not entity:
                            entity = Entity(
                                name=entity_name,
                                type=entity_type
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
            try:
                # Check if user has owner access and get current access level
                access_check = await session.execute(
                    select(UserContent)
                    .where(and_(
                        UserContent.content_id == content_id,
                        UserContent.user_id == UUID(user_id),
                        UserContent.is_owner == True
                    ))
                )
                user_content = access_check.scalar_one_or_none()
                if not user_content:
                    return False

                # For physical delete, content must be archived first
                if physical_delete and user_content.access_level != AccessLevel.REMOVED.value:
                    return False

                if physical_delete:
                    # Delete in correct order respecting foreign key constraints
                    # First delete content_entity associations
                    await session.execute(delete(content_entity_association).where(content_entity_association.c.content_id == content_id))
                    # Delete transcript access
                    await session.execute(delete(TranscriptAccess).where(TranscriptAccess.transcript_id.in_(
                        select(Transcript.id).where(Transcript.content_id == content_id)
                    )))
                    # Delete transcripts
                    await session.execute(delete(Transcript).where(Transcript.content_id == content_id))
                    # Delete content access
                    await session.execute(delete(ContentAccess).where(ContentAccess.content_id == content_id))
                    # Delete user content
                    await session.execute(delete(UserContent).where(UserContent.content_id == content_id))
                    # Finally delete content
                    await session.execute(delete(Content).where(Content.id == content_id))
                else:
                    # Just mark as removed for all users
                    await session.execute(
                        update(UserContent)
                        .where(UserContent.content_id == content_id)
                        .values(access_level=AccessLevel.REMOVED.value)
                    )
                
                await session.commit()
                return True
            
            except Exception as e:
                await session.rollback()
                logger.error(f"Error deleting content: {str(e)}")
                return False
    
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


    async def update_content_access(
        self,
        content_id: UUID,
        user_id: UUID,
        target_user_id: UUID,
        access_level: AccessLevel,
        session: AsyncSession = None
    ) -> bool:
        async with (session or get_session()) as session:
            async with session.begin():
                # Check if user has owner access
                owner_query = select(ContentAccess).where(
                    and_(
                        ContentAccess.content_id == content_id,
                        ContentAccess.user_id == user_id,
                        ContentAccess.access_level == AccessLevel.OWNER.value
                    )
                )
                result = await session.execute(owner_query)
                if not result.scalar_one_or_none():
                    return False
                
                # Update or create target user's access
                target_query = select(ContentAccess).where(
                    and_(
                        ContentAccess.content_id == content_id,
                        ContentAccess.user_id == target_user_id
                    )
                )
                result = await session.execute(target_query)
                target_access = result.scalar_one_or_none()
                
                if target_access:
                    target_access.access_level = access_level.value
                else:
                    target_access = ContentAccess(
                        content_id=content_id,
                        user_id=target_user_id,
                        granted_by=user_id,
                        access_level=access_level.value
                    )
                    session.add(target_access)
                
                # Also update or create UserContent record for backward compatibility
                user_content_query = select(UserContent).where(
                    and_(
                        UserContent.content_id == content_id,
                        UserContent.user_id == target_user_id
                    )
                )
                result = await session.execute(user_content_query)
                user_content = result.scalar_one_or_none()
                
                if user_content:
                    user_content.access_level = access_level.value
                else:
                    user_content = UserContent(
                        content_id=content_id,
                        user_id=target_user_id,
                        created_by=user_id,
                        access_level=access_level.value,
                        is_owner=False
                    )
                    session.add(user_content)
                
                await session.commit()
                return True 