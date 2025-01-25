from uuid import UUID, uuid4
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_

from app.models.psql_models import (
    Content, UserContent, Entity, 
    content_entity_association, AccessLevel
)
from app.models.schema.content import ContentType, EntityType, EntityRef
from app.core.logger import logger
from app.models.schema.content import ContentCreate, ContentResponse, ContentIndexStatus

async def create_content(
    session: AsyncSession,
    user_id: UUID,
    content_data: ContentCreate
) -> ContentResponse:
    logger.debug(f"Creating content: type={content_data.type}, user_id={user_id}")
    
    try:
        # Create new content
        now = datetime.now(timezone.utc)
        new_content = Content(
            id=uuid4(),
            type=content_data.type.value,
            text=content_data.text,
            timestamp=now,
            last_update=now,
            parent_id=content_data.parent_id,
            is_indexed=False
        )
        session.add(new_content)
        await session.flush()
        
        # Create user content association
        user_content = UserContent(
            content_id=new_content.id,
            user_id=user_id,
            access_level=AccessLevel.OWNER.value,
            is_owner=True,
            created_by=user_id
        )
        session.add(user_content)
        
        # Handle entities
        if content_data.entities:
            for entity_ref in content_data.entities:
                # Check if entity exists with exact name AND type match
                entity_result = await session.execute(
                    select(Entity).where(
                        and_(
                            Entity.name == entity_ref.name,
                            Entity.type == entity_ref.type.value
                        )
                    ).limit(1)  # Add limit to prevent multiple results
                )
                entity = entity_result.scalar_one_or_none()
                
                if not entity:
                    # Create new entity
                    entity = Entity(
                        name=entity_ref.name,
                        type=entity_ref.type.value
                    )
                    session.add(entity)
                    await session.flush()
                
                # Create association
                await session.execute(
                    content_entity_association.insert().values(
                        content_id=new_content.id,
                        entity_id=entity.id
                    )
                )
        
        # Explicit entity loading with proper async query
        entities_result = await session.execute(
            select(Entity)
            .join(content_entity_association)
            .where(content_entity_association.c.content_id == new_content.id)
        )
        entities = entities_result.scalars().all()

        await session.commit()  # Ensure commit before returning

        return ContentResponse(
            content_id=new_content.id,
            type=ContentType(new_content.type),
            text=new_content.text,
            timestamp=new_content.timestamp,
            last_update=new_content.last_update,
            parent_id=new_content.parent_id,
            entities=[
                EntityRef(name=e.name, type=EntityType(e.type))
                for e in entities
            ],
            is_indexed=new_content.is_indexed,
            access_level=AccessLevel.OWNER,
            is_owner=True,
            index_status=ContentIndexStatus.PENDING,
            metadata={}
        )

    except Exception as e:
        await session.rollback()
        logger.error(f"Error creating content: {str(e)}", exc_info=True)
        raise

async def update_content(
    session: AsyncSession,
    content_id: UUID,
    user_id: UUID,
    text: str,
    entities: List[EntityRef]
) -> ContentResponse:
    logger.debug(f"Updating content: id={content_id}, user_id={user_id}")
    
    try:
        # Get content with access check
        result = await session.execute(
            select(Content, UserContent)
            .join(UserContent, Content.id == UserContent.content_id)
            .where(and_(
                Content.id == content_id,
                UserContent.user_id == user_id,
                UserContent.access_level != AccessLevel.REMOVED.value
            ))
        )
        row = result.first()
        if not row:
            raise ValueError("Content not found")
            
        content, user_content = row
        
        # Check access level - compare the enum values directly
        if user_content.access_level != AccessLevel.OWNER.value:
            raise ValueError("Insufficient access rights")
        
        if content.type == ContentType.MEETING:
            raise ValueError("Cannot update meeting content")
        
        # Update text if provided
        if text is not None:
            content.text = text
            content.last_update = datetime.now(timezone.utc)
        
        # Update entities if provided
        if entities is not None:
            # Remove existing associations
            await session.execute(
                content_entity_association.delete().where(
                    content_entity_association.c.content_id == content_id
                )
            )
            
            # Add new entities
            for entity_ref in entities:
                entity_result = await session.execute(
                    select(Entity).where(
                        and_(
                            Entity.name == entity_ref.name,
                            Entity.type == entity_ref.type.value
                        )
                    ).limit(1)
                )
                entity = entity_result.scalar_one_or_none()
                
                if not entity:
                    entity = Entity(
                        name=entity_ref.name,
                        type=entity_ref.type.value
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
        logger.info(f"Content updated successfully: id={content_id}")
        return ContentResponse(
            id=content.id,
            type=content.type,
            text=content.text,
            timestamp=content.timestamp,
            parent_id=content.parent_id,
            access_level=user_content.access_level,
            entities=[
                EntityRef(name=e.name, type=e.type) 
                for e in content.entities
            ],
            is_indexed=content.is_indexed,
            index_status=ContentIndexStatus.UPDATED
        )
        
    except Exception as e:
        await session.rollback()
        logger.error(f"Error updating content: {str(e)}", exc_info=True)
        raise

async def archive_content(
    session: AsyncSession,
    content_id: UUID,
    user_id: UUID,
    archive_children: bool = False
) -> None:
    logger.debug(f"Archiving content: id={content_id}, user_id={user_id}")
    
    try:
        # Get content with access check
        result = await session.execute(
            select(Content, UserContent)
            .join(UserContent, Content.id == UserContent.content_id)
            .where(and_(
                Content.id == content_id,
                UserContent.user_id == user_id,
                UserContent.access_level != AccessLevel.REMOVED.value
            ))
        )
        row = result.first()
        if not row:
            raise ValueError("Content not found")
            
        content, user_content = row
        
        # Check access level - compare the actual values
        if user_content.access_level != AccessLevel.OWNER.value:
            raise ValueError("Insufficient access rights")
        
        # Archive content
        user_content.access_level = AccessLevel.REMOVED.value
        
        # Archive children if requested
        if archive_children:
            child_contents = await session.execute(
                select(UserContent)
                .join(Content, Content.id == UserContent.content_id)
                .where(and_(
                    Content.parent_id == content_id,
                    UserContent.user_id == user_id,
                    UserContent.access_level != AccessLevel.REMOVED.value
                ))
            )
            child_user_contents = child_contents.scalars().all()
            
            for child_user_content in child_user_contents:
                child_user_content.access_level = AccessLevel.REMOVED.value
        
        await session.commit()
        logger.info(f"Content archived successfully: id={content_id}")
        
    except Exception as e:
        await session.rollback()
        logger.error(f"Error archiving content: {str(e)}", exc_info=True)
        raise 