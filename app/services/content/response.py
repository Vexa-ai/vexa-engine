from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.models.psql_models import Entity, content_entity_association
from app.models.schema.content import ContentResponse, EntityRef, ContentType, AccessLevel

async def get_content_response(
    session: AsyncSession,
    content,
    user_content
) -> ContentResponse:
    entities_result = await session.execute(
        select(Entity)
        .join(content_entity_association)
        .where(content_entity_association.c.content_id == content.id)
    )
    entities = entities_result.scalars().all()
    
    metadata = {}
    if content.type == ContentType.MEETING.value:
        speaker_entities = [e for e in entities if e.type == 'speaker']
        metadata = {
            "speakers": [e.name for e in speaker_entities if e.name != 'TBD'],
            "start_datetime": content.timestamp
        }
    
    return ContentResponse(
        content_id=content.id,
        type=content.type,
        text=content.text,
        timestamp=content.timestamp,
        last_update=content.last_update or content.timestamp,
        parent_id=content.parent_id,
        entities=[EntityRef(name=e.name, type=e.type) for e in entities],
        is_indexed=content.is_indexed,
        access_level=AccessLevel(user_content.access_level),
        is_owner=user_content.is_owner,
        metadata=metadata
    ) 