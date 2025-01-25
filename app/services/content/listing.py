from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, func, distinct, case
from app.models.psql_models import Content, UserContent, Entity, content_entity_association
from app.models.schema.content import ContentListRequest, ContentFilter, AccessLevel

async def list_contents(
    session: AsyncSession,
    user_id: UUID,
    request: ContentListRequest
) -> dict:
    select_columns = [
        Content.id,
        Content.type,
        Content.timestamp,
        Content.is_indexed,
        Content.parent_id,
        UserContent.is_owner,
        UserContent.access_level,
        func.json_agg(
            func.json_build_object(
                'name', Entity.name,
                'type', Entity.type
            )
        ).label('entities')
    ]
    
    base_query = (
        select(*select_columns)
        .join(UserContent, Content.id == UserContent.content_id)
        .outerjoin(content_entity_association, Content.id == content_entity_association.c.content_id)
        .outerjoin(Entity, content_entity_association.c.entity_id == Entity.id)
        .where(
            and_(
                UserContent.user_id == user_id,
                UserContent.access_level != AccessLevel.REMOVED.value
            )
        )
        .group_by(
            Content.id,
            Content.type,
            Content.timestamp,
            Content.is_indexed,
            Content.parent_id,
            UserContent.is_owner,
            UserContent.access_level
        )
    )

    # Add filters if they exist
    if request.filter:
        if request.filter.parent_id is not None:
            base_query = base_query.where(Content.parent_id == request.filter.parent_id)
        if request.filter.entity_type and request.filter.entity_names:
            entity_subquery = (
                select(content_entity_association.c.content_id)
                .join(Entity, content_entity_association.c.entity_id == Entity.id)
                .where(
                    and_(
                        Entity.type == request.filter.entity_type,
                        Entity.name.in_(request.filter.entity_names)
                    )
                )
            )
            base_query = base_query.where(Content.id.in_(entity_subquery))

    # Get total count without the limit/offset
    count_query = (
        select(func.count(distinct(Content.id)))
        .select_from(Content)
        .join(UserContent, Content.id == UserContent.content_id)
        .where(
            and_(
                UserContent.user_id == user_id,
                UserContent.access_level != AccessLevel.REMOVED.value
            )
        )
    )

    # Add pagination
    query = base_query.order_by(Content.timestamp.desc())
    if request.limit:
        query = query.limit(request.limit)
    if request.offset:
        query = query.offset(request.offset)

    # Execute queries
    total_count = await session.scalar(count_query)
    result = await session.execute(query)
    contents = result.all()

    contents_list = [
        {
            "content_id": str(content.id),
            "type": content.type,
            "timestamp": content.timestamp,
            "is_indexed": content.is_indexed,
            "parent_id": str(content.parent_id) if content.parent_id else None,
            "access_level": content.access_level,
            "is_owner": content.is_owner,
            "entities": [
                entity for entity in content.entities 
                if entity and entity.get('name') and entity.get('name') != 'TBD'
            ] if content.entities else []
        }
        for content in contents
    ]
    
    return {
        "total": total_count or 0,
        "contents": contents_list
    } 