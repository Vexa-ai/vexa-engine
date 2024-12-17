from datetime import datetime, timezone, timedelta
from typing import List, Optional, Union, Tuple
from uuid import UUID
from sqlalchemy import select, and_, or_, func, distinct
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

from psql_models import (
    Base, Content, UserContent, ContentType, AccessLevel,
    User, UserToken, Entity, content_entity_association
)
from psql_helpers import get_session, async_session

# Access Control Functions
async def get_user_content_access(session: AsyncSession, user_id: UUID, content_id: UUID) -> AccessLevel:
    """Get user's access level for content"""
    result = await session.execute(
        select(UserContent)
        .filter_by(content_id=content_id, user_id=user_id)
    )
    user_content = result.scalar_one_or_none()
    
    if user_content:
        return AccessLevel(user_content.access_level)
    return AccessLevel.REMOVED

async def can_access_transcript(session: AsyncSession, user_id: UUID, content_id: UUID) -> bool:
    """Check if user has transcript-level access or higher"""
    access = await get_user_content_access(session, user_id, content_id)
    return access in [AccessLevel.TRANSCRIPT, AccessLevel.OWNER]

async def is_content_owner(session: AsyncSession, user_id: UUID, content_id: UUID) -> bool:
    """Check if user is the owner of the content"""
    result = await session.execute(
        select(UserContent)
        .filter_by(content_id=content_id, user_id=user_id)
    )
    user_content = result.scalar_one_or_none()
    return user_content and user_content.access_level == AccessLevel.OWNER.value

# Content Access Functions
async def get_first_content_timestamp(
    session: AsyncSession, 
    user_id: UUID,
    content_type: ContentType = ContentType.MEETING
) -> Optional[str]:
    """Get the timestamp of the user's earliest content of given type"""
    query = (
        select(Content.timestamp)
        .join(UserContent)
        .where(and_(
            UserContent.user_id == user_id,
            Content.type == content_type.value,
            UserContent.access_level != AccessLevel.REMOVED.value
        ))
        .order_by(Content.timestamp.asc())
        .limit(1)
    )
    
    result = await session.execute(query)
    timestamp = result.scalar_one_or_none()
    return timestamp.isoformat() if timestamp else None

async def get_last_content_timestamp(
    session: AsyncSession, 
    user_id: UUID,
    content_type: ContentType = ContentType.MEETING
) -> Optional[str]:
    """Get the timestamp of the user's most recent content of given type"""
    query = (
        select(Content.timestamp)
        .join(UserContent)
        .where(and_(
            UserContent.user_id == user_id,
            Content.type == content_type.value,
            UserContent.access_level != AccessLevel.REMOVED.value
        ))
        .order_by(Content.timestamp.desc())
        .limit(1)
    )
    
    result = await session.execute(query)
    timestamp = result.scalar_one_or_none()
    return timestamp.isoformat() if timestamp else None

# Token Management
async def get_meeting_token(content_id: UUID) -> Optional[str]:
    """Get valid token for content access"""
    async with async_session() as session:
        query = (
            select(UserToken.token)
            .join(UserContent, UserToken.user_id == UserContent.user_id)
            .where(and_(
                UserContent.content_id == content_id,
                UserContent.access_level.in_([
                    AccessLevel.OWNER.value,
                    AccessLevel.TRANSCRIPT.value
                ])
            ))
            .order_by(UserToken.last_used_at.desc())
            .limit(1)
        )
        result = await session.execute(query)
        return result.scalar_one_or_none()

async def get_user_token(user_id: UUID) -> Optional[str]:
    """Get most recently used token for user"""
    async with async_session() as session:
        query = (
            select(UserToken.token)
            .where(UserToken.user_id == user_id)
            .order_by(UserToken.last_used_at.desc())
            .limit(1)
        )
        result = await session.execute(query)
        return result.scalar_one_or_none()

async def get_token_by_email(email: str, session: AsyncSession = None) -> Optional[tuple[str, dict]]:
    """Get token and user data by email"""
    async with (session or get_session()) as session:
        query = select(UserToken, User)\
            .join(User, UserToken.user_id == User.id)\
            .where(User.email == email)\
            .order_by(UserToken.last_used_at.desc())\
            .limit(1)
        
        result = await session.execute(query)
        row = result.first()
        if not row:
            return None
        
        token, user = row
        user_data = {
            'id': user.id,
            'email': user.email,
            'username': user.username,
            'first_name': user.first_name,
            'last_name': user.last_name,
            'image': user.image
        }
        return token.token, user_data

# Content Management
async def get_content_by_user_id(
    user_id: UUID,
    content_type: ContentType = ContentType.MEETING,
    include_transcript: bool = False,
    include_summary: bool = False,
    include_entities: bool = True,
    limit: int = 100,
    offset: int = 0
) -> Tuple[List[dict], int]:
    """Get content items for a user with optional fields"""
    async with async_session() as session:
        # Select columns
        columns = [
            Content.content_id,
            Content.timestamp,
            Content.type,
            UserContent.access_level,
        ]
        
        # Build base query
        query = (
            select(*columns)
            .join(UserContent)
            .where(and_(
                UserContent.user_id == user_id,
                Content.type == content_type.value,
                UserContent.access_level != AccessLevel.REMOVED.value
            ))
        )

        # Get total count
        count_query = select(func.count()).select_from(query.subquery())
        total_count = await session.scalar(count_query)

        # Add ordering and pagination
        query = (
            query
            .order_by(Content.timestamp.desc())
            .offset(offset)
            .limit(limit)
        )

        result = await session.execute(query)
        rows = result.all()

        contents = []
        for row in rows:
            content_dict = {
                "content_id": row.content_id,
                "timestamp": row.timestamp,
                "type": row.type,
                "access_level": row.access_level,
            }
            
            # Add entities if requested
            if include_entities:
                entities_query = (
                    select(Entity.name, Entity.type)
                    .join(content_entity_association)
                    .where(content_entity_association.c.content_id == row.content_id)
                )
                entities_result = await session.execute(entities_query)
                content_dict["entities"] = [
                    {"name": e.name, "type": e.type} 
                    for e in entities_result.fetchall()
                ]
                
            contents.append(content_dict)

        return contents, total_count

async def get_content_by_ids(
    content_ids: List[Union[UUID, str]], 
    user_id: Union[UUID, str],
    content_type: ContentType = ContentType.MEETING,
    session: AsyncSession = None
) -> dict:
    """Get content items by IDs with associated data"""
    if isinstance(user_id, str):
        user_id = UUID(user_id)
    content_ids = [UUID(cid) if isinstance(cid, str) else cid for cid in content_ids]
    
    async with (session or get_session()) as session:
        query = (
            select(
                Content.content_id,
                Content.timestamp,
                Content.type,
                Content.is_indexed,
                UserContent.access_level,
                func.array_agg(distinct(Entity.name)).label('entities')
            )
            .join(UserContent)
            .join(content_entity_association, Content.content_id == content_entity_association.c.content_id)
            .join(Entity)
            .where(and_(
                UserContent.user_id == user_id,
                UserContent.access_level != AccessLevel.REMOVED.value,
                Content.type == content_type.value,
                Content.content_id.in_(content_ids)
            ))
            .group_by(
                Content.content_id,
                Content.timestamp,
                Content.type,
                Content.is_indexed,
                UserContent.access_level
            )
        )

        result = await session.execute(query)
        contents = result.all()
        
        contents_dict = {
            str(content.content_id): {
                "content_id": str(content.content_id),
                "timestamp": content.timestamp.astimezone(timezone.utc).replace(tzinfo=None),
                "type": content.type,
                "is_indexed": content.is_indexed,
                "access_level": content.access_level,
                "entities": [e for e in content.entities if e]
            }
            for content in contents
        }
        
        # Maintain input order
        contents_list = [
            contents_dict[str(cid)]
            for cid in content_ids
            if str(cid) in contents_dict
        ]
        
        return {
            "total": len(contents_list),
            "contents": contents_list
        }

async def clean_content_data(
    content_id: Union[UUID, str], 
    session: AsyncSession = None
) -> bool:
    """Clean all data for a content item"""
    if isinstance(content_id, str):
        content_id = UUID(content_id)
        
    async with (session or get_session()) as session:
        # Get content
        query = select(Content).where(Content.content_id == content_id)
        result = await session.execute(query)
        content = result.scalar_one_or_none()
        
        if not content:
            return False
            
        # Delete child content
        from sqlalchemy import delete
        delete_stmt = delete(Content).where(Content.parent_id == content_id)
        await session.execute(delete_stmt)
        
        # Clean content data
        content.is_indexed = False
        
        await session.commit()
        return True

async def get_accessible_content(
    user_id: UUID,
    content_type: ContentType = ContentType.MEETING,
    access_level: Optional[AccessLevel] = None,
    limit: int = 100,
    offset: int = 0,
    session: AsyncSession = None
) -> tuple[List[dict], int]:
    async with (session or get_session()) as session:
        # Build base query
        query = (
            select(Content, UserContent.access_level)
            .join(UserContent)
            .where(and_(
                UserContent.user_id == user_id,
                Content.type == content_type.value,
                UserContent.access_level != AccessLevel.REMOVED.value
            ))
        )

        # Add access level filter if specified
        if access_level:
            query = query.where(
                UserContent.access_level.in_([
                    level.value for level in AccessLevel 
                    if level.value >= access_level.value
                ])
            )

        # Get total count
        count_query = select(func.count()).select_from(query.subquery())
        total_count = await session.scalar(count_query)

        # Add ordering and pagination
        query = (
            query
            .order_by(Content.timestamp.desc())
            .offset(offset)
            .limit(limit)
        )

        result = await session.execute(query)
        rows = result.all()
        
        contents = [{
            "content_id": str(row.Content.content_id),
            "timestamp": row.Content.timestamp,
            "type": row.Content.type,
            "access_level": row.access_level,
            "is_indexed": row.Content.is_indexed
        } for row in rows]

        return contents, total_count

async def get_user_name(user_id: Union[UUID, str], session: AsyncSession = None) -> Optional[str]:
    """Get user's full name by ID, returns 'First Last' or None if not found"""
    if isinstance(user_id, str):
        user_id = UUID(user_id)
        
    async with (session or get_session()) as session:
        query = select(User.first_name, User.last_name).where(User.id == user_id)
        result = await session.execute(query)
        user = result.first()
        
        if not user:
            return None
            
        first_name, last_name = user
        if first_name and last_name:
            return f"{first_name} {last_name}".strip()
        elif first_name:
            return first_name
        elif last_name:
            return last_name
        return None
