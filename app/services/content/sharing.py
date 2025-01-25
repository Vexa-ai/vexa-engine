from datetime import datetime, timezone, timedelta
from typing import Optional, List
from uuid import UUID
import secrets

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from psql_models import (
    User, Content, UserContent, ContentType, AccessLevel,
    DefaultAccess, ShareLink
)

async def set_default_access(
    session: AsyncSession, 
    owner_user_id: UUID, 
    granted_user_id: UUID, 
    access_level: AccessLevel
) -> DefaultAccess:
    # First check if both users exist
    users_query = select(User).filter(
        User.id.in_([owner_user_id, granted_user_id])
    )
    result = await session.execute(users_query)
    existing_users = result.scalars().all()
    
    if len(existing_users) != 2:
        raise ValueError("Both owner and granted users must exist in the users table")

    # Check if default access already exists
    existing = await session.execute(
        select(DefaultAccess)
        .filter_by(
            owner_user_id=owner_user_id,
            granted_user_id=granted_user_id
        )
    )
    default_access = existing.scalar_one_or_none()
    
    if default_access:
        # Update existing
        default_access.access_level = access_level.value
    else:
        # Create new
        default_access = DefaultAccess(
            owner_user_id=owner_user_id,
            granted_user_id=granted_user_id,
            access_level=access_level.value
        )
        session.add(default_access)
    
    await session.commit()
    return default_access

async def create_user(
    session: AsyncSession, 
    user_id: UUID, 
    email: str,
    username: str = None,
    first_name: str = None,
    last_name: str = None,
    image: str = None
) -> User:
    user = User(
        id=user_id,
        email=email,
        username=username,
        first_name=first_name,
        last_name=last_name,
        image=image
    )
    session.add(user)
    await session.commit()
    return user

async def update_user_content_access(
    session: AsyncSession,
    owner_id: UUID,
    granted_user_id: UUID,
    access_level: AccessLevel
) -> None:
    """
    Update UserContent records for all content owned by owner_id to grant 
    specified access level to granted_user_id
    """
    # Get all content where the owner has owner access
    owner_content_query = await session.execute(
        select(Content.id)
        .join(UserContent)
        .filter(
            and_(
                UserContent.user_id == owner_id,
                UserContent.is_owner == True,
                Content.type == ContentType.MEETING
            )
        )
    )
    owner_content_ids = owner_content_query.scalars().all()
    
    for content_id in owner_content_ids:
        # Check if UserContent already exists for this user and content
        existing_query = await session.execute(
            select(UserContent)
            .filter(
                and_(
                    UserContent.content_id == content_id,
                    UserContent.user_id == granted_user_id
                )
            )
        )
        existing_access = existing_query.scalar_one_or_none()
        
        if existing_access:
            # Update existing access level
            existing_access.access_level = access_level.value
        else:
            # Create new UserContent record
            new_access = UserContent(
                content_id=content_id,
                user_id=granted_user_id,
                access_level=access_level.value,
                created_by=owner_id,
                is_owner=False
            )
            session.add(new_access)
    
    await session.commit()

async def create_share_link(
    session: AsyncSession,
    owner_id: UUID,
    access_level: AccessLevel,
    content_ids: Optional[List[UUID]] = None,
    target_email: Optional[str] = None,
    expiration_hours: Optional[int] = 24
) -> str:
    """Create a sharing link with specified access level and optional target email"""
    # Verify ownership of content if specific content is being shared
    if content_ids:
        for content_id in content_ids:
            user_content = await session.execute(
                select(UserContent)
                .where(
                    and_(
                        UserContent.content_id == content_id,
                        UserContent.user_id == owner_id,
                        UserContent.is_owner == True
                    )
                )
            )
            if not user_content.scalar_one_or_none():
                raise ValueError(f"User {owner_id} is not the owner of content {content_id}")

    token = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc) + timedelta(hours=expiration_hours) if expiration_hours else None
    
    share_link = ShareLink(
        token=token,
        owner_id=owner_id,
        target_email=target_email.lower() if target_email else None,
        access_level=access_level.value,
        expires_at=expires_at,
        shared_content=content_ids
    )
    
    session.add(share_link)
    await session.commit()
    
    return token

async def accept_share_link(
    session: AsyncSession,
    token: str,
    accepting_user_id: UUID,
    accepting_email: Optional[str] = None
) -> bool:
    """Accept a share link and grant access to the accepting user"""
    
    # Get the share link
    share_link_query = select(ShareLink).where(ShareLink.token == token)
    result = await session.execute(share_link_query)
    share_link = result.scalar_one_or_none()
    
    if not share_link:
        return False
        
    # Check if link is expired
    if share_link.expires_at and share_link.expires_at < datetime.now(timezone.utc):
        return False
        
    # Check if link is already used
    if share_link.is_used:
        return False
        
    # Check if target email matches if specified
    if share_link.target_email and accepting_email:
        if share_link.target_email.lower() != accepting_email.lower():
            return False
            
    # Mark link as used
    share_link.is_used = True
    share_link.accepted_by = accepting_user_id
    share_link.accepted_at = datetime.now(timezone.utc)
    
    # Get content to share
    content = share_link.shared_content or []
    
    # Grant access to each content
    for content_id in content:
        # Check if user already has access
        existing_access = await session.execute(
            select(UserContent)
            .where(
                and_(
                    UserContent.content_id == content_id,
                    UserContent.user_id == accepting_user_id
                )
            )
        )
        existing = existing_access.scalar_one_or_none()
        
        if existing:
            # Update access level if new level is higher
            if AccessLevel(share_link.access_level).value > AccessLevel(existing.access_level).value:
                existing.access_level = share_link.access_level
                existing.created_by = share_link.owner_id
                existing.created_at = datetime.now(timezone.utc)
        else:
            # Create new access
            user_content = UserContent(
                content_id=content_id,
                user_id=accepting_user_id,
                access_level=share_link.access_level,
                created_at=datetime.now(timezone.utc),
                created_by=share_link.owner_id,
                is_owner=False
            )
            session.add(user_content)
    
    await session.commit()
    return True

async def has_content_access(
    session: AsyncSession,
    user_id: UUID,
    content_id: UUID,
    required_level: AccessLevel = AccessLevel.SEARCH
) -> bool:
    result = await session.execute(
        select(UserContent)
        .where(and_(
            UserContent.content_id == content_id,
            UserContent.user_id == user_id,
            UserContent.access_level != AccessLevel.REMOVED.value
        ))
    )
    user_content = result.scalar_one_or_none()
    return user_content is not None and AccessLevel(user_content.access_level) >= required_level




