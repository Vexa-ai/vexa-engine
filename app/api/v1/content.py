from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_, or_, func, case
from typing import List, Optional
from uuid import UUID, uuid4
from datetime import datetime, timezone
import logging
from enum import Enum

from app.models.schema.content import (
    ContentListRequest, ContentCreate, ContentUpdate,
    ContentResponse, ContentIndexStatus, ContentArchiveRequest,
    ContentType, EntityRef, ContentFilter, AccessLevel
)

from app.services.content.access import get_content_with_access
from app.services.content.response import get_content_response
from app.services.indexing import ContentProcessor
from app.api.deps import get_db, get_current_user
from app.models.psql_models import User
from app.services.content.management import (
    create_content, update_content, archive_content
)
from app.services.content.listing import list_contents

# Set up logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create handlers
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

router = APIRouter(prefix="/api/v1/contents", tags=["content"])

@router.get("", response_model=dict)
async def list_all_contents(
    request: ContentListRequest = Depends(),
    session: AsyncSession = Depends(get_db),
    current_user: tuple = Depends(get_current_user)
):
    user_id, _, _ = current_user
    try:
        return await list_contents(session, user_id, request)
    except Exception as e:
        logger.error(f"Error listing contents: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("", response_model=ContentResponse)
async def create_new_content(
    content: ContentCreate,
    session: AsyncSession = Depends(get_db),
    current_user: tuple = Depends(get_current_user)
):
    user_id, _, _ = current_user
    try:
        content_response = await create_content(
            session=session,
            user_id=user_id,
            content_data=content
        )
        return content_response
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error creating content: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/{content_id}", response_model=ContentResponse)
async def get_content(
    content_id: UUID,
    session: AsyncSession = Depends(get_db),
    current_user: tuple = Depends(get_current_user)
):
    user_id, _, _ = current_user
    try:
        content, user_content = await get_content_with_access(
            session, user_id, content_id, AccessLevel.VIEWER
        )
        return await get_content_response(session, content, user_content)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error getting content: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.put("/{content_id}", response_model=ContentResponse)
async def update_existing_content(
    content_id: UUID,
    content_update: ContentUpdate,
    session: AsyncSession = Depends(get_db),
    current_user: tuple = Depends(get_current_user)
):
    user_id, _, _ = current_user
    try:
        updated_content, user_content = await update_content(
            session=session,
            content_id=content_id,
            user_id=user_id,
            text=content_update.text,
            entities=content_update.entities
        )
        
        return await get_content_response(session, updated_content, user_content)
        
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        logger.error(f"Error updating content: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/{content_id}/archive")
async def archive_content_endpoint(
    content_id: UUID,
    request: ContentArchiveRequest,
    session: AsyncSession = Depends(get_db),
    current_user: tuple = Depends(get_current_user)
):
    user_id, _, _ = current_user
    try:
        await archive_content(
            session=session,
            content_id=content_id,
            user_id=user_id,
            archive_children=request.archive_children
        )
        return {"message": "Content archived successfully"}
        
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        logger.error(f"Error archiving content: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")

@router.post("/{content_id}/index")
async def index_content(
    content_id: UUID,
    session: AsyncSession = Depends(get_db),
    current_user: tuple = Depends(get_current_user)
):
    user_id, _, _ = current_user
    
    # Get content with access check
    content = await get_content_with_access(
        session, user_id, content_id, AccessLevel.VIEWER
    )
    
    # Queue content for indexing
    processor = ContentProcessor()
    await processor.queue_content(content_id)
    
    return {"message": "Content queued for indexing"}

@router.get("/{content_id}/index", response_model=ContentIndexStatus)
async def get_index_status(
    content_id: UUID,
    session: AsyncSession = Depends(get_db),
    current_user: tuple = Depends(get_current_user)
):
    user_id, _, _ = current_user
    
    # Get content with access check
    content = await get_content_with_access(
        session, user_id, content_id, AccessLevel.VIEWER
    )
    
    # Get indexing status
    processor = ContentProcessor()
    status = await processor.get_content_status(content_id)
    
    return status
