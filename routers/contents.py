from fastapi import FastAPI, HTTPException, Depends, Header, Path, Request, BackgroundTasks, Query, Body
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.decorator import cache
from fastapi_cache.backends.redis import RedisBackend

from pydantic import BaseModel, EmailStr, Field
from typing import List, Optional, Dict, Any, AsyncGenerator, Tuple
from uuid import UUID, uuid4
from datetime import datetime, timezone, timedelta
from enum import Enum

from sqlalchemy import (
    func, select, update, insert, and_, case, distinct, desc, or_, text, delete
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from psql_models import (
    User, Content, Entity, Thread, ContentType, 
    EntityType, content_entity_association, thread_entity_association,
    AccessLevel, UserContent
)

from psql_helpers import (
    async_session, get_session,

)

from psql_sharing import (
    create_share_link, accept_share_link,has_content_access
)

import sys

from sqlalchemy.orm import joinedload

from token_manager import TokenManager
from vexa import VexaAPI, VexaAuth
from chat import UnifiedChatManager
from logger import logger
from prompts import Prompts
from indexing.redis_keys import RedisKeys
from qdrant_search import QdrantSearchEngine
from bm25_search import ElasticsearchBM25

import redis
import os
import json
import pandas as pd
import httpx
import asyncio
import logging

from qdrant_search import QdrantSearchEngine
from bm25_search import ElasticsearchBM25

from thread_manager import ThreadManager

from analytics.api import router as analytics_router

from psql_access import (
    get_user_content_access, can_access_transcript,
    is_content_owner, get_first_content_timestamp,
    get_last_content_timestamp, get_meeting_token,
    get_user_token, get_token_by_email,
    get_content_by_user_id, get_content_by_ids,
    clean_content_data, get_accessible_content,
    get_user_name, has_content_access,
    get_content_token, mark_content_deleted,
    cleanup_search_indices
)

from content_manager import ContentManager
from routers.common import get_current_user

router = FastAPI(prefix="/contents", tags=["contents"])
content_manager = ContentManager()

class MeetingOwnership(str, Enum):
    MY = "my"
    SHARED = "shared"
    ALL = "all"
    
    
class ContentFilter(BaseModel):
    type: str
    values: List[str]


@router.get("/all")
async def get_contents(
    content_type: Optional[str] = None,
    filters: Optional[str] = None,
    parent_id: Optional[UUID] = None,
    offset: int = Query(0),
    limit: int = Query(20),
    ownership: str = Query(MeetingOwnership.ALL),
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    filter_list = []
    if filters:
        try:
            filter_list = [ContentFilter(**f).dict() for f in json.loads(filters)]
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid filters format: {str(e)}")
    
    return await content_manager.get_contents(
        user_id=user_id,
        content_type=content_type,
        filters=filter_list,
        parent_id=parent_id,
        offset=offset,
        limit=limit,
        ownership=ownership
    )

@router.get("/{content_id}")
async def get_content(
    content_id: UUID,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    content = await content_manager.get_content(
        user_id=user_id,
        content_id=content_id,
        token=token
    )
    if not content:
        raise HTTPException(status_code=404, detail="Content not found or no access")
    return content

@router.post("")
async def add_content(
    request: AddContentRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    try:
        content_type = ContentType(request.type)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid content type. Must be one of: {[e.value for e in ContentType]}"
        )
    
    content_id = await content_manager.add_content(
        user_id=user_id,
        type=content_type.value,
        text=request.text,
        parent_id=request.parent_id,
        entities=request.entities
    )
    return {"content_id": content_id}

@router.put("/{content_id}")
async def modify_content(
    content_id: UUID,
    request: ModifyContentRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    success = await content_manager.modify_content(
        user_id=user_id,
        content_id=content_id,
        text=request.text,
        entities=request.entities
    )
    if not success:
        raise HTTPException(status_code=404, detail="Content not found or no access")
    return {"success": True}

@router.delete("/{content_id}")
async def delete_content(
    content_id: UUID,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    success = await content_manager.delete_content(
        user_id=user_id,
        content_id=content_id
    )
    if not success:
        raise HTTPException(status_code=404, detail="Content not found or no access")
    return {"success": True}

@router.post("/{content_id}/archive")
async def archive_content(
    content_id: UUID,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    success = await content_manager.archive_content(
        user_id=user_id,
        content_id=content_id
    )
    if not success:
        raise HTTPException(status_code=404, detail="Content not found or no access")
    return {"success": True}

@router.post("/{content_id}/restore")
async def restore_content(
    content_id: UUID,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    success = await content_manager.restore_content(
        user_id=user_id,
        content_id=content_id
    )
    if not success:
        raise HTTPException(status_code=404, detail="Content not found or not archived")
    return {"success": True}

