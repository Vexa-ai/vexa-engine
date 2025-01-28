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
from routers.auth import router as auth_router
from routers.chat import router as chat_router

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

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize FastAPI cache with Redis backend
    FastAPICache.init(
        backend=RedisBackend(redis_client),
        prefix="fastapi-cache"
    )
    yield

app = FastAPI(lifespan=lifespan)

# Move this BEFORE any other middleware or app setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://assistant.dev.vexa.ai", "http://localhost:5173", "http://localhost:5174","https://vexa.ai"],  # Must be explicit
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Other middleware and routes should come after CORS middleware



class CreateShareLinkRequest(BaseModel):
    access_level: str
    meeting_ids: Optional[List[UUID]] = None
    target_email: Optional[EmailStr] = None
    expiration_hours: Optional[int] = None

class CreateShareLinkResponse(BaseModel):
    token: str

class AcceptShareLinkRequest(BaseModel):
    token: str
    accepting_email: Optional[EmailStr] = None
    

REDIS_HOST=os.getenv('REDIS_HOST', '127.0.0.1')
if REDIS_HOST == '127.0.0.1':
    DEV = True
REDIS_PORT=int(os.getenv('REDIS_PORT', 6379))

# Initialize Redis connection
redis_client = redis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}")

# Add logging configuration after the imports and before app initialization
def setup_logger():
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    logger = logging.getLogger('vexa_api')
    logger.setLevel(logging.DEBUG)

    # Create formatters
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Create and setup file handler
    file_handler = logging.handlers.RotatingFileHandler(
        'logs/api.log',
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    file_handler.setFormatter(formatter)
    
    # Create and setup console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    
    # Add both handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logger()


@app.post("/share-links", response_model=CreateShareLinkResponse)
async def create_new_share_link(
    request: CreateShareLinkRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    
    try:
        access_level = AccessLevel(request.access_level)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid access level. Must be one of: {[e.value for e in AccessLevel]}"
        )
    
    try:
        async with async_session() as session:
            token = await create_share_link(
                session=session,
                owner_id=user_id,
                access_level=access_level,
                meeting_ids=request.meeting_ids,
                target_email=request.target_email,
                expiration_hours=request.expiration_hours
            )
            
        return CreateShareLinkResponse(token=token)
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))

@app.post("/share-links/accept")
async def accept_new_share_link(
    request: AcceptShareLinkRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    
    async with async_session() as session:
        success = await accept_share_link(
            session=session,
            token=request.token,
            accepting_user_id=user_id,
            accepting_email=request.accepting_email
        )
        
    if not success:
        raise HTTPException(
            status_code=400,
            detail="Invalid or expired share link"
        )
        
    return {"message": "Share link accepted successfully"}




from indexing.redis_keys import RedisKeys
from redis import Redis
from datetime import datetime

# Add this near other Redis initialization
REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
redis_client = Redis(host=REDIS_HOST, port=REDIS_PORT)

class IndexMeetingRequest(BaseModel):
    meeting_id: UUID

@app.post("/meetings/{meeting_id}/index")
async def index_meeting(
    meeting_id: UUID,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    meeting_id_str = str(meeting_id)
    
    try:
        async with get_session() as session:
            if not await has_content_access(session, user_id, meeting_id):
                raise HTTPException(status_code=403, detail="No access to meeting")
            
            # Check if meeting is already being processed
            if redis_client.sismember(RedisKeys.PROCESSING_SET, meeting_id_str):
                return {"status": "already_processing", "message": "Meeting is already being processed"}
            
            # Check if meeting is already in queue
            if redis_client.zscore(RedisKeys.INDEXING_QUEUE, meeting_id_str) is not None:
                return {"status": "already_queued", "message": "Meeting is already in indexing queue"}
            
            # Check if meeting is in failed state
            failed_info = redis_client.hget(RedisKeys.FAILED_SET, meeting_id_str)
            if failed_info:
                redis_client.hdel(RedisKeys.FAILED_SET, meeting_id_str)
            
            # Add to indexing queue with current timestamp
            redis_client.zadd(RedisKeys.INDEXING_QUEUE, {meeting_id_str: datetime.now().timestamp()})
            
            return {
                "status": "queued",
                "message": "Meeting has been queued for indexing"
            }
            
    except Exception as e:
        logger.error(f"Error queuing meeting for indexing: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# Add this with other router includes
app.include_router(auth_router)
app.include_router(analytics_router)
app.include_router(chat_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8010, reload=True)
    
    # conda activate langchain && uvicorn app:app --host 0.0.0.0 --port 8765 --workers 1 --loop uvloop --reload

