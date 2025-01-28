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




class ChatRequest(BaseModel):
    query: str = Field(..., description="The chat message or query from the user")
    thread_id: Optional[str] = Field(None, description="Optional thread ID to continue a conversation")
    model: Optional[str] = Field(None, description="Optional model name to use for chat")
    temperature: Optional[float] = Field(None, description="Optional temperature parameter for model response randomness")
    content_id: Optional[UUID] = Field(None, description="Optional single content ID to provide context")
    entity: Optional[str] = Field(None, description="Optional single entity name to associate with thread")
    content_ids: Optional[List[UUID]] = Field(None, description="Optional list of content IDs to provide context")
    entities: Optional[List[str]] = Field(None, description="Optional list of entity names to scope search")
    meta: Optional[Dict[str, Any]] = Field(None, description="Optional metadata for the chat")

class TokenRequest(BaseModel):
    token: str
    
class MeetingTimestamps(BaseModel):
    first_meeting: Optional[str]
    last_meeting: Optional[str]

# Add this new class for the indexing request
class IndexingRequest(BaseModel):
    num_meetings: Optional[int] = 200

class MeetingsProcessedResponse(BaseModel):
    meetings_processed: int
    total_meetings: int

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


# Initialize search engines
qdrant_engine = QdrantSearchEngine(os.getenv('VOYAGE_API_KEY'))
es_engine = ElasticsearchBM25()

# Initialize chat manager
chat_manager = UnifiedChatManager(
    qdrant_engine=qdrant_engine,
    es_engine=es_engine
)

@app.post("/chat")
async def chat(
    request: Request,
    query: str = Body(...),
    content_id: Optional[UUID] = Body(None),
    entity: Optional[str] = Body(None),
    content_ids: Optional[List[UUID]] = Body(None),
    entities: Optional[List[str]] = Body(None),
    thread_id: Optional[str] = Body(None),
    model: Optional[str] = Body(None),
    temperature: Optional[float] = Body(None),
    meta: Optional[dict] = Body(None),
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    
    # Validate ID combinations
    if content_id and entity:
        raise HTTPException(
            status_code=400,
            detail="Cannot specify both content_id and entity for thread mapping"
        )
        
    if content_ids and entities:
        raise HTTPException(
            status_code=400,
            detail="Cannot specify both content_ids and entities for search scope"
        )
        
    if (content_id and content_ids) or (entity and entities):
        raise HTTPException(
            status_code=400,
            detail="Cannot specify both single and multiple values of the same type"
        )
    
    # Validate thread access if thread_id provided
    if thread_id:
        async with get_session() as session:
            thread = await session.scalar(
                select(Thread).where(
                    Thread.thread_id == thread_id,
                    Thread.user_id == UUID(user_id)
                )
            )
            if not thread:
                raise HTTPException(
                    status_code=404,
                    detail="Thread not found or no access"
                )
    
    # Initialize chat manager
    chat_manager = UnifiedChatManager(
        session=get_session(),
        qdrant_engine=qdrant_engine,
        es_engine=es_engine
    )
    
    # Create response stream
    async def generate():
        async for chunk in chat_manager.chat(
            user_id=user_id,
            query=query,
            content_id=content_id,
            entity=entity,
            content_ids=content_ids,
            entities=entities,
            thread_id=thread_id,
            model=model,
            temperature=temperature,
            meta=meta
        ):
            if "error" in chunk:
                yield f"data: {json.dumps(chunk)}\n\n"
                return
            elif "chunk" in chunk:
                yield f"data: {json.dumps({'chunk': chunk['chunk']})}\n\n"
            else:
                # Final response with metadata
                response_data = {
                    'thread_id': chunk['thread_id'],
                    'content_id': str(content_id) if content_id else None,
                    'entity': entity,
                    'content_ids': [str(cid) for cid in content_ids] if content_ids else None,
                    'entities': entities,
                    'output': chunk['output'],
                    'linked_output': chunk.get('linked_output', chunk['output']),
                    'service_content': chunk.get('service_content', {})
                }
                yield f"data: {json.dumps(response_data)}\n\n"
    
    return StreamingResponse(
        generate(),
        media_type="text/event-stream"
    )

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



class GoogleAuthRequest(BaseModel):
    utm_source: Optional[str] = None
    utm_medium: Optional[str] = None
    utm_campaign: Optional[str] = None
    utm_term: Optional[str] = None
    utm_content: Optional[str] = None
    ref: Optional[str] = None  # Added ref parameter
    token: str

@app.post("/auth/google", response_model=dict)
async def google_auth(request: GoogleAuthRequest):
    try:
        vexa_auth = VexaAuth()
        params = {k: v for k, v in {
            "utm_source": request.utm_source,
            "utm_medium": request.utm_medium,
            "utm_campaign": request.utm_campaign,
            "utm_term": request.utm_term,
            "utm_content": request.utm_content,
            "ref": request.ref  # Added ref parameter
        }.items() if v is not None}
        
        # Increase delay and add retry logic
        max_retries = 3
        base_delay = 1
        
        for attempt in range(max_retries):
            try:
                await asyncio.sleep(base_delay * (attempt + 1))
                result = await vexa_auth.google_auth(
                    token=request.token,
                    utm_params=params
                )
                return result
            except Exception as e:
                if "Token used too early" in str(e) and attempt < max_retries - 1:
                    continue
                raise
                
    except Exception as e:
        logger.error(f"Google auth failed: {str(e)}", exc_info=True)
        if "Token used too early" in str(e):
            raise HTTPException(
                status_code=400,
                detail="Authentication timing error. Please try again."
            )
        raise HTTPException(status_code=500, detail=str(e))



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

from analytics.api import router as analytics_router

app.include_router(analytics_router)


@app.get("/api/entities/{entity_type}")
async def get_entities(
    entity_type: str,
    offset: int = 0,
    limit: int = 20,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    
    # Validate entity type before any DB operations
    if entity_type not in [e.value for e in EntityType]:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid entity type. Must be one of: {[e.value for e in EntityType]}"
        )
    
    try:
        async with async_session() as session:
            # Query to get entities and their latest content timestamps
            query = (
                select(
                    Entity.id,
                    Entity.name,
                    func.max(Content.timestamp).label('last_seen'),
                    func.count(distinct(Content.id)).label('content_count')
                )
                .join(content_entity_association, Entity.id == content_entity_association.c.entity_id)
                .join(Content, Content.id == content_entity_association.c.content_id)
                .join(UserContent, Content.id == UserContent.content_id)
                .where(
                    and_(
                        UserContent.user_id == user_id,
                        UserContent.access_level != AccessLevel.REMOVED.value,
                        Entity.type == entity_type,
                        Entity.name != 'TBD',
                        Entity.name != None
                    )
                )
                .group_by(Entity.id, Entity.name)
                .order_by(desc('last_seen'))
                .offset(offset)
                .limit(limit)
            )
            
            result = await session.execute(query)
            entities = [
                {
                    "id": row.id,
                    "name": row.name,
                    "last_seen": row.last_seen.isoformat() if row.last_seen else None,
                    "content_count": row.content_count
                }
                for row in result
            ]
            
            # Get total count
            count_query = (
                select(func.count(distinct(Entity.name)))
                .select_from(Entity)
                .join(content_entity_association, Entity.id == content_entity_association.c.entity_id)
                .join(Content, Content.id == content_entity_association.c.content_id)
                .join(UserContent, Content.id == UserContent.content_id)
                .where(
                    and_(
                        UserContent.user_id == user_id,
                        UserContent.access_level != AccessLevel.REMOVED.value,
                        Entity.type == entity_type,
                        Entity.name != 'TBD',
                        Entity.name != None
                    )
                )
            )
            
            total_count = await session.execute(count_query)
            total = total_count.scalar() or 0
            
            return {
                "total": total,
                "entities": entities
            }
            
    except Exception as e:
        logger.error(f"Error getting entities: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8010, reload=True)
    
    # conda activate langchain && uvicorn app:app --host 0.0.0.0 --port 8765 --workers 1 --loop uvloop --reload

