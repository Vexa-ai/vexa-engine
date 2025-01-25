from fastapi import FastAPI, HTTPException, Depends, Header, Path, Request, BackgroundTasks, APIRouter, Query
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi_cache import FastAPICache
from fastapi_cache.decorator import cache
from fastapi_cache.backends.redis import RedisBackend

from pydantic import BaseModel, EmailStr, Field
from typing import List, Optional, Dict, Any, AsyncGenerator, Tuple
from uuid import UUID
from datetime import datetime, timezone, timedelta
from enum import Enum

from sqlalchemy import (
    func, select, update, insert, and_, case, distinct, desc
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.psql_models import (
    Content, UserContent, ContentType, AccessLevel,
    User, Thread as ThreadModel, ShareLink, Entity,
    DefaultAccess, EntityType, content_entity_association,
    thread_entity_association, Prompt
)

from app.utils.db import async_session, get_session

from app.services.content.sharing import create_share_link, accept_share_link, has_content_access

import sys

from sqlalchemy.orm import joinedload

from app.services.auth.token_manager import TokenManager
from vexa import VexaAPI, VexaAuth
from app.services.chat.manager import UnifiedChatManager
from app.core.logger import logger
from app.core.prompts import Prompts
from app.services.indexing.redis.keys import RedisKeys
from app.services.search.qdrant import QdrantSearchEngine
from app.services.search.elastic import ElasticsearchBM25

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
from notes.api import router as notes_router

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

token_manager = TokenManager()
thread_manager = ThreadManager()

class Thread(BaseModel):
    thread_id: str
    thread_name: str
    timestamp: datetime

class ChatRequest(BaseModel):
    query: str = Field(..., description="The chat message or query from the user")
    thread_id: Optional[str] = Field(None, description="Optional thread ID to continue a conversation")
    model: Optional[str] = Field(None, description="Optional model name to use for chat")
    temperature: Optional[float] = Field(None, description="Optional temperature parameter for model response randomness")
    meeting_ids: Optional[List[UUID]] = Field(None, description="Optional list of meeting IDs to provide context")
    entities: Optional[List[str]] = Field(None, description="Optional list of entity names to provide context")

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

# Add this middleware after app initialization
@app.middleware("http")
async def log_requests(request: Request, call_next):
    logger.info(f"Request: {request.method} {request.url}")
    logger.debug(f"Headers: {request.headers}")
    
    try:
        response = await call_next(request)
        logger.info(f"Response status: {response.status_code}")
        return response
    except Exception as e:
        logger.error(f"Request failed: {str(e)}", exc_info=True)
        raise

async def get_current_user(authorization: str = Header(...)):
    logger.debug("Checking authorization token")
    token = authorization.split("Bearer ")[-1]
    try:
        user_id, user_name = await token_manager.check_token(token)
        if not user_id:
            logger.warning("Invalid token provided")
            raise HTTPException(status_code=401, detail="Invalid token")
        logger.debug(f"Authenticated user: {user_name} ({user_id})")
        return user_id, user_name, token
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=401, detail="Invalid token")

async def get_thread_by_id(thread_id: str, user_id: str) -> dict:
    """Get a thread by its ID and user ID"""
    async with get_session() as session:
        # Query thread for the user
        result = await session.execute(
            select(ThreadModel)
            .where(
                and_(
                    ThreadModel.thread_id == thread_id,
                    ThreadModel.user_id == UUID(user_id)
                )
            )
        )
        
        thread = result.scalar_one_or_none()
        if not thread:
            raise HTTPException(status_code=404, detail="Thread not found")
            
        # Get associated entities
        entities_result = await session.execute(
            select(Entity)
            .join(thread_entity_association)
            .where(thread_entity_association.c.thread_id == thread.thread_id)
        )
        entities = entities_result.scalars().all()
        
        # Parse messages from JSON string
        messages = json.loads(thread.messages) if thread.messages else []
        
        return {
            "thread_id": thread.thread_id,
            "thread_name": thread.thread_name,
            "timestamp": thread.timestamp,
            "entities": [entity.name for entity in entities],
            "messages": messages
        }


# Initialize search engines
qdrant_engine = QdrantSearchEngine(os.getenv('VOYAGE_API_KEY'))
es_engine = ElasticsearchBM25()

# Initialize chat manager
chat_manager = UnifiedChatManager(
    qdrant_engine=qdrant_engine,
    es_engine=es_engine
)

@app.post("/chat", 
    response_class=StreamingResponse,
    summary="Chat endpoint",
    description="Streaming chat endpoint that handles real-time chat interactions with context from meetings and entities",
    response_description="Server-sent events stream containing chat responses",
    responses={
        200: {
            "description": "Successful chat response stream",
            "content": {"text/event-stream": {}}
        },
        500: {"description": "Internal server error"}
    }
)
async def chat(
    request: ChatRequest,
    background_tasks: BackgroundTasks,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    return await handle_chat_request(request, user_id, background_tasks)

async def handle_chat_request(request: ChatRequest, user_id: str, background_tasks: BackgroundTasks):
    """Handle chat requests by streaming responses from the chat manager."""
    try:
        async def event_generator():
            try:
                async for result in chat_manager.chat(
                    user_id=user_id,
                    query=request.query,
                    meeting_ids=request.meeting_ids,
                    entities=request.entities,
                    thread_id=request.thread_id,
                    model=request.model,
                    temperature=request.temperature
                ):
                    if isinstance(result, dict):
                        yield f"data: {json.dumps(result)}\n\n"
                    else:
                        yield f"data: {json.dumps({'chunk': result})}\n\n"
            except Exception as e:
                logger.error(f"Error in event generator: {str(e)}", exc_info=True)
                yield f"data: {json.dumps({'error': str(e)})}\n\n"

        return StreamingResponse(
            event_generator(),
            media_type='text/event-stream',
            headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'X-Accel-Buffering': 'no'
            }
        )
    except Exception as e:
        logger.error(f"Error in chat request: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/thread/{thread_id}")
async def delete_thread(thread_id: str, current_user: tuple = Depends(get_current_user)):
    user_id, user_name, token = current_user
    return await delete_thread_by_id(thread_id, user_id)

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

@app.get("/content/{content_id}")
async def get_content(
    content_id: UUID,
    authorization: str = Header(...),
    current_user: tuple = Depends(get_current_user)
):
    user_id, _, _ = current_user
    token = authorization.split("Bearer ")[-1]

    async with get_session() as session:
        # Check access
        access_check = await session.execute(
            select(UserContent)
            .where(and_(
                UserContent.content_id == content_id,
                UserContent.user_id == user_id,
                UserContent.access_level != AccessLevel.REMOVED.value
            ))
        )
        
        if not access_check.scalar_one_or_none():
            raise HTTPException(status_code=403, detail="No access to content")

        # Get content details
        content_query = select(Content).where(Content.id == content_id)
        content = await session.scalar(content_query)
        
        if not content:
            raise HTTPException(status_code=404, detail="Content not found")

        # Handle different content types
        if content.type == ContentType.MEETING.value:
            try:
                vexa_api = VexaAPI(token=token)
                transcription = await vexa_api.get_transcription(meeting_session_id=str(content_id))
                if transcription is None:
                    raise HTTPException(status_code=404, detail="Transcript not found")
                
                df, formatted_output, start_datetime, speakers, transcript = transcription
                
                # Get parent content if exists
                parent_data = None
                if content.parent_id:
                    parent = await session.scalar(select(Content).where(Content.id == content.parent_id))
                    if parent:
                        parent_data = {
                            "content_id": str(parent.id),
                            "type": parent.type,
                            "timestamp": parent.timestamp,
                            "last_update": parent.last_update
                        }
                
                return {
                    "content_id": str(content.id),
                    "type": content.type,
                    "text": str(transcript),  # Full transcript data
                    "timestamp": content.timestamp,
                    "last_update": content.last_update,
                    "parent": parent_data,  # Will be None if no parent exists
                    "metadata": {
                        "speakers": speakers,
                        "start_datetime": start_datetime
                    }
                }
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        else:
            # Get parent content if exists
            parent_data = None
            if content.parent_id:
                parent = await session.scalar(select(Content).where(Content.id == content.parent_id))
                if parent:
                    parent_data = {
                        "content_id": str(parent.id),
                        "type": parent.type,
                        "timestamp": parent.timestamp,
                        "last_update": parent.last_update
                    }
            
            # Return content text for non-meeting types
            return {
                "content_id": str(content.id),
                "type": content.type,
                "text": content.text,
                "timestamp": content.timestamp,
                "last_update": content.last_update,
                "parent": parent_data,  # Will be None if no parent exists
                "metadata": {}  # Empty metadata for non-meeting content
            }

from enum import Enum

class MeetingOwnership(str, Enum):
    MY = "my"
    SHARED = "shared"
    ALL = "all"
    
class ContentFilter(BaseModel):
    entity_type: str
    entity_names: List[str]


@app.get("/contents")
async def get_contents(
    authorization: str = Header(None),
    offset: int = 0,
    limit: int = None,
    parent_id: Optional[UUID] = None,
    filter: Optional[ContentFilter] = None,
    current_user: tuple = Depends(get_current_user)
):
    logger.info(f"Getting contents with offset={offset}, limit={limit}, parent_id={parent_id}")
    user_id, user_name, token = current_user
    
    try:
        async with async_session() as session:
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
                        UserContent.access_level != AccessLevel.REMOVED.value,
                        case(
                            (parent_id != None, Content.parent_id == parent_id),
                            else_=Content.parent_id.is_(None)
                        )
                    )
                )
            )

            # Add entity filter if provided
            if filter:
                entity_subquery = (
                    select(content_entity_association.c.content_id)
                    .join(Entity, content_entity_association.c.entity_id == Entity.id)
                    .where(
                        and_(
                            Entity.type == filter.entity_type,
                            Entity.name.in_(filter.entity_names)
                        )
                    )
                )
                base_query = base_query.where(Content.id.in_(entity_subquery))
            
            query = (
                base_query
                .group_by(
                    Content.id,
                    Content.type,
                    Content.timestamp,
                    Content.is_indexed,
                    Content.parent_id,
                    UserContent.is_owner,
                    UserContent.access_level
                )
                .order_by(Content.timestamp.desc())
            )
            
            if limit is not None:
                query = query.limit(limit)
            if offset:
                query = query.offset(offset)

            # Get total count
            count_query = select(func.count(distinct(Content.id))).select_from(
                base_query.subquery()
            )
            total_count = await session.scalar(count_query)

            # Execute main query
            result = await session.execute(query)
            contents = result.all()
            
            contents_list = [
                {
                    "content_id": str(content.id),
                    "type": content.type,
                    "timestamp": content.timestamp.astimezone(timezone.utc).replace(tzinfo=None),
                    "is_indexed": content.is_indexed,
                    "parent_id": str(content.parent_id) if content.parent_id else None,
                    "access_level": content.access_level,
                    "is_owner": content.is_owner,
                    "entities": [
                        entity for entity in content.entities 
                        if entity.get('name') and entity.get('name') != 'TBD'
                    ] if content.entities else []
                }
                for content in contents
            ]
            
            return {
                "total": total_count,
                "contents": contents_list
            }
            
    except Exception as e:
        logger.error(f"Error getting contents: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/threads/global")
async def get_all_threads(current_user: tuple = Depends(get_current_user)):
    user_id, user_name, token = current_user
    
    async with get_session() as session:
        # Query threads that have no content_id and no entity associations
        result = await session.execute(
            select(ThreadModel)
            .outerjoin(thread_entity_association)
            .where(and_(
                ThreadModel.user_id == UUID(user_id),
                ThreadModel.content_id.is_(None),
                thread_entity_association.c.thread_id.is_(None)
            ))
            .order_by(ThreadModel.timestamp.desc())
        )
        
        threads = result.unique().scalars().all()
        
        return [{
            "thread_id": t.thread_id,
            "thread_name": t.thread_name,
            "timestamp": t.timestamp.isoformat() if t.timestamp else None,
        } for t in threads]



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

@app.get("/speakers")
async def get_speakers(current_user: tuple = Depends(get_current_user)):
    user_id, user_name, token = current_user
    
    try:
        async with async_session() as session:
            # Query to get speakers and their latest meeting timestamps
            query = (
                select(
                    Entity.name,
                    func.max(Content.timestamp).label('last_seen'),
                    func.count(distinct(Content.id)).label('meeting_count')
                )
                .join(content_entity_association, Entity.id == content_entity_association.c.entity_id)
                .join(Content, and_(
                    Content.id == content_entity_association.c.content_id,
                    Content.type == ContentType.MEETING
                ))
                .join(UserContent, Content.id == UserContent.content_id)
                .where(
                    and_(
                        UserContent.user_id == user_id,
                        UserContent.access_level != AccessLevel.REMOVED.value,
                        Entity.type == EntityType.SPEAKER,
                        Entity.name != 'TBD',
                        Entity.name != None
                    )
                )
                .group_by(Entity.name)
                .order_by(desc('last_seen'))
            )
            
            result = await session.execute(query)
            speakers = [
                {
                    "name": row.name,
                    "last_seen": row.last_seen.isoformat(),
                    "meeting_count": row.meeting_count
                }
                for row in result
            ]
            
            return {"speakers": speakers}
            
    except Exception as e:
        logger.error(f"Error getting speakers: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

class SpeakerMeetingsRequest(BaseModel):
    speakers: List[str]
    limit: Optional[int] = 50
    offset: Optional[int] = 0

@app.post("/meetings/by-speakers")
async def get_meetings_by_speakers(
    request: SpeakerMeetingsRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    logger.info(f"Getting meetings for speakers: {request.speakers}")
    
    try:
        async with async_session() as session:
            # Subquery to get all meeting IDs where any of the requested speakers participated
            meeting_ids_subquery = (
                select(Content.id)
                .join(content_entity_association, Content.id == content_entity_association.c.content_id)
                .join(Entity, content_entity_association.c.entity_id == Entity.id)
                .where(Entity.name.in_(request.speakers))
                .distinct()
                .scalar_subquery()
            )

            # Main query using the subquery
            query = (
                select(
                    Content.id.label('meeting_id'),
                    Content.timestamp,
                    Content.is_indexed,
                    UserContent.access_level,
                    UserContent.is_owner,
                    func.array_agg(distinct(Entity.name)).label('speakers')
                )
                .join(UserContent, Content.id == UserContent.content_id)
                .join(content_entity_association, Content.id == content_entity_association.c.content_id)
                .join(Entity, content_entity_association.c.entity_id == Entity.id)
                .where(
                    and_(
                        UserContent.user_id == user_id,
                        UserContent.access_level != AccessLevel.REMOVED.value,
                        Content.id.in_(meeting_ids_subquery)
                    )
                )
                .group_by(
                    Content.id,
                    Content.timestamp,
                    Content.is_indexed,
                    UserContent.access_level,
                    UserContent.is_owner
                )
                .order_by(Content.timestamp.desc())
            )

            if request.limit:
                query = query.limit(request.limit)
            if request.offset:
                query = query.offset(request.offset)

            result = await session.execute(query)
            meetings = result.all()
            
            meetings_list = [
                {
                    "meeting_id": str(meeting.meeting_id),
                    "timestamp": meeting.timestamp.astimezone(timezone.utc).replace(tzinfo=None),
                    "is_indexed": meeting.is_indexed,
                    "access_level": meeting.access_level,
                    "is_owner": meeting.is_owner,
                    "speakers": [s for s in meeting.speakers if s and s != 'TBD']
                }
                for meeting in meetings
            ]
            
            # Count query using the same subquery logic
            count_query = (
                select(func.count(distinct(Content.id)))
                .join(UserContent, Content.id == UserContent.content_id)
                .where(
                    and_(
                        UserContent.user_id == user_id,
                        UserContent.access_level != AccessLevel.REMOVED.value,
                        Content.id.in_(meeting_ids_subquery)
                    )
                )
            )

            count = await session.execute(count_query)
            total_count = count.scalar()

            return {
                "meetings": meetings_list,
                "total": total_count
            }

    except Exception as e:
        logger.error(f"Error getting meetings by speakers: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

class MeetingSummaryChatRequest(BaseModel):
    query: str
    meeting_ids: List[UUID]
    thread_id: Optional[str] = None
    model: Optional[str] = None
    temperature: Optional[float] = 0.7


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
from notes.api import router as notes_router

app.include_router(analytics_router)
app.include_router(notes_router)

class ThreadEntitiesRequest(BaseModel):
    entity_names: List[str]

@app.post("/threads/by-entities")
async def get_threads_by_entities(
    request: ThreadEntitiesRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    threads = await thread_manager.get_threads_by_exact_entities(
        user_id=user_id,
        entity_names=request.entity_names
    )
    return threads

class RenameThreadRequest(BaseModel):
    thread_name: str

@app.put("/thread/{thread_id}/rename")
async def rename_thread(
    thread_id: str, 
    request: RenameThreadRequest, 
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    thread = await thread_manager.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    if thread.user_id != user_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    success = await thread_manager.rename_thread(thread_id, request.thread_name)
    if success:
        return {"message": "Thread renamed successfully"}
    else:
        raise HTTPException(status_code=500, detail="Failed to rename thread")

class DeleteContentRequest(BaseModel):
    content_id: UUID

async def get_current_user_id(token: str = Header(...)) -> UUID:
    """Get current user ID from token"""
    try:
        user_id = await token_manager.get_user_id(token)
        return user_id
    except ValueError:
        raise HTTPException(status_code=401, detail="Invalid token")

@app.delete("/api/content/{content_id}")
async def delete_content(
    content_id: UUID,
    session: AsyncSession = Depends(get_session),
    user_id: UUID = Depends(get_current_user_id)
) -> Dict[str, bool]:
    """Delete content for a user and clean up search indices"""
    try:
        # Mark content as deleted in database
        deleted = await mark_content_deleted(session, user_id, content_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Content not found or no access")
            
        # Clean up search indices
        cleaned = await cleanup_search_indices(session, content_id)
        if not cleaned:
            logger.error(f"Failed to clean up search indices for content {content_id}")
            # Don't fail the request if search cleanup fails
            
        return {"success": True}
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error deleting content: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Initialize FastAPI app
app = FastAPI(
    title="Vexa Dashboard API",
    description="API for Vexa Dashboard",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware (keep this before any routes)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://assistant.dev.vexa.ai", "http://localhost:5173", "http://localhost:5174", "https://vexa.ai"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create main router
router = APIRouter()

# Move the contents endpoint to the router
@router.get("/contents")
async def get_contents(
    authorization: str = Header(None),
    offset: int = 0,
    limit: int = None,
    parent_id: Optional[UUID] = None,
    filter: Optional[ContentFilter] = None,
    current_user: tuple = Depends(get_current_user)
):
    logger.info(f"Getting contents with offset={offset}, limit={limit}, parent_id={parent_id}")
    user_id, user_name, token = current_user
    
    try:
        async with async_session() as session:
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
                        UserContent.access_level != AccessLevel.REMOVED.value,
                        case(
                            (parent_id != None, Content.parent_id == parent_id),
                            else_=Content.parent_id.is_(None)
                        )
                    )
                )
            )

            # Add entity filter if provided
            if filter:
                entity_subquery = (
                    select(content_entity_association.c.content_id)
                    .join(Entity, content_entity_association.c.entity_id == Entity.id)
                    .where(
                        and_(
                            Entity.type == filter.entity_type,
                            Entity.name.in_(filter.entity_names)
                        )
                    )
                )
                base_query = base_query.where(Content.id.in_(entity_subquery))
            
            query = (
                base_query
                .group_by(
                    Content.id,
                    Content.type,
                    Content.timestamp,
                    Content.is_indexed,
                    Content.parent_id,
                    UserContent.is_owner,
                    UserContent.access_level
                )
                .order_by(Content.timestamp.desc())
            )
            
            if limit is not None:
                query = query.limit(limit)
            if offset:
                query = query.offset(offset)

            # Get total count
            count_query = select(func.count(distinct(Content.id))).select_from(
                base_query.subquery()
            )
            total_count = await session.scalar(count_query)

            # Execute main query
            result = await session.execute(query)
            contents = result.all()
            
            contents_list = [
                {
                    "content_id": str(content.id),
                    "type": content.type,
                    "timestamp": content.timestamp.astimezone(timezone.utc).replace(tzinfo=None),
                    "is_indexed": content.is_indexed,
                    "parent_id": str(content.parent_id) if content.parent_id else None,
                    "access_level": content.access_level,
                    "is_owner": content.is_owner,
                    "entities": [
                        entity for entity in content.entities 
                        if entity.get('name') and entity.get('name') != 'TBD'
                    ] if content.entities else []
                }
                for content in contents
            ]
            
            return {
                "total": total_count,
                "contents": contents_list
            }
            
    except Exception as e:
        logger.error(f"Error getting contents: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# Include all routers
app.include_router(router)
app.include_router(analytics_router)
app.include_router(notes_router)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=8010, reload=True)
    
    # conda activate langchain && uvicorn app:app --host 0.0.0.0 --port 8765 --workers 1 --loop uvloop --reload

