from fastapi import FastAPI, HTTPException, Depends, Header, Path, Request, BackgroundTasks
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
    func, select, update, insert, and_, case, distinct, desc
)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from psql_models import (
    Content, UserContent, ContentType, AccessLevel,
    User, Thread as ThreadModel, ShareLink, Entity,
    DefaultAccess, EntityType, content_entity_association,
    thread_entity_association
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

async def get_user_threads(user_id: str):
    async with get_session() as session:
        # Query threads for the user
        result = await session.execute(
            select(ThreadModel)
            .where(ThreadModel.user_id == UUID(user_id))
            .order_by(ThreadModel.timestamp.desc())
        )
        
        threads = result.scalars().all()
        
        return [{
            "thread_id": thread.thread_id,
            "thread_name": thread.thread_name,
            "timestamp": thread.timestamp
        } for thread in threads]

@app.post("/submit_token")
async def submit_token(request: TokenRequest):
    user_id, user_name, image = await token_manager.submit_token(request.token)
    if user_id is None or user_name is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    return {"user_id": user_id, "user_name": user_name, "image": image}

@app.get("/threads", response_model=List[Thread])
async def get_threads(current_user: tuple = Depends(get_current_user)):
    user_id, user_name, token = current_user
    return await get_user_threads(user_id)

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

@app.get("/thread/{thread_id}")
async def get_thread(thread_id: str, current_user: tuple = Depends(get_current_user)):
    user_id, user_name, token = current_user
    return await get_thread_by_id(thread_id, user_id)

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

@app.get("/meeting/{meeting_id}")
async def get_transcript(
    meeting_id: str,
    authorization: str = Header(...)
):
    token = authorization.split("Bearer ")[-1]
    vexa_api = VexaAPI(token=token)

    try:
        transcript = await vexa_api.get_transcription_(meeting_session_id=meeting_id)
        if transcript is None:
            raise HTTPException(status_code=404, detail="Transcript not found")
        
        return transcript
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

from enum import Enum

class MeetingOwnership(str, Enum):
    MY = "my"
    SHARED = "shared"
    ALL = "all"
    
    
class ContentFilter(BaseModel):
    type: str
    values: List[str]

class GetContentsRequest(BaseModel):
    content_type: Optional[str] = None
    filters: Optional[List[ContentFilter]] = None
    parent_id: Optional[UUID] = None
    limit: int = 20
    offset: int = 0
    ownership: MeetingOwnership = MeetingOwnership.ALL

@app.get("/contents/all")
async def get_contents(
    content_type: Optional[str] = None,
    filters: Optional[str] = None,  # JSON string of filters
    parent_id: Optional[UUID] = None,
    offset: int = 0,
    limit: int = 20,
    ownership: MeetingOwnership = MeetingOwnership.ALL,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    logger.info(f"Getting contents with type={content_type}, filters={filters}, parent_id={parent_id}")
    
    try:
        # Parse filters if provided
        filter_list = []
        if filters:
            filter_list = [ContentFilter(**f) for f in json.loads(filters)]
            
        async with async_session() as session:
            # Base columns to select
            select_columns = [
                Content.id.label('content_id'),
                Content.timestamp,
                Content.type,
                Content.is_indexed,
                UserContent.is_owner,
                UserContent.access_level,
                func.array_agg(distinct(Entity.name)).label('entities')
            ]
            
            # Base query
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
                            (ownership == MeetingOwnership.MY, UserContent.is_owner == True),
                            (ownership == MeetingOwnership.SHARED, UserContent.is_owner == False),
                            else_=True
                        )
                    )
                )
            )
            
            # Add content type filter if specified
            if content_type:
                base_query = base_query.where(Content.type == content_type)
                
            # Add parent filter if specified
            if parent_id:
                base_query = base_query.where(Content.parent_id == parent_id)
                
            # Apply additional filters
            for filter_item in filter_list:
                if filter_item.type == "speakers":
                    base_query = base_query.where(Entity.name.in_(filter_item.values))
                elif filter_item.type == "date":
                    # Assuming date values are in ISO format
                    start_date, end_date = filter_item.values
                    base_query = base_query.where(
                        and_(
                            Content.timestamp >= start_date,
                            Content.timestamp <= end_date
                        )
                    )
                    
            # Group by and order
            query = (
                base_query
                .group_by(
                    Content.id,
                    Content.timestamp,
                    Content.type,
                    Content.is_indexed,
                    UserContent.is_owner,
                    UserContent.access_level
                )
                .order_by(Content.timestamp.desc())
            )
            
            # Apply pagination
            if limit:
                query = query.limit(limit)
            if offset:
                query = query.offset(offset)

            # Execute query
            result = await session.execute(query)
            contents = result.all()
            
            # Format results
            contents_list = [
                {
                    "content_id": str(content.content_id),
                    "timestamp": content.timestamp.astimezone(timezone.utc).replace(tzinfo=None),
                    "type": content.type,
                    "is_indexed": content.is_indexed,
                    "access_level": content.access_level,
                    "is_owner": content.is_owner,
                    "entities": [e for e in content.entities if e and e != 'TBD']
                }
                for content in contents
            ]
            
            # Get total count
            count_query = (
                select(func.count(distinct(Content.id)))
                .join(UserContent, Content.id == UserContent.content_id)
                .where(
                    and_(
                        UserContent.user_id == user_id,
                        UserContent.access_level != AccessLevel.REMOVED.value,
                        case(
                            (ownership == MeetingOwnership.MY, UserContent.is_owner == True),
                            (ownership == MeetingOwnership.SHARED, UserContent.is_owner == False),
                            else_=True
                        )
                    )
                )
            )
            
            if content_type:
                count_query = count_query.where(Content.type == content_type)
            if parent_id:
                count_query = count_query.where(Content.parent_id == parent_id)
                
            total_count = await session.execute(count_query)
            total = total_count.scalar() or 0
            
            return {
                "total": total,
                "contents": contents_list
            }
            
    except Exception as e:
        logger.error(f"Error getting contents: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/meeting/{meeting_id}/details")
async def get_meeting_details(
    meeting_id: UUID = Path(..., description="The UUID of the meeting"),
    current_user: tuple = Depends(get_current_user),
    authorization: str = Header(...)
):
    user_id, user_name, token = current_user
    token = authorization.split("Bearer ")[-1]
    
    async with get_session() as session:
        # Query for meeting details
        query = (
            select(Content, UserContent)
            .join(UserContent, Content.id == UserContent.content_id)
            .where(
                and_(
                    Content.id == meeting_id,
                    Content.type == ContentType.MEETING,
                    UserContent.user_id == user_id,
                    UserContent.access_level != AccessLevel.REMOVED.value
                )
            )
        )
        
        result = await session.execute(query)
        row = result.first()
        
        if not row:
            raise HTTPException(status_code=404, detail="Meeting not found or access denied")
            
        content, user_content = row
        
        transcript_data = None
        speakers = []
        
        try:
            vexa_api = VexaAPI(token=token)
            transcription = await vexa_api.get_transcription(meeting_session_id=str(meeting_id))
            
            if transcription:
                df, _, start_datetime, speakers, transcript = transcription
                utc_timestamp = start_datetime.astimezone(timezone.utc).replace(tzinfo=None)
                
                # Update database with latest transcript
                await session.execute(
                    update(Content)
                    .where(Content.id == meeting_id)
                    .values(
                        text=str(transcript),
                        timestamp=utc_timestamp
                    )
                )
                
                # Update or create speaker entities
                for speaker_name in speakers:
                    if speaker_name and speaker_name != 'TBD':
                        entity_query = select(Entity).where(
                            and_(
                                Entity.name == speaker_name,
                                Entity.type == EntityType.SPEAKER
                            )
                        )
                        existing_entity = await session.execute(entity_query)
                        entity = existing_entity.scalar_one_or_none()
                        
                        if not entity:
                            entity = Entity(name=speaker_name, type=EntityType.SPEAKER)
                            session.add(entity)
                            await session.flush()
                        
                        await session.execute(
                            pg_insert(content_entity_association)
                            .values(
                                content_id=content.id,
                                entity_id=entity.id
                            )
                            .on_conflict_do_nothing()
                        )
                
                await session.commit()
                transcript_data = transcript
                speakers = list(set(segment.get('speaker') for segment in transcript_data 
                            if segment.get('speaker') and segment.get('speaker') != 'TBD'))
        except Exception as e:
            logger.error(f"Failed to fetch transcript from Vexa: {str(e)}")
            if content.text:
                transcript_data = eval(content.text)
                speakers = list(set(segment.get('speaker') for segment in transcript_data 
                            if segment.get('speaker') and segment.get('speaker') != 'TBD'))
        
        # Get speakers from entity associations if not available from transcript
        if not speakers:
            speaker_query = (
                select(Entity.name)
                .join(content_entity_association, Entity.id == content_entity_association.c.entity_id)
                .where(
                    and_(
                        content_entity_association.c.content_id == content.id,
                        Entity.type == EntityType.SPEAKER
                    )
                )
            )
            speaker_result = await session.execute(speaker_query)
            speakers = [row[0] for row in speaker_result if row[0] and row[0] != 'TBD']
        
        response = {
            "meeting_id": str(content.id),
            "timestamp": content.timestamp,

            "transcript": json.dumps(transcript_data) if transcript_data else [],
            "meeting_summary": '',
            "speakers": speakers,
            "access_level": user_content.access_level,
            "is_owner": user_content.is_owner,
            "is_indexed": content.is_indexed
        }
        
        return response


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

@app.get("/threads/meeting/{meeting_id}")
async def get_meeting_threads(meeting_id: UUID, current_user: tuple = Depends(get_current_user)):
    user_id, user_name, token = current_user
    logger.info(f"Getting threads for meeting {meeting_id} by user {user_id}")
    
    async with get_session() as session:
        # First check if meeting exists
        meeting_check = await session.execute(
            select(Content)
            .where(and_(
                Content.id == meeting_id,
                Content.type == ContentType.MEETING
            ))
        )
        
        if not meeting_check.scalar_one_or_none():
            logger.warning(f"Meeting {meeting_id} not found")
            raise HTTPException(status_code=404, detail="Meeting not found")
            
        # Check access using UserContent
        access_check = await session.execute(
            select(UserContent)
            .where(and_(
                UserContent.content_id == meeting_id,
                UserContent.user_id == user_id,
                UserContent.access_level != AccessLevel.REMOVED.value
            ))
        )
        
        if not access_check.scalar_one_or_none():
            logger.warning(f"User {user_id} has no access to meeting {meeting_id}")
            raise HTTPException(status_code=403, detail="No access to meeting")
            
        # Get threads associated with the content
        result = await session.execute(
            select(ThreadModel)
            .where(and_(
                ThreadModel.user_id == user_id,
                ThreadModel.content_id == meeting_id
            ))
            .order_by(ThreadModel.timestamp.desc())
        )
        
        threads = [{
            "thread_id": t.thread_id,
            "thread_name": t.thread_name,
            "timestamp": t.timestamp
        } for t in result.scalars().all()]
        
        logger.info(f"Found {len(threads)} threads for meeting {meeting_id}")
        return threads

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

app.include_router(analytics_router)


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

class AddContentRequest(BaseModel):
    parent_id: Optional[UUID] = None
    entities: List[Dict[str, str]] = Field(default_factory=list, description="List of entities with type and name")
    body: str = Field(..., description="Content text body")
    type: ContentType = Field(..., description="Type of content")

@app.post("/api/content", response_model=Dict[str, Any])
async def add_content(
    request: AddContentRequest,
    current_user: tuple = Depends(get_current_user)
) -> Dict[str, Any]:
    """Add new content with optional parent and entities"""
    user_id, user_name, token = current_user
    
    try:
        async with get_session() as session:
            logger.debug(f"Creating content with type={request.type}, body={request.body}")
            # Create content
            content = Content(
                id=uuid4(),
                type=request.type.value,
                text=request.body,
                timestamp=datetime.now(timezone.utc).replace(tzinfo=None),
                parent_id=request.parent_id,
                last_update=datetime.now(timezone.utc).replace(tzinfo=None)
            )
            session.add(content)
            await session.flush()
            logger.debug(f"Created content with id={content.id}")

            # Create user content access
            user_content = UserContent(
                content_id=content.id,
                user_id=user_id,
                access_level=AccessLevel.OWNER.value,
                created_by=user_id,
                is_owner=True
            )
            session.add(user_content)
            logger.debug("Added user content access")

            # Add entities
            for entity_data in request.entities:
                logger.debug(f"Processing entity: {entity_data}")
                # Validate entity type
                try:
                    entity_type = EntityType(entity_data["type"])
                    logger.debug(f"Validated entity type: {entity_type}")
                except ValueError as e:
                    logger.error(f"Invalid entity type: {entity_data['type']}")
                    raise HTTPException(
                        status_code=400, 
                        detail=f"'{entity_data['type']}' is not a valid EntityType. Valid types are: {[e.value for e in EntityType]}"
                    )
                
                # Check if entity already exists
                entity_query = select(Entity).where(
                    and_(
                        Entity.name == entity_data["name"],
                        Entity.type == entity_type.value
                    )
                )
                result = await session.execute(entity_query)
                entity = result.scalar_one_or_none()
                
                if not entity:
                    logger.debug(f"Creating new entity: {entity_data['name']} ({entity_type})")
                    # Create new entity if it doesn't exist
                    entity = Entity(
                        name=entity_data["name"],
                        type=entity_type.value
                    )
                    session.add(entity)
                    await session.flush()
                    logger.debug(f"Created new entity with id={entity.id}")
                else:
                    logger.debug(f"Found existing entity with id={entity.id}")
                
                logger.debug(f"Associating entity {entity.id} with content {content.id}")
                # Associate entity with content using PostgreSQL-specific insert
                try:
                    await session.execute(
                        pg_insert(content_entity_association)
                        .values(
                            content_id=content.id,
                            entity_id=entity.id
                        )
                        .on_conflict_do_nothing()
                    )
                    logger.debug("Successfully associated entity with content")
                except Exception as e:
                    logger.error(f"Error associating entity with content: {str(e)}", exc_info=True)
                    raise
            
            try:
                await session.commit()
                logger.debug("Successfully committed all changes")
            except Exception as e:
                logger.error(f"Error during commit: {str(e)}", exc_info=True)
                raise
            
            return {
                "content_id": str(content.id),
                "timestamp": content.timestamp,
                "type": content.type,
                "parent_id": str(content.parent_id) if content.parent_id else None
            }
            
    except ValueError as e:
        logger.error(f"Validation error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Error adding content: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

class ModifyContentRequest(BaseModel):
    content_id: UUID
    body: str = Field(..., description="Content text body")
    entities: List[Dict[str, str]] = Field(default_factory=list, description="List of entities with type and name")

@app.post("/api/content/modify", response_model=Dict[str, Any])
async def modify_content(
    request: ModifyContentRequest,
    current_user: tuple = Depends(get_current_user)
) -> Dict[str, Any]:
    """Modify existing content text and entities"""
    user_id, user_name, token = current_user
    
    try:
        async with get_session() as session:
            # Check content exists and user has access
            content_query = (
                select(Content)
                .join(UserContent)
                .where(
                    and_(
                        Content.id == request.content_id,
                        UserContent.user_id == user_id,
                        UserContent.access_level == AccessLevel.OWNER.value
                    )
                )
            )
            result = await session.execute(content_query)
            content = result.scalar_one_or_none()
            
            if not content:
                raise HTTPException(status_code=404, detail="Content not found or no access")
            
            # Update content text
            content.text = request.body
            content.last_update = datetime.now(timezone.utc).replace(tzinfo=None)
            
            # Remove existing entity associations
            await session.execute(
                content_entity_association.delete().where(
                    content_entity_association.c.content_id == content.id
                )
            )
            
            # Add new entities
            for entity_data in request.entities:
                try:
                    entity_type = EntityType(entity_data["type"])
                except ValueError:
                    raise HTTPException(
                        status_code=400,
                        detail=f"'{entity_data['type']}' is not a valid EntityType. Valid types are: {[e.value for e in EntityType]}"
                    )
                
                # Check if entity exists
                entity_query = select(Entity).where(
                    and_(
                        Entity.name == entity_data["name"],
                        Entity.type == entity_type.value
                    )
                )
                result = await session.execute(entity_query)
                entity = result.scalar_one_or_none()
                
                if not entity:
                    entity = Entity(
                        name=entity_data["name"],
                        type=entity_type.value
                    )
                    session.add(entity)
                    await session.flush()
                
                # Associate entity with content
                await session.execute(
                    pg_insert(content_entity_association)
                    .values(
                        content_id=content.id,
                        entity_id=entity.id
                    )
                    .on_conflict_do_nothing()
                )
            
            await session.commit()
            
            return {
                "content_id": str(content.id),
                "timestamp": content.timestamp,
                "type": content.type,
                "parent_id": str(content.parent_id) if content.parent_id else None
            }
            
    except Exception as e:
        logger.error(f"Error modifying content: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

class ContentIdRequest(BaseModel):
    content_id: UUID

@app.post("/api/content/archive")
async def archive_content(
    request: ContentIdRequest,
    current_user: tuple = Depends(get_current_user)
) -> Dict[str, bool]:
    """Archive content for a user"""
    user_id, user_name, token = current_user
    
    try:
        async with get_session() as session:
            # Check content exists and user has access
            content_query = (
                select(Content)
                .join(UserContent)
                .where(
                    and_(
                        Content.id == request.content_id,
                        UserContent.user_id == user_id,
                        UserContent.access_level == AccessLevel.OWNER.value
                    )
                )
            )
            result = await session.execute(content_query)
            content = result.scalar_one_or_none()
            
            if not content:
                raise HTTPException(status_code=404, detail="Content not found or no access")
            
            # Update user content access level to REMOVED
            await session.execute(
                update(UserContent)
                .where(
                    and_(
                        UserContent.content_id == request.content_id,
                        UserContent.user_id == user_id
                    )
                )
                .values(access_level=AccessLevel.REMOVED.value)
            )
            
            await session.commit()
            return {"success": True}
            
    except Exception as e:
        logger.error(f"Error archiving content: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/content/restore")
async def restore_content(
    request: ContentIdRequest,
    current_user: tuple = Depends(get_current_user)
) -> Dict[str, bool]:
    """Restore archived content for a user"""
    user_id, user_name, token = current_user
    
    try:
        async with get_session() as session:
            # Check content exists and user has access
            content_query = (
                select(Content)
                .join(UserContent)
                .where(
                    and_(
                        Content.id == request.content_id,
                        UserContent.user_id == user_id,
                        UserContent.access_level == AccessLevel.REMOVED.value
                    )
                )
            )
            result = await session.execute(content_query)
            content = result.scalar_one_or_none()
            
            if not content:
                raise HTTPException(status_code=404, detail="Content not found or not archived")
            
            # Update user content access level back to OWNER
            await session.execute(
                update(UserContent)
                .where(
                    and_(
                        UserContent.content_id == request.content_id,
                        UserContent.user_id == user_id
                    )
                )
                .values(access_level=AccessLevel.OWNER.value)
            )
            
            await session.commit()
            return {"success": True}
            
    except Exception as e:
        logger.error(f"Error restoring content: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/content/{content_id}")
async def get_content(
    content_id: UUID,
    current_user: tuple = Depends(get_current_user),
    authorization: str = Header(...)
) -> Dict[str, Any]:
    """Get content by ID including children and entities"""
    user_id, user_name, token = current_user
    token = authorization.split("Bearer ")[-1]
    
    try:
        async with get_session() as session:
            # Get content with entities
            query = (
                select(
                    Content,
                    UserContent.access_level,
                    UserContent.is_owner,
                    func.array_agg(distinct(Entity.name)).label('entities'),
                    func.array_agg(distinct(Entity.type)).label('entity_types')
                )
                .join(UserContent, Content.id == UserContent.content_id)
                .outerjoin(content_entity_association, Content.id == content_entity_association.c.content_id)
                .outerjoin(Entity, content_entity_association.c.entity_id == Entity.id)
                .where(
                    and_(
                        Content.id == content_id,
                        UserContent.user_id == user_id,
                        UserContent.access_level != AccessLevel.REMOVED.value
                    )
                )
                .group_by(Content.id, UserContent.access_level, UserContent.is_owner)
            )
            
            result = await session.execute(query)
            row = result.first()
            if not row:
                raise HTTPException(status_code=404, detail="Content not found or no access")
                
            content, access_level, is_owner, entity_names, entity_types = row
            
            # If meeting type, refresh transcript from Vexa
            text = content.text
            if content.type == ContentType.MEETING.value:
                try:
                    vexa_api = VexaAPI(token=token)
                    transcription = await vexa_api.get_transcription(meeting_session_id=str(content_id))
                    
                    if transcription:
                        df, _, start_datetime, speakers, transcript = transcription
                        text = str(transcript)
                        
                        # Update database with latest transcript
                        content.text = text
                        content.timestamp = start_datetime.astimezone(timezone.utc).replace(tzinfo=None)
                        await session.commit()
                        
                except Exception as e:
                    logger.error(f"Failed to fetch transcript from Vexa: {str(e)}")
            
            # Get children
            children_query = (
                select(
                    Content.id,
                    Content.type,
                    Content.timestamp,
                    Content.is_indexed,
                    UserContent.access_level,
                    UserContent.is_owner,
                    func.array_agg(distinct(Entity.name)).label('entities')
                )
                .join(UserContent, Content.id == UserContent.content_id)
                .outerjoin(content_entity_association, Content.id == content_entity_association.c.content_id)
                .outerjoin(Entity, content_entity_association.c.entity_id == Entity.id)
                .where(
                    and_(
                        Content.parent_id == content_id,
                        UserContent.user_id == user_id,
                        UserContent.access_level != AccessLevel.REMOVED.value
                    )
                )
                .group_by(
                    Content.id,
                    Content.type,
                    Content.timestamp,
                    Content.is_indexed,
                    UserContent.access_level,
                    UserContent.is_owner
                )
                .order_by(Content.timestamp.desc())
            )
            
            children_result = await session.execute(children_query)
            children = children_result.all()
            
            # Format entities
            entities = []
            if entity_names and entity_types:
                for name, type_ in zip(entity_names, entity_types):
                    if name and name != 'TBD':
                        entities.append({"name": name, "type": type_})
            
            return {
                "content_id": str(content.id),
                "parent_id": str(content.parent_id) if content.parent_id else None,
                "type": content.type,
                "text": text,
                "timestamp": content.timestamp,
                "is_indexed": content.is_indexed,
                "access_level": access_level,
                "is_owner": is_owner,
                "entities": entities,
                "children": [
                    {
                        "content_id": str(c.id),
                        "type": c.type,
                        "timestamp": c.timestamp,
                        "is_indexed": c.is_indexed,
                        "access_level": c.access_level,
                        "is_owner": c.is_owner,
                        "entities": [e for e in c.entities if e and e != 'TBD']
                    }
                    for c in children
                ]
            }
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting content: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/entities/{entity_type}")
async def get_entities(
    entity_type: str,
    offset: int = 0,
    limit: int = 20,
    current_user: tuple = Depends(get_current_user)
) -> Dict[str, Any]:
    """Get entities by type with pagination and last seen timestamp"""
    user_id, user_name, token = current_user
    
    try:
        # Validate entity type
        try:
            entity_type_enum = EntityType(entity_type)
        except ValueError:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid entity type. Must be one of: {[e.value for e in EntityType]}"
            )
        
        async with get_session() as session:
            # Query to get entities and their latest content timestamps
            query = (
                select(
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
                        Entity.type == entity_type_enum.value,
                        Entity.name != 'TBD',
                        Entity.name != None
                    )
                )
                .group_by(Entity.name)
                .order_by(desc('last_seen'))
                .offset(offset)
                .limit(limit)
            )
            
            result = await session.execute(query)
            entities = [
                {
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
                        Entity.type == entity_type_enum.value,
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

