from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from search import SearchAssistant
from token_manager import TokenManager
from indexing import Indexing
import asyncio
from datetime import datetime
import json
# Add these new imports
from psql_helpers import (
    async_session,
    get_first_meeting_timestamp,
    get_last_meeting_timestamp,
    create_share_link,
    accept_share_link,
)
from sqlalchemy import func, select
from vexa import VexaAPI

from psql_models import Meeting,UserMeeting
from sqlalchemy import and_
from pydantic import BaseModel, EmailStr
from uuid import UUID
from psql_models import AccessLevel
from pyinstrument import Profiler
from fastapi import FastAPI, Request
from functools import lru_cache
from fastapi_cache import FastAPICache
from fastapi_cache.decorator import cache
from fastapi_cache.backends.redis import RedisBackend
import redis
import os



app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)


search_assistant = SearchAssistant()
token_manager = TokenManager()

class Thread(BaseModel):
    thread_id: str
    thread_name: str
    timestamp: datetime

class ChatRequest(BaseModel):
    query: str
    thread_id: Optional[str] = None
    model: Optional[str] = None
    temperature: Optional[float] = None

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
    target_email: Optional[EmailStr] = None
    expiration_hours: Optional[int] = 24
    include_existing_meetings: Optional[bool] = False

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

# Setup FastAPI cache with Redis backend
@app.on_event("startup")
async def startup():
    FastAPICache.init(
        backend=RedisBackend(redis_client),
        prefix="fastapi-cache"
    )

@app.on_event("startup")
async def startup_event():
    await search_assistant.initialize()

async def get_current_user(authorization: str = Header(...)):
    token = authorization.split("Bearer ")[-1]
    try:
        user_id, user_name = await token_manager.check_token(token)
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
        return user_id, user_name
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid token")

@app.post("/submit_token")
async def submit_token(request: TokenRequest):
    user_id, user_name = await token_manager.submit_token(request.token)
    if user_id is None or user_name is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    return {"user_id": user_id, "user_name": user_name}

@app.get("/threads", response_model=List[Thread])
async def get_threads(current_user: tuple = Depends(get_current_user)):
    user_id, _ = current_user
    threads = await search_assistant.get_user_threads(user_id)
    return [Thread(**thread) for thread in threads]

@app.get("/thread/{thread_id}")
async def get_thread(thread_id: str, current_user: tuple = Depends(get_current_user)):
    user_id, _ = current_user
    thread = await search_assistant.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    if thread.user_id != user_id:
        raise HTTPException(status_code=403, detail="Access denied")
    return thread

@app.post("/chat")
async def chat(request: ChatRequest, current_user: tuple = Depends(get_current_user)):
    user_id, _ = current_user
    try:
        async def stream_response():
            async for item in search_assistant.chat(
                query=request.query,
                thread_id=request.thread_id,
                model=request.model,
                temperature=request.temperature,
                user_id=user_id
            ):
                if isinstance(item, dict) and "search_results" in item:
                    # Handle search results
                    yield f"data: {json.dumps({'type': 'search_results', 'results': item['search_results']})}\n\n"
                elif isinstance(item, str):
                    # Handle streaming text chunks
                    yield f"data: {json.dumps({'type': 'stream', 'content': item})}\n\n"
                elif isinstance(item, dict):
                    # Handle final response with thread info
                    yield f"data: {json.dumps({'type': 'final', **item})}\n\n"
            yield "data: {\"type\": \"done\"}\n\n"

        return StreamingResponse(stream_response(), media_type="text/event-stream")
    except ValueError as e:
        if "Thread with id" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise

@app.delete("/thread/{thread_id}")
async def delete_thread(thread_id: str, current_user: tuple = Depends(get_current_user)):
    user_id, _ = current_user
    thread = await search_assistant.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    if thread.user_id != user_id:
        raise HTTPException(status_code=403, detail="Access denied")
    
    success = await search_assistant.delete_thread(thread_id)
    if success:
        return {"message": "Thread deleted successfully"}
    else:
        raise HTTPException(status_code=500, detail="Failed to delete thread")

@app.get("/user/meetings/timestamps", response_model=MeetingTimestamps)
async def get_meeting_timestamps(current_user: tuple = Depends(get_current_user)):
    user_id, _ = current_user
    async with async_session() as session:
        first_meeting = await get_first_meeting_timestamp(session, user_id)
        last_meeting = await get_last_meeting_timestamp(session, user_id)
        
    return MeetingTimestamps(
        first_meeting=first_meeting,
        last_meeting=last_meeting
    )

async def run_indexing_job(token: str, num_meetings: int):
    indexer = Indexing(token=token)
    await indexer.index_meetings(num_meetings=num_meetings)

@app.post("/start_indexing")
async def start_indexing(
    request: IndexingRequest,
    authorization: str = Header(...)
):
    token = authorization.split("Bearer ")[-1]
    user_id, user_name = await token_manager.check_token(token)
    
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token")

    try:
        # Create indexer instance
        indexer = Indexing(token=token)
        
        # Check if indexing is already in progress
        status = indexer.status
        if status["is_indexing"]:
            raise HTTPException(
                status_code=409,
                detail="Indexing already in progress for this user"
            )

        # Start the indexing job as a background task using asyncio.create_task
        asyncio.create_task(
            run_indexing_job(token=token, num_meetings=request.num_meetings or 200)
        )
        return {"message": f"Indexing job started for {request.num_meetings or 200} meetings"}
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

@app.get("/meetings_processed", response_model=MeetingsProcessedResponse)
@cache(expire=300)  # Cache for 5 minutes
async def get_meetings_processed(
    authorization: str = Header(...),
    current_user: tuple = Depends(get_current_user)
):
    user_id, _ = current_user
    token = authorization.split("Bearer ")[-1]
    
    # Get total meetings from Vexa
    vexa_api = VexaAPI(token=token)
    total_meetings = await vexa_api.get_meetings(include_total=True, limit=1)
    total_meetings_count = total_meetings[1] if isinstance(total_meetings, tuple) else 0
    
    # Get processed meetings count from database
    async with async_session() as session:
        query = select(func.count()).select_from(Meeting)\
            .join(UserMeeting, Meeting.meeting_id == UserMeeting.meeting_id)\
            .where(and_(
                UserMeeting.user_id == user_id,
                UserMeeting.is_owner == True
            ))
        result = await session.execute(query)
        meetings_processed = result.scalar() or 0
        
    return MeetingsProcessedResponse(
        meetings_processed=meetings_processed,
        total_meetings=total_meetings_count
    )

@app.get("/indexing_status")
async def get_indexing_status(current_user: tuple = Depends(get_current_user)):
    user_id, _ = current_user
    indexer = Indexing(token=None)  # Token not needed for checking status
    status = indexer.status
    
    # Check if any indexing is happening for this specific user
    is_indexing = status["is_indexing"]
    current_meeting = status["current_meeting"]
    processing_meetings = status["processing_meetings"]
    
    return {
        "is_indexing": is_indexing,
        "current_meeting": current_meeting,
        "processing_meetings": processing_meetings
    }

@app.delete("/user/data")
async def remove_user_data(current_user: tuple = Depends(get_current_user)):
    user_id, user_name = current_user
    deleted_points = await search_assistant.remove_user_data(user_id)
    return {"message": f"Successfully removed {deleted_points} data points for user {user_name}"}

@app.post("/share-links", response_model=CreateShareLinkResponse)
async def create_new_share_link(
    request: CreateShareLinkRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, _ = current_user
    
    try:
        access_level = AccessLevel(request.access_level)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid access level. Must be one of: {[e.value for e in AccessLevel]}"
        )
    
    async with async_session() as session:
        token = await create_share_link(
            session=session,
            owner_id=user_id,
            access_level=access_level,
            target_email=request.target_email,
            expiration_hours=request.expiration_hours,
            include_existing_meetings=request.include_existing_meetings
        )
        
    return CreateShareLinkResponse(token=token)

@app.post("/share-links/accept")
async def accept_new_share_link(
    request: AcceptShareLinkRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, _ = current_user
    
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

# if DEV:
#     @app.middleware("http")
#     async def profiler_middleware(request: Request, call_next):

#         profiler = Profiler()
#         profiler.start()
#         response = await call_next(request)
#         profiler.stop()
#         print(profiler.output_text(unicode=True, color=True))
#         return response
#         return await call_next(request)

@app.get("/transcript/{meeting_id}")
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

@app.get("/meetings/all")
async def get_meetings(
    authorization: str = Header(...),
    current_user: tuple = Depends(get_current_user),
    offset: int = 0,
    limit: int = 200
):
    token = authorization.split("Bearer ")[-1]
    vexa_api = VexaAPI(token=token)

    try:
        meetings = await vexa_api.get_meetings(offset=offset, limit=limit)
        if meetings is None:
            return {"total": 0, "meetings": []}
        
        return {
            "total": len(meetings) if isinstance(meetings, list) else 0,
            "meetings": meetings if isinstance(meetings, list) else []
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8765, reload=True)
    
    
    # conda activate langchain && uvicorn api:app --host 0.0.0.0 --port 8766 --workers 1 --loop uvloop




