from fastapi import FastAPI, HTTPException, Depends, Header, Path
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from search import SearchAssistant
from token_manager import TokenManager
#from indexing import Indexing
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
    get_session,
    has_meeting_access,
    get_meeting_by_id,
)
from sqlalchemy import func, select, update,insert
from vexa import VexaAPI
from psql_models import Thread as ThreadModel

from psql_models import Meeting,UserMeeting
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import and_
from pydantic import BaseModel, EmailStr
from uuid import UUID
from psql_models import AccessLevel
#from pyinstrument import Profiler
from fastapi import FastAPI, Request
from functools import lru_cache
from fastapi_cache import FastAPICache
from fastapi_cache.decorator import cache
from fastapi_cache.backends.redis import RedisBackend
import redis
import os
from sqlalchemy import desc

from psql_models import Speaker, DiscussionPoint, Meeting, UserMeeting, AccessLevel,meeting_speaker_association

import logging
from logging.handlers import RotatingFileHandler
import sys
from sqlalchemy import distinct

from logger import logger

from chat import MeetingChatManager

from vexa import VexaAuth

import pandas as pd

import httpx



from datetime import datetime, timezone
from chat import MeetingSummaryContextProvider,MeetingContextProvider

from prompts import Prompts
prompts = Prompts()

app = FastAPI()

# Move this BEFORE any other middleware or app setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://assistant.dev.vexa.ai", "http://localhost:5173", "http://localhost:5174","https://vexa.ai"],  # Must be explicit
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Other middleware and routes should come after CORS middleware

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

# Add logging configuration after the imports and before app initialization
def setup_logger():
    logger = logging.getLogger('vexa_api')
    logger.setLevel(logging.DEBUG)

    # Create console handler with formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    )
    
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
        return user_id, user_name
    except Exception as e:
        logger.error(f"Authentication failed: {str(e)}", exc_info=True)
        raise HTTPException(status_code=401, detail="Invalid token")

@app.post("/submit_token")
async def submit_token(request: TokenRequest):
    user_id, user_name, image = await token_manager.submit_token(request.token)
    if user_id is None or user_name is None:
        raise HTTPException(status_code=401, detail="Invalid token")
    return {"user_id": user_id, "user_name": user_name, "image": image}

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

@app.get("/meetings/all")
async def get_meetings(
    authorization: str = Header(None),
    offset: int = 0,
    limit: int = None,
    include_summary: bool = False,
    current_user: tuple = Depends(get_current_user)
):
    logger.info(f"Getting meetings with offset={offset}, limit={limit}, include_summary={include_summary}")
    user_id, _ = current_user
    
    try:
        async with async_session() as session:
            # Build base query with outer joins
            select_columns = [
                Meeting.meeting_id,
                Meeting.meeting_name,
                Meeting.timestamp,
                Meeting.is_indexed,
                func.array_agg(distinct(Speaker.name)).label('speakers')
            ]
            
            # Add meeting_summary to select columns if requested
            if include_summary:
                select_columns.append(Meeting.meeting_summary)
            
            query = (
                select(*select_columns)
                .join(UserMeeting, Meeting.meeting_id == UserMeeting.meeting_id)
                .outerjoin(meeting_speaker_association, Meeting.id == meeting_speaker_association.c.meeting_id)
                .outerjoin(Speaker, meeting_speaker_association.c.speaker_id == Speaker.id)
                .where(
                    and_(
                        UserMeeting.user_id == user_id,
                        UserMeeting.access_level != AccessLevel.REMOVED.value
                    )
                )
                .group_by(Meeting.meeting_id, Meeting.meeting_name, Meeting.timestamp, Meeting.is_indexed, 
                         *([] if not include_summary else [Meeting.meeting_summary]))
                .order_by(Meeting.timestamp.desc())
            )
            
            if limit is not None:
                query = query.limit(limit)
            if offset:
                query = query.offset(offset)

            result = await session.execute(query)
            meetings = result.all()
            
            # Format response
            meetings_list = [
                {
                    "meeting_id": str(meeting.meeting_id),
                    "meeting_name": meeting.meeting_name,
                    "timestamp": meeting.timestamp.astimezone(timezone.utc).replace(tzinfo=None),
                    "is_indexed": meeting.is_indexed,
                    "speakers": [s for s in meeting.speakers if s and s != 'TBD'],
                    **({"meeting_summary": meeting.meeting_summary} if include_summary else {})
                }
                for meeting in meetings
            ]
            
            # Get total count
            count_query = (
                select(func.count(distinct(Meeting.meeting_id)))
                .join(UserMeeting, Meeting.meeting_id == UserMeeting.meeting_id)
                .where(
                    and_(
                        UserMeeting.user_id == user_id,
                        UserMeeting.access_level != AccessLevel.REMOVED.value
                    )
                )
            )
            total_count = await session.execute(count_query)
            total = total_count.scalar() or 0
            
            return {
                "total": total,
                "meetings": meetings_list
            }
            
    except Exception as e:
        logger.error(f"Error getting meetings: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/meeting/{meeting_id}/details")
async def get_meeting_details(
    meeting_id: UUID = Path(..., description="The UUID of the meeting"),
    current_user: tuple = Depends(get_current_user),
    authorization: str = Header(...)
):
    user_id, _ = current_user
    token = authorization.split("Bearer ")[-1]
    
    async with get_session() as session:
        query = (
            select(Meeting, UserMeeting, func.array_agg(func.json_build_object(
                'id', DiscussionPoint.id,
                'summary', DiscussionPoint.summary,
                'details', DiscussionPoint.details,
                'referenced_text', DiscussionPoint.referenced_text,
                'speaker_id', DiscussionPoint.speaker_id,
                'topic_name', DiscussionPoint.topic_name,
                'topic_type', DiscussionPoint.topic_type
            )).label('discussion_points'))
            .join(UserMeeting, Meeting.meeting_id == UserMeeting.meeting_id)
            .outerjoin(DiscussionPoint, Meeting.meeting_id == DiscussionPoint.meeting_id)
            .where(
                and_(
                    Meeting.meeting_id == meeting_id,
                    UserMeeting.user_id == user_id,
                    UserMeeting.access_level != AccessLevel.REMOVED.value
                )
            )
            .group_by(Meeting.id, UserMeeting.id)
        )
        
        result = await session.execute(query)
        row = result.first()
        
        if not row:
            raise HTTPException(status_code=404, detail="Meeting not found or access denied")
            
        meeting, user_meeting, discussion_points = row
        
        # Convert discussion points to pandas DataFrame and deduplicate
        if discussion_points and discussion_points[0] is not None:
            import pandas as pd
            df = pd.DataFrame(discussion_points)
            df = df.drop_duplicates(subset=['topic_name']).to_dict('records')
            discussion_points = df
        else:
            discussion_points = []

        transcript_data = None
        speakers = []
        
        if not meeting.is_indexed or not meeting.transcript:
            try:
                vexa_api = VexaAPI(token=token)
                transcription = await vexa_api.get_transcription(meeting_session_id=str(meeting_id))
                
                if transcription:
                    df, _, start_datetime, speakers, transcript = transcription
                    utc_timestamp = start_datetime.astimezone(timezone.utc).replace(tzinfo=None)
                    
                    await session.execute(
                        update(Meeting)
                        .where(Meeting.meeting_id == meeting_id)
                        .values(
                            transcript=str(transcript),
                            timestamp=utc_timestamp
                        )
                    )
                    
                    for speaker_name in speakers:
                        if speaker_name and speaker_name != 'TBD':
                            speaker_query = select(Speaker).where(Speaker.name == speaker_name)
                            existing_speaker = await session.execute(speaker_query)
                            speaker = existing_speaker.scalar_one_or_none()
                            
                            if not speaker:
                                speaker = Speaker(name=speaker_name)
                                session.add(speaker)
                                await session.flush()
                            
                            await session.execute(
                                pg_insert(meeting_speaker_association).values(
                                    meeting_id=meeting.id,
                                    speaker_id=speaker.id
                                ).on_conflict_do_nothing()
                            )
                    
                    await session.commit()
                    await session.refresh(meeting)
                    transcript_data = eval(meeting.transcript)
                    speakers = list(set(segment.get('speaker') for segment in transcript_data 
                                if segment.get('speaker') and segment.get('speaker') != 'TBD'))
            except Exception as e:
                logger.error(f"Failed to fetch transcript from Vexa: {str(e)}")
        else:
            transcript_data = eval(meeting.transcript)
            speakers = list(set(segment.get('speaker') for segment in transcript_data 
                        if segment.get('speaker') and segment.get('speaker') != 'TBD'))
        
        response = {
            "meeting_id": meeting.meeting_id,
            "timestamp": meeting.timestamp,
            "meeting_name": meeting.meeting_name,
            "transcript": json.dumps(transcript_data) if transcript_data else [],
            "meeting_summary": meeting.meeting_summary,
            "speakers": speakers,
            "access_level": user_meeting.access_level,
            "is_owner": user_meeting.is_owner,
            "is_indexed": meeting.is_indexed,
            "discussion_points": discussion_points
        }
        
        return response

class SearchTranscriptsRequest(BaseModel):
    query: str
    meeting_ids: Optional[List[str]] = None
    min_score: Optional[float] = 0.80

@app.post("/search/transcripts")
async def search_transcripts(
    request: SearchTranscriptsRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, _ = current_user
    logger.info(f"Starting transcript search for user {user_id}")
    
    # Add debug points
    print("DEBUG: Request received:", request.dict())
    
    try:
        # Add timing
        import time
        start_time = time.time()
        
        results = await search_assistant.search_transcripts(
            query=request.query,
            user_id=user_id,
            meeting_ids=request.meeting_ids,
            min_score=request.min_score
        )
        
        elapsed_time = time.time() - start_time
        print(f"DEBUG: Search completed in {elapsed_time:.2f} seconds")
        
        return {"results": results}
    except Exception as e:
        print("DEBUG: Error occurred:", str(e))
        logger.error(f"Error searching transcripts: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

class GlobalSearchRequest(BaseModel):
    query: str
    limit: Optional[int] = 200
    min_score: Optional[float] = 0.4
    
from psql_helpers import get_meetings_by_ids

@app.post("/search/global")
async def global_search(
    request: GlobalSearchRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, _ = current_user
    
    try:
        # Get search results just to obtain meeting IDs
        results = await search_assistant.search(
            query=request.query,
            user_id=user_id,
            limit=200,
            min_score=request.min_score
        )
        
        if results.empty:
            return {
                "total": 0,
                "meetings": []
            }
        
        # Extract unique meeting IDs from search results
        meeting_ids = results.sort_values('score', ascending=False)['meeting_id'].unique().tolist()
        
        async with async_session() as session:
            result = await get_meetings_by_ids(
            meeting_ids=meeting_ids, 
            user_id=user_id
        )
            
            return result
            
    except Exception as e:
        logger.error(f"Error in global search: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/threads/{meeting_id}")
async def get_meeting_threads(meeting_id: UUID, current_user: tuple = Depends(get_current_user)):
    user_id, _ = current_user
    async with get_session() as session:
        if not await has_meeting_access(session, user_id, meeting_id):
            raise HTTPException(status_code=403, detail="No access to meeting")
        result = await session.execute(
            select(ThreadModel)
            .where(and_(ThreadModel.user_id == user_id, ThreadModel.meeting_id == meeting_id))
            .order_by(ThreadModel.timestamp.desc())
        )
        return [{
            "thread_id": t.thread_id,
            "thread_name": t.thread_name,
            "timestamp": t.timestamp
        } for t in result.scalars().all()]

class MeetingChatRequest(BaseModel):
    query: str
    meeting_ids: List[UUID]
    thread_id: Optional[str] = None
    model: Optional[str] = None
    temperature: Optional[float] = None


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
    user_id, _ = current_user
    
    try:
        async with async_session() as session:
            # Query to get speakers and their latest meeting timestamps
            query = (
                select(
                    Speaker.name,
                    func.max(Meeting.timestamp).label('last_seen'),
                    func.count(distinct(Meeting.meeting_id)).label('meeting_count')
                )
                .join(meeting_speaker_association, Speaker.id == meeting_speaker_association.c.speaker_id)
                .join(Meeting, Meeting.id == meeting_speaker_association.c.meeting_id)
                .join(UserMeeting, Meeting.meeting_id == UserMeeting.meeting_id)
                .where(
                    and_(
                        UserMeeting.user_id == user_id,
                        UserMeeting.access_level != AccessLevel.REMOVED.value,
                        Speaker.name != 'TBD',
                        Speaker.name != None
                    )
                )
                .group_by(Speaker.name)
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
    user_id, _ = current_user
    logger.info(f"Getting meetings for speakers: {request.speakers}")
    
    try:
        async with async_session() as session:
            # Subquery to get all meeting IDs where any of the requested speakers participated
            meeting_ids_subquery = (
                select(Meeting.id)
                .join(meeting_speaker_association, Meeting.id == meeting_speaker_association.c.meeting_id)
                .join(Speaker, meeting_speaker_association.c.speaker_id == Speaker.id)
                .where(Speaker.name.in_(request.speakers))
                .distinct()
                .scalar_subquery()
            )

            # Main query using the subquery
            query = (
                select(
                    Meeting.meeting_id,
                    Meeting.meeting_name,
                    Meeting.timestamp,
                    Meeting.is_indexed,
                    Meeting.meeting_summary,
                    UserMeeting.access_level,
                    UserMeeting.is_owner,
                    func.array_agg(distinct(Speaker.name)).label('speakers')
                )
                .join(UserMeeting, Meeting.meeting_id == UserMeeting.meeting_id)
                .join(meeting_speaker_association, Meeting.id == meeting_speaker_association.c.meeting_id)
                .join(Speaker, meeting_speaker_association.c.speaker_id == Speaker.id)
                .where(
                    and_(
                        UserMeeting.user_id == user_id,
                        UserMeeting.access_level != AccessLevel.REMOVED.value,
                        Meeting.id.in_(meeting_ids_subquery)
                    )
                )
                .group_by(
                    Meeting.meeting_id,
                    Meeting.meeting_name,
                    Meeting.timestamp,
                    Meeting.is_indexed,
                    Meeting.meeting_summary,
                    UserMeeting.access_level,
                    UserMeeting.is_owner
                )
                .order_by(Meeting.timestamp.desc())
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
                    "meeting_name": meeting.meeting_name,
                    "timestamp": meeting.timestamp.astimezone(timezone.utc).replace(tzinfo=None),
                    "is_indexed": meeting.is_indexed,
                    "meeting_summary": meeting.meeting_summary,
                    "access_level": meeting.access_level,
                    "is_owner": meeting.is_owner,
                    "speakers": [s for s in meeting.speakers if s and s != 'TBD']
                }
                for meeting in meetings
            ]
            
            # Count query using the same subquery logic
            count_query = (
                select(func.count(distinct(Meeting.meeting_id)))
                .join(UserMeeting, Meeting.meeting_id == UserMeeting.meeting_id)
                .where(
                    and_(
                        UserMeeting.user_id == user_id,
                        UserMeeting.access_level != AccessLevel.REMOVED.value,
                        Meeting.id.in_(meeting_ids_subquery)
                    )
                )
            )
            
            total_count = await session.execute(count_query)
            total = total_count.scalar() or 0
            
            return {
                "total": total,
                "meetings": meetings_list
            }
            
    except Exception as e:
        logger.error(f"Error getting meetings by speakers: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

class MeetingSummaryChatRequest(BaseModel):
    query: str
    meeting_ids: List[UUID]
    include_discussion_points: Optional[bool] = True
    thread_id: Optional[str] = None
    model: Optional[str] = None
    temperature: Optional[float] = 0.7

@app.post("/chat/meeting/summary")
async def meeting_summary_chat(
    request: MeetingSummaryChatRequest, 
    current_user: tuple = Depends(get_current_user)
):
    user_id, _ = current_user
    try:
        # If more than 10 meetings, use SearchChatManager
        if len(request.meeting_ids) > 20:
            async def stream_response():
                async for item in search_assistant.chat(
                    query=request.query,
                    thread_id=request.thread_id,
                    model=request.model,
                    temperature=request.temperature,
                    user_id=user_id
                ):
                    if isinstance(item, dict):
                        yield f"data: {json.dumps(item)}\n\n"
                    else:
                        yield f"data: {json.dumps({'type': 'stream', 'content': item})}\n\n"
                yield "data: {\"type\": \"done\"}\n\n"
        else:
            # Original logic for ≤ 10 meetings
            async with get_session() as session:
                summary_provider = MeetingSummaryContextProvider(
                    meeting_ids=request.meeting_ids,
                    include_discussion_points=request.include_discussion_points
                )
                chat_manager = MeetingChatManager(session, context_provider=summary_provider)
                
                async def stream_response():
                    async for item in chat_manager.chat(
                        user_id=user_id,
                        query=request.query,
                        meeting_ids=request.meeting_ids,
                        thread_id=request.thread_id,
                        model=request.model,
                        temperature=request.temperature,
                        prompt=prompts.multiple_meetings_2711
                    ):
                        if isinstance(item, dict):
                            yield f"data: {json.dumps(item)}\n\n"
                        else:
                            yield f"data: {json.dumps({'type': 'stream', 'content': item})}\n\n"
                    yield "data: {\"type\": \"done\"}\n\n"

        return StreamingResponse(stream_response(), media_type="text/event-stream")
    except ValueError as e:
        if "Thread with id" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise
    

@app.post("/chat/meeting")
async def meeting_chat(request: MeetingChatRequest, current_user: tuple = Depends(get_current_user)):
    user_id, _ = current_user
    try:
        async with get_session() as session:
            context_provider = MeetingContextProvider(request.meeting_ids)
            meeting_chat_manager = MeetingChatManager(session, context_provider=context_provider)
            
            async def stream_response():
                async for item in meeting_chat_manager.chat(
                    user_id=user_id,
                    query=request.query,
                    meeting_ids=request.meeting_ids,
                    thread_id=request.thread_id,
                    model=request.model,
                    temperature=request.temperature,
                    prompt=prompts.single_meeting_2711
                ):
                    if isinstance(item, dict):
                        yield f"data: {json.dumps(item)}\n\n"
                    else:
                        yield f"data: {json.dumps({'type': 'stream', 'content': item})}\n\n"
                yield "data: {\"type\": \"done\"}\n\n"

            return StreamingResponse(stream_response(), media_type="text/event-stream")
    except ValueError as e:
        if "Thread with id" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise

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
    user_id, _ = current_user
    meeting_id_str = str(meeting_id)
    
    try:
        async with get_session() as session:
            if not await has_meeting_access(session, user_id, meeting_id):
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8765, reload=True)
    
    
    # conda activate langchain && uvicorn api:app --host 0.0.0.0 --port 8765 --workers 1 --loop uvloop --reload




