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
)
from sqlalchemy import func, select
from vexa import VexaAPI

from psql_models import Meeting,UserMeeting
from sqlalchemy import and_

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
async def get_meetings_processed(current_user: tuple = Depends(get_current_user), authorization: str = Header(...)):
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8765, reload=True)
    
    
    # conda activate langchain && uvicorn api:app --host 0.0.0.0 --port 8765 --reload




