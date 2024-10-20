from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from search import SearchAssistant
from token_manager import TokenManager
import asyncio
from datetime import datetime
import json

app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Initialize SearchAssistant and TokenManager
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

async def get_current_user(authorization: str = Header(...)):
    token = authorization.split("Bearer ")[-1]
    try:
        user_id, user_name = token_manager.check_token(token)
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
        return user_id, user_name
    except Exception as e:
        raise HTTPException(status_code=401, detail="Invalid token")

@app.post("/submit_token")
async def submit_token(request: TokenRequest):
    user_id, user_name = token_manager.submit_token(request.token)
    return {"user_id": user_id, "user_name": user_name}

@app.get("/threads", response_model=List[Thread])
async def get_threads(current_user: str = Depends(get_current_user)):
    user_id, user_name = current_user
    threads = search_assistant.get_user_threads(user_id)
    return [Thread(**thread) for thread in threads]

@app.get("/thread/{thread_id}")
async def get_thread(thread_id: str, current_user: tuple = Depends(get_current_user)):
    user_id, user_name = current_user
    thread = search_assistant.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    if thread.user_id != user_id:
        raise HTTPException(status_code=403, detail="Access denied")
    return thread

@app.post("/chat")
async def chat(request: ChatRequest, current_user: str = Depends(get_current_user)):
    user_id, user_name = current_user
    try:
        async def stream_response():
            thread_id = None
            linked_output = None
            async for item in search_assistant.chat(
                query=request.query,
                thread_id=request.thread_id,
                model=request.model,
                temperature=request.temperature,
                user_id=user_id
            ):
                if isinstance(item, str):
                    # Stream string responses
                    yield f"data: {json.dumps({'type': 'stream', 'content': item})}\n\n"
                elif isinstance(item, dict):
                    # Store thread_id and linked_output from the final response
                    thread_id = item.get('thread_id')
                    linked_output = item.get('linked_output')
                    # Don't yield the dictionary immediately
            
            # Send final response with thread_id and linked_output
            final_response = {
                'type': 'final',
                'thread_id': thread_id,
                'linked_output': linked_output
            }
            yield f"data: {json.dumps(final_response)}\n\n"
            yield "data: {\"type\": \"done\"}\n\n"

        return StreamingResponse(stream_response(), media_type="text/event-stream")
    except ValueError as e:
        if "Thread with id" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("api:app", host="0.0.0.0", port=8765, reload=True)
    
    
    #uvicorn api:app --host 0.0.0.0 --port 8765 --reload
