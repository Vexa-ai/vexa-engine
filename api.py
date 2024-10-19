from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
from search import SearchAssistant
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

# Initialize SearchAssistant
search_assistant = SearchAssistant()

class Thread(BaseModel):
    thread_id: str
    thread_name: str
    timestamp: datetime

class ChatRequest(BaseModel):
    query: str
    thread_id: Optional[str] = None
    model: Optional[str] = None
    temperature: Optional[float] = None

@app.get("/threads", response_model=List[Thread])
async def get_threads():
    threads = search_assistant.get_user_threads()
    return [Thread(**thread) for thread in threads]

@app.get("/thread/{thread_id}")
async def get_thread(thread_id: str):
    thread = search_assistant.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    return thread

@app.post("/chat")
async def chat(request: ChatRequest):
    try:
        async def stream_response():
            thread_id = None
            linked_output = None
            async for item in search_assistant.chat(
                query=request.query,
                thread_id=request.thread_id,
                model=request.model,
                temperature=request.temperature
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
