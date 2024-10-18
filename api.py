from fastapi import FastAPI, HTTPException, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Optional
from search import SearchAssistant
import asyncio
from datetime import datetime

app = FastAPI()

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
    async def stream_response():
        async for item in search_assistant.chat(
            query=request.query,
            thread_id=request.thread_id,
            model=request.model,
            temperature=request.temperature
        ):
            if isinstance(item, str):
                yield f"data: {item}\n\n"
            else:
                yield f"data: {item.json()}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(stream_response(), media_type="text/event-stream")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8765)
