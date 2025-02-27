from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel, Field, UUID4
from typing import Optional, List, Dict, Any
import json
from chat_manager import ChatManager
from fastapi.responses import StreamingResponse
from psql_helpers import get_session
import os
from qdrant_search import QdrantSearchEngine
from bm25_search import ElasticsearchBM25
import logging
from routers.common import get_current_user
from contextlib import asynccontextmanager
from fastapi import Body


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/chat", tags=["chat"])

class ChatRequest(BaseModel):
    query: str = Field(..., description="The chat query")
    content_id: Optional[UUID4] = Field(None, description="Single content ID to map thread to")
    entity_id: Optional[int] = Field(None, description="Single entity ID to map thread to")
    content_ids: Optional[List[UUID4]] = Field(None, description="Content IDs to search over")
    entity_ids: Optional[List[int]] = Field(None, description="Entity IDs to search over")
    thread_id: Optional[str] = Field(None, description="Thread ID for continuing conversation")
    model: Optional[str] = Field(None, description="Model to use for chat")
    temperature: Optional[float] = Field(None, description="Temperature for model sampling")
    meta: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")

# Initialize search engine variables
qdrant_engine = None
es_engine = None

@asynccontextmanager
async def lifespan(app):
    global qdrant_engine, es_engine
    # Initialize search engines on startup
    qdrant_engine = QdrantSearchEngine(os.getenv('VOYAGE_API_KEY'))
    es_engine = await ElasticsearchBM25.create()
    yield
    # Cleanup on shutdown
    if es_engine.es_client:
        await es_engine.es_client.close()

@router.post("/chat", response_model=None)
async def chat(
    request: Request,
    chat_request: ChatRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    
    # Validate input combinations
    if chat_request.content_id and chat_request.entity_id:
        raise HTTPException(
            status_code=400,
            detail="Cannot specify both content_id and entity_id for thread mapping"
        )
        
    if chat_request.content_ids and chat_request.entity_ids:
        raise HTTPException(
            status_code=400,
            detail="Cannot specify both content_ids and entity_ids for search scope"
        )
        
    if (chat_request.content_id and chat_request.content_ids) or (chat_request.entity_id and chat_request.entity_ids):
        raise HTTPException(
            status_code=400,
            detail="Cannot specify both single and multiple values of the same type"
        )
    
    # Initialize chat manager
    chat_manager = await ChatManager.create(
        qdrant_engine=qdrant_engine,
        es_engine=es_engine
    )
    
    # Create response stream
    async def generate():
        try:
            async for chunk in chat_manager.chat(
                user_id=user_id,
                query=chat_request.query,
                content_id=chat_request.content_id,
                entity_id=chat_request.entity_id,
                content_ids=chat_request.content_ids,
                entity_ids=chat_request.entity_ids,
                thread_id=chat_request.thread_id,
                model=chat_request.model,
                temperature=chat_request.temperature,
                meta=chat_request.meta
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
                        'content_id': str(chat_request.content_id) if chat_request.content_id else None,
                        'entity_id': chat_request.entity_id,
                        'content_ids': [str(cid) for cid in chat_request.content_ids] if chat_request.content_ids else None,
                        'entity_ids': chat_request.entity_ids,
                        'output': chunk['output'],
                        'linked_output': chunk.get('linked_output', chunk['output']),
                        'service_content': chunk.get('service_content', {})
                    }
                    yield f"data: {json.dumps(response_data)}\n\n"
        except ValueError as e:
            error_data = {"error": str(e)}
            yield f"data: {json.dumps(error_data)}\n\n"
                
    return StreamingResponse(
        generate(),
        media_type="text/event-stream"
    )

@router.post("/edit", response_model=None)
async def edit_chat_message(
    thread_id: str = Body(...),
    message_index: int = Body(...),
    new_content: str = Body(...),
    model: Optional[str] = Body(None),
    temperature: Optional[float] = Body(None),
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    
    chat_manager = await ChatManager.create(
        qdrant_engine=qdrant_engine,
        es_engine=es_engine
    )
    
    async def generate():
        try:
            async for chunk in chat_manager.edit_and_continue(
                user_id=user_id,
                thread_id=thread_id,
                message_index=message_index,
                new_content=new_content,
                model=model,
                temperature=temperature
            ):
                if "error" in chunk:
                    yield f"data: {json.dumps(chunk)}\n\n"
                    return
                elif "chunk" in chunk:
                    yield f"data: {json.dumps({'chunk': chunk['chunk']})}\n\n"
                else:
                    yield f"data: {json.dumps(chunk)}\n\n"
        except ValueError as e:
            error_data = {"error": str(e)}
            yield f"data: {json.dumps(error_data)}\n\n"
                
    return StreamingResponse(
        generate(),
        media_type="text/event-stream"
    )