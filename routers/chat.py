from fastapi import APIRouter, HTTPException, Depends, Body, Request
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from uuid import UUID
import json
from chat_manager import ChatManager
from fastapi.responses import StreamingResponse
from psql_helpers import get_session
import os
from qdrant_search import QdrantSearchEngine
from bm25_search import ElasticsearchBM25
import logging
from routers.common import get_current_user

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/chat", tags=["chat"]) 

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

# Initialize search engines
qdrant_engine = QdrantSearchEngine(os.getenv('VOYAGE_API_KEY'))
es_engine = ElasticsearchBM25()

@router.post("/chat")
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
    
    # Validate input combinations
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
    
    # Initialize chat manager
    chat_manager = await ChatManager.create(
        session=get_session(),
        qdrant_engine=qdrant_engine,
        es_engine=es_engine
    )
    
    # Create response stream
    async def generate():
        try:
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
        except ValueError as e:
            error_data = {"error": str(e)}
            yield f"data: {json.dumps(error_data)}\n\n"
    
    return StreamingResponse(
        generate(),
        media_type="text/event-stream"
    )

@router.post("/edit")
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
        session=get_session(),
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