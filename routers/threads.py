from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional, Dict, Any
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, Field

from thread_manager import ThreadManager
from psql_helpers import get_session
from routers.common import get_current_user

router = APIRouter(prefix="/threads", tags=["threads"])
thread_manager = ThreadManager()

# Request Models
class CreateThreadRequest(BaseModel):
    thread_name: str
    content_id: Optional[UUID] = None
    entity_id: Optional[int] = None
    meta: Optional[Dict[str, Any]] = None

class AddMessageRequest(BaseModel):
    message: str
    role: str = "user"
    meta: Optional[Dict[str, Any]] = None

class RenameThreadRequest(BaseModel):
    thread_name: str

class ThreadEntitiesRequest(BaseModel):
    entity_names: List[str]

class EditMessageRequest(BaseModel):
    message_id: str
    message: str
    role: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None

# Routes
@router.get("")
async def get_threads(
    content_id: Optional[UUID] = Query(None),
    entity_id: Optional[int] = Query(None),
    only_archived: bool = Query(False),
    limit: int = Query(50),
    offset: int = Query(0),
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    return await thread_manager.get_user_threads(
        user_id=user_id,
        content_id=content_id,
        entity_id=entity_id,
        only_archived=only_archived,
        limit=limit,
        offset=offset,
        start_date=start_date,
        end_date=end_date
    )

@router.get("/{thread_id}")
async def get_thread(thread_id: str, current_user: tuple = Depends(get_current_user)):
    user_id, user_name, token = current_user
    thread = await thread_manager.get_thread(thread_id)
    if not thread or str(thread.user_id) != user_id:
        raise HTTPException(status_code=404, detail="Thread not found")
    return thread

@router.post("/{thread_id}/archive")
async def archive_thread(thread_id: str, current_user: tuple = Depends(get_current_user)):
    user_id, user_name, token = current_user
    success = await thread_manager.archive_thread(thread_id)
    if not success:
        raise HTTPException(status_code=404, detail="Thread not found")
    return {"success": True}

@router.post("/{thread_id}/restore")
async def restore_thread(thread_id: str, current_user: tuple = Depends(get_current_user)):
    user_id, user_name, token = current_user
    success = await thread_manager.unarchive_thread(thread_id)
    if not success:
        raise HTTPException(status_code=404, detail="Thread not found")
    return {"success": True}

@router.put("/{thread_id}/rename")
async def rename_thread(
    thread_id: str, 
    request: RenameThreadRequest, 
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    success = await thread_manager.rename_thread(thread_id, request.thread_name)
    if not success:
        raise HTTPException(status_code=404, detail="Thread not found")
    return {"success": True}

@router.put("/{thread_id}/messages/{message_id}")
async def edit_message(
    thread_id: str,
    message_id: str,
    request: EditMessageRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    success = await thread_manager.edit_message(
        thread_id=thread_id,
        message_id=message_id,
        message=request.message,
        role=request.role,
        meta=request.meta
    )
    if not success:
        raise HTTPException(status_code=404, detail="Thread or message not found")
    return {"success": True}

@router.post("/threads/by-entities")
async def get_threads_by_entities(
    request: ThreadEntitiesRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    return await thread_manager.get_threads_by_exact_entities(
        user_id=user_id,
        entity_names=request.entity_names
    )

@router.post("")
async def create_thread(request: CreateThreadRequest, current_user: tuple = Depends(get_current_user)):
    user_id, user_name, token = current_user
    thread = await thread_manager.create_thread(
        user_id=user_id,
        thread_name=request.thread_name,
        content_id=request.content_id,
        entity_id=request.entity_id,
        meta=request.meta
    )
    if not thread:
        raise HTTPException(status_code=500, detail="Failed to create thread")
    return thread

@router.post("/{thread_id}/message")
async def add_message(
    thread_id: str,
    request: AddMessageRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    success = await thread_manager.add_message(
        thread_id=thread_id,
        message=request.message,
        role=request.role,
        meta=request.meta
    )
    if not success:
        raise HTTPException(status_code=404, detail="Thread not found")
    return {"success": True}

@router.get("/{thread_id}/messages")
async def get_messages(thread_id: str, current_user: tuple = Depends(get_current_user)):
    user_id, user_name, token = current_user
    messages = await thread_manager.get_messages(thread_id)
    if messages is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    return {"messages": messages} 