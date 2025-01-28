from fastapi import FastAPI, HTTPException, Depends, Query, APIRouter
from pydantic import BaseModel
from typing import Optional, List, Dict
from uuid import UUID
from enum import Enum
from psql_models import ContentType, AccessLevel
import json
from content_manager import ContentManager
from routers.common import get_current_user




from indexing.redis_keys import RedisKeys
from redis import Redis
from datetime import datetime


router = APIRouter(prefix="/contents", tags=["contents"])

class MeetingOwnership(str, Enum):
    MY = "my"
    SHARED = "shared"
    ALL = "all"
    
    
class ContentFilter(BaseModel):
    type: str
    values: List[str]

class AddContentRequest(BaseModel):
    type: str
    text: str
    parent_id: Optional[UUID] = None
    entities: Optional[List[Dict[str, str]]] = None

class ModifyContentRequest(BaseModel):
    text: str
    entities: Optional[List[Dict[str, str]]] = None

class CreateShareLinkRequest(BaseModel):
    access_level: str
    meeting_ids: List[UUID]
    target_email: Optional[str] = None
    expiration_hours: Optional[int] = None

class CreateShareLinkResponse(BaseModel):
    token: str

class AcceptShareLinkRequest(BaseModel):
    token: str
    accepting_email: str

@router.get("/all")
async def get_contents(
    content_type: Optional[str] = None,
    filters: Optional[str] = None,
    offset: int = Query(0),
    limit: int = Query(20),
    ownership: str = Query(MeetingOwnership.ALL),
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    filter_list = []
    if filters:
        try:
            filter_list = [ContentFilter(**f).dict() for f in json.loads(filters)]
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Invalid filters format: {str(e)}")
    
    manager = await ContentManager.create()
    return await manager.get_contents(
        user_id=user_id,
        content_type=content_type,
        filters=filter_list,
        offset=offset,
        limit=limit,
        ownership=ownership
    )

@router.get("/{content_id}")
async def get_content(
    content_id: UUID,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    manager = await ContentManager.create()
    content = await manager.get_content(
        user_id=user_id,
        content_id=content_id,
        token=token
    )
    if not content:
        raise HTTPException(status_code=404, detail="Content not found or no access")
    return content

@router.post("")
async def add_content(
    request: AddContentRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    try:
        content_type = ContentType(request.type)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid content type. Must be one of: {[e.value for e in ContentType]}"
        )
    
    manager = await ContentManager.create()
    content_id = await manager.add_content(
        user_id=user_id,
        type=content_type.value,
        text=request.text,
        parent_id=request.parent_id,
        entities=request.entities
    )
    return {"content_id": content_id}

@router.put("/{content_id}")
async def modify_content(
    content_id: UUID,
    request: ModifyContentRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    manager = await ContentManager.create()
    success = await manager.modify_content(
        user_id=user_id,
        content_id=content_id,
        text=request.text,
        entities=request.entities
    )
    if not success:
        raise HTTPException(status_code=404, detail="Content not found or no access")
    return {"success": True}

@router.delete("/{content_id}")
async def delete_content(
    content_id: UUID,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    manager = await ContentManager.create()
    success = await manager.delete_content(
        user_id=user_id,
        content_id=content_id
    )
    if not success:
        raise HTTPException(status_code=404, detail="Content not found or no access")
    return {"success": True}

@router.post("/{content_id}/archive")
async def archive_content(
    content_id: UUID,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    manager = await ContentManager.create()
    success = await manager.archive_content(
        user_id=user_id,
        content_id=content_id
    )
    if not success:
        raise HTTPException(status_code=404, detail="Content not found or no access")
    return {"success": True}

@router.post("/{content_id}/restore")
async def restore_content(
    content_id: UUID,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    manager = await ContentManager.create()
    success = await manager.restore_content(
        user_id=user_id,
        content_id=content_id
    )
    if not success:
        raise HTTPException(status_code=404, detail="Content not found or not archived")
    return {"success": True}

@router.post("/share-links", response_model=CreateShareLinkResponse)
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
        manager = await ContentManager.create()
        token = await manager.create_share_link(
            owner_id=user_id,
            access_level=access_level,
            meeting_ids=request.meeting_ids,
            target_email=request.target_email,
            expiration_hours=request.expiration_hours
        )
        return CreateShareLinkResponse(token=token)
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))

@router.post("/share-links/accept")
async def accept_new_share_link(
    request: AcceptShareLinkRequest,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    
    manager = await ContentManager.create()
    success = await manager.accept_share_link(
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

@router.post("/{content_id}/index")
async def index_content(
    content_id: UUID,
    current_user: tuple = Depends(get_current_user)
):
    user_id, user_name, token = current_user
    
    try:
        manager = await ContentManager.create()
        result = await manager.queue_content_indexing(
            user_id=user_id,
            content_id=content_id
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        logger.error(f"Error queuing content for indexing: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


