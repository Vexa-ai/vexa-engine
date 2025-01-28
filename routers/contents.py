from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import Optional, List, Dict
from uuid import UUID
from enum import Enum
from psql_models import ContentType
import json
from content_manager import ContentManager
from routers.common import get_current_user

router = FastAPI(prefix="/contents", tags=["contents"])

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

