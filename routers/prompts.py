from fastapi import APIRouter,HTTPException,Depends
from pydantic import BaseModel
from typing import Optional,List
from uuid import UUID
from prompts_manager import PromptsManager
from routers.common import get_current_user
from psql_models import PromptTypeEnum

router=APIRouter(prefix="/prompts",tags=["prompts"])

class CreatePromptRequest(BaseModel):
    prompt:str
    type:str
    alias:Optional[str]=None

class UpdatePromptRequest(BaseModel):
    prompt:Optional[str]=None
    type:Optional[str]=None
    alias:Optional[str]=None

class PromptResponse(BaseModel):
    id:int
    prompt:str
    type:str
    alias:Optional[str]
    user_id:Optional[UUID]

@router.post("",response_model=dict)
async def create_prompt(request:CreatePromptRequest,current_user:tuple=Depends(get_current_user)):
    user_id,_,_=current_user
    try:
        manager=await PromptsManager.create()
        prompt_id=await manager.create_prompt(request.prompt,request.type,user_id,request.alias)
        return {"prompt_id":prompt_id}
    except ValueError as e:
        raise HTTPException(status_code=400,detail=str(e))

@router.get("",response_model=List[PromptResponse])
async def get_prompts(prompt_type:Optional[str]=None,current_user:tuple=Depends(get_current_user)):
    user_id,_,_=current_user
    try:
        if prompt_type:
            PromptTypeEnum(prompt_type.lower())
        manager=await PromptsManager.create()
        prompts=await manager.get_prompts(user_id,PromptTypeEnum(prompt_type.lower()) if prompt_type else None)
        return [PromptResponse(
            id=p.id,
            prompt=p.prompt,
            type=p.type,
            alias=p.alias,
            user_id=p.user_id
        ) for p in prompts]
    except ValueError as e:
        raise HTTPException(status_code=400,detail=str(e))

@router.put("/{prompt_id}",response_model=dict)
async def update_prompt(prompt_id:int,request:UpdatePromptRequest,current_user:tuple=Depends(get_current_user)):
    try:
        manager=await PromptsManager.create()
        success=await manager.update_prompt(prompt_id,request.prompt,request.type,request.alias)
        if not success: raise HTTPException(status_code=404,detail="Prompt not found")
        return {"success":True}
    except ValueError as e:
        raise HTTPException(status_code=400,detail=str(e))

@router.delete("/{prompt_id}",response_model=dict)
async def delete_prompt(prompt_id:int,current_user:tuple=Depends(get_current_user)):
    try:
        manager=await PromptsManager.create()
        success=await manager.delete_prompt(prompt_id)
        if not success: raise HTTPException(status_code=404,detail="Prompt not found")
        return {"success":True}
    except ValueError as e:
        raise HTTPException(status_code=400,detail=str(e)) 