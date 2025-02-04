from sqlalchemy import select, update, delete, and_
from sqlalchemy.ext.asyncio import AsyncSession
from psql_models import Prompt, PromptTypeEnum, AccessLevel
from psql_helpers import get_session
from uuid import UUID
from datetime import datetime, timezone

class PromptsManager:
 def __init__(self): pass
 @classmethod
 async def create(cls): return cls()
 async def get_prompts(self,user_id:str,prompt_type:PromptTypeEnum=None,session:AsyncSession=None):
  async with (session or get_session()) as session:
   q=select(Prompt)
   if prompt_type: q=q.where(Prompt.type==prompt_type.value)
   q=q.where((Prompt.user_id==None)|(Prompt.user_id==UUID(user_id)))
   r=await session.execute(q)
   return r.scalars().all()
 async def get_global_prompts(self,prompt_type:PromptTypeEnum=None,session:AsyncSession=None):
  async with (session or get_session()) as session:
   q=select(Prompt).where(Prompt.user_id==None)
   if prompt_type: q=q.where(Prompt.type==prompt_type.value)
   r=await session.execute(q)
   return r.scalars().all()
 async def create_prompt(self,prompt_text:str,prompt_type:str,user_id:str=None,alias:str=None,session:AsyncSession=None):
  try: prompt_type=PromptTypeEnum(prompt_type.lower())
  except ValueError: raise ValueError(f"Invalid prompt type. Must be one of: {[e.value for e in PromptTypeEnum]}")
  async with (session or get_session()) as session:
   new=Prompt(prompt=prompt_text,type=prompt_type.value,user_id=UUID(user_id) if user_id else None,alias=alias)
   session.add(new)
   await session.commit()
   return new.id
 async def create_global_prompt(self,prompt_text:str,prompt_type:str,alias:str=None,session:AsyncSession=None):
  return await self.create_prompt(prompt_text,prompt_type,None,alias,session)
 async def update_prompt(self,prompt_id:UUID,prompt_text:str=None,prompt_type:str=None,alias:str=None,session:AsyncSession=None):
  async with (session or get_session()) as session:
   async with session.begin():
    update_values={}
    if prompt_text: update_values['prompt']=prompt_text
    if prompt_type:
     try: prompt_type=PromptTypeEnum(prompt_type.lower())
     except ValueError: raise ValueError(f"Invalid prompt type. Must be one of: {[e.value for e in PromptTypeEnum]}")
     update_values['type']=prompt_type.value
    if alias is not None: update_values['alias']=alias
    if update_values:
     result=await session.execute(update(Prompt).where(Prompt.id==prompt_id).values(**update_values))
     await session.commit()
     return result.rowcount>0
    return False
 async def delete_prompt(self,prompt_id:UUID,session:AsyncSession=None):
  async with (session or get_session()) as session:
   async with session.begin():
    result=await session.execute(delete(Prompt).where(Prompt.id==prompt_id))
    await session.commit()
    return result.rowcount>0
 async def archive_prompt(self,prompt_id:UUID,session:AsyncSession=None):
  async with (session or get_session()) as session:
   async with session.begin():
    result=await session.execute(update(Prompt).where(Prompt.id==prompt_id).values(access_level=AccessLevel.REMOVED.value))
    await session.commit()
    return result.rowcount>0 