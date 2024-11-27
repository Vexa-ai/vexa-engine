import json
from datetime import datetime
from typing import List, Optional, Dict
import uuid
from pydantic import BaseModel, Field
from core import Msg
from psql_models import Thread
from psql_helpers import get_session
from sqlalchemy import select, update
from uuid import UUID

class SearchAssistantThread(BaseModel):
    thread_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    thread_name: str
    messages: List[Msg] = []
    meeting_id: Optional[UUID] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        arbitrary_types_allowed = True

class ThreadManager:
    def __init__(self):
        pass

    @classmethod
    async def create(cls):
        return cls()

    async def upsert_thread(
        self, 
        user_id: str, 
        messages: List[Msg], 
        thread_name: Optional[str] = None, 
        thread_id: Optional[str] = None,
        meeting_id: Optional[UUID] = None
    ) -> str:
        # Truncate thread_name if it's longer than 125 characters
        truncated_thread_name = (thread_name or "")[:125] if thread_name else ""
        
        # Convert Msg objects to dicts for storage
        messages_dicts = [{
            "role": msg.role,
            "content": msg.content,
            "service_content": msg.service_content
        } for msg in messages]
        
        if thread_id:
            thread = SearchAssistantThread(
                thread_id=thread_id,
                user_id=user_id,
                thread_name=truncated_thread_name,
                messages=messages_dicts,
                meeting_id=meeting_id
            )
        else:
            thread = SearchAssistantThread(
                user_id=user_id,
                thread_name=truncated_thread_name,
                messages=messages_dicts,
                meeting_id=meeting_id
            )

        messages_json = json.dumps(messages_dicts)
        
        async with get_session() as session:
            if thread_id:
                await session.execute(
                    update(Thread)
                    .where(Thread.thread_id == thread_id)
                    .values(messages=messages_json, meeting_id=meeting_id)
                )
            else:
                db_thread = Thread(
                    thread_id=thread.thread_id,
                    user_id=UUID(user_id),
                    thread_name=truncated_thread_name,
                    messages=messages_json,
                    meeting_id=meeting_id
                )
                session.add(db_thread)
            await session.commit()
            
        return thread.thread_id

    async def get_thread(self, thread_id: str) -> Optional[SearchAssistantThread]:
        async with get_session() as session:
            result = await session.execute(
                select(Thread).where(Thread.thread_id == thread_id)
            )
            thread = result.scalar_one_or_none()
            if not thread:
                return None
            
            # Convert stored dict messages back to Msg objects
            messages_dicts = json.loads(thread.messages)
            messages = [
                Msg(
                    role=msg["role"],
                    content=msg["content"],
                    service_content=msg.get("service_content")
                ) 
                for msg in messages_dicts
            ]
            
            return SearchAssistantThread(
                thread_id=thread.thread_id,
                user_id=str(thread.user_id),
                thread_name=thread.thread_name,
                messages=messages,
                meeting_id=thread.meeting_id,
                timestamp=thread.timestamp
            )

    async def get_user_threads(self, user_id: str, meeting_id: Optional[UUID] = None) -> List[dict]:
        async with get_session() as session:
            # Base query
            query = select(Thread).filter(Thread.user_id == UUID(user_id))
            
            # Filter by meeting_id if provided, otherwise get global threads
            if meeting_id:
                query = query.filter(Thread.meeting_id == meeting_id)
            else:
                query = query.filter(Thread.meeting_id.is_(None))
                
            # Get threads ordered by timestamp
            result = await session.execute(query.order_by(Thread.timestamp.desc()))
            threads = result.scalars().all()
            
            return [{
                "thread_id": thread.thread_id,
                "thread_name": thread.thread_name,
                "timestamp": thread.timestamp,
                "meeting_id": str(thread.meeting_id) if thread.meeting_id else None
            } for thread in threads]

    async def get_messages_by_thread_id(self, thread_id: str) -> Optional[List[Msg]]:
        thread = await self.get_thread(thread_id)
        if thread:
            return thread.messages
        return None

    async def delete_thread(self, thread_id: str) -> bool:
        async with get_session() as session:
            try:
                result = await session.execute(
                    select(Thread).filter(Thread.thread_id == thread_id)
                )
                thread = result.scalar_one_or_none()
                if thread:
                    await session.delete(thread)
                    await session.commit()
                    print(f"Thread deleted with id {thread_id}")
                    return True
                return False
            except Exception as e:
                print(f"Error deleting thread {thread_id}: {str(e)}")
                return False
