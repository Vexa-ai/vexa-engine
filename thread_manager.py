import json
from datetime import datetime
from typing import List, Optional
import uuid
from pydantic import BaseModel, Field
from core import Msg
from psql_models import Thread, async_session, get_session
from sqlalchemy import select
from uuid import UUID

class SearchAssistantThread(BaseModel):
    thread_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    thread_name: str
    messages: List[Msg]
    timestamp: datetime = Field(default_factory=datetime.utcnow)

class ThreadManager:
    def __init__(self):
        pass

    @classmethod
    async def create(cls):
        return cls()

    async def upsert_thread(self, user_id: str, messages: List[Msg], thread_name: Optional[str] = None, thread_id: Optional[str] = None) -> str:
        async with get_session() as session:
            if thread_id:
                existing_thread = await self.get_thread(thread_id)
                if existing_thread:
                    thread = SearchAssistantThread(
                        thread_id=thread_id,
                        user_id=user_id,
                        thread_name=existing_thread.thread_name,
                        messages=messages,
                        timestamp=existing_thread.timestamp
                    )
                else:
                    thread = SearchAssistantThread(
                        thread_id=thread_id,
                        user_id=user_id,
                        thread_name=thread_name or "",
                        messages=messages
                    )
            else:
                thread = SearchAssistantThread(
                    user_id=user_id,
                    thread_name=thread_name or "",
                    messages=messages
                )

            # Convert messages to JSON string
            messages_json = json.dumps([msg.__dict__ for msg in thread.messages])
            
            db_thread = Thread(
                thread_id=thread.thread_id,
                user_id=UUID(thread.user_id),
                thread_name=thread.thread_name,
                messages=messages_json,
                timestamp=thread.timestamp
            )
            
            await session.merge(db_thread)
            await session.commit()
            
            return thread.thread_id

    async def get_thread(self, thread_id: str) -> Optional[SearchAssistantThread]:
        async with get_session() as session:
            result = await session.execute(
                select(Thread).filter(Thread.thread_id == thread_id)
            )
            thread = result.scalar_one_or_none()
            
            if thread:
                messages = [Msg(**msg) for msg in json.loads(thread.messages)]
                return SearchAssistantThread(
                    thread_id=thread.thread_id,
                    user_id=str(thread.user_id),
                    thread_name=thread.thread_name,
                    messages=messages,
                    timestamp=thread.timestamp
                )
            return None

    async def get_user_threads(self, user_id: str) -> List[dict]:
        async with get_session() as session:
            result = await session.execute(
                select(Thread)
                .filter(Thread.user_id == UUID(user_id))
                .order_by(Thread.timestamp.desc())
            )
            threads = result.scalars().all()
            
            return [
                {
                    "thread_id": thread.thread_id,
                    "thread_name": thread.thread_name,
                    "timestamp": thread.timestamp,
                }
                for thread in threads
            ]

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
