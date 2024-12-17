import json
from datetime import datetime
from typing import List, Optional, Dict
import uuid
from pydantic import BaseModel, Field
from core import Msg
from psql_models import Thread, content_entity_association, thread_entity_association, Entity
from psql_helpers import get_session
from sqlalchemy import select, update, delete, insert, and_
from uuid import UUID
from dataclasses import asdict

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
        content_ids: Optional[List[UUID]] = None,
        speaker_names: Optional[List[str]] = None
    ) -> str:
        truncated_thread_name = (thread_name or "")[:125] if thread_name else ""
        messages_json = json.dumps([asdict(msg) for msg in messages])
        
        async with get_session() as session:
            async with session.begin():
                if thread_id:
                    # Update existing thread
                    await session.execute(
                        update(Thread)
                        .where(Thread.thread_id == thread_id)
                        .values(messages=messages_json)
                    )
                    thread = await session.execute(
                        select(Thread).where(Thread.thread_id == thread_id)
                    )
                    thread = thread.scalar_one()
                else:
                    # Create new thread
                    thread = Thread(
                        thread_id=str(uuid.uuid4()),
                        user_id=UUID(user_id),
                        thread_name=truncated_thread_name,
                        messages=messages_json
                    )
                    session.add(thread)
                    await session.flush()
                
                # Add content associations
                if content_ids:
                    # First clear existing
                    await session.execute(
                        delete(content_entity_association).where(
                            content_entity_association.c.content_id.in_(content_ids)
                        )
                    )
                    # Then add new
                    for content_id in content_ids:
                        # Get or create entity for the thread
                        entity = await session.execute(
                            select(Entity).where(
                                and_(
                                    Entity.name == thread.thread_id,
                                    Entity.type == "thread"
                                )
                            )
                        )
                        entity = entity.scalar_one_or_none()
                        if not entity:
                            entity = Entity(name=thread.thread_id, type="thread")
                            session.add(entity)
                            await session.flush()
                        
                        await session.execute(
                            insert(content_entity_association).values({
                                "content_id": content_id,
                                "entity_id": entity.id
                            })
                        )
                
                # Add speaker entity associations
                if speaker_names:
                    # First clear existing
                    await session.execute(
                        delete(thread_entity_association).where(
                            thread_entity_association.c.thread_id == thread.thread_id
                        )
                    )
                    # Then add new speakers
                    for speaker in speaker_names:
                        entity = await session.execute(
                            select(Entity).where(
                                and_(
                                    Entity.name == speaker,
                                    Entity.type == "speaker"
                                )
                            )
                        )
                        entity = entity.scalar_one_or_none()
                        if not entity:
                            entity = Entity(name=speaker, type="speaker")
                            session.add(entity)
                            await session.flush()
                        
                        await session.execute(
                            insert(thread_entity_association).values({
                                "thread_id": thread.thread_id,
                                "entity_id": entity.id
                            })
                        )
                
                await session.commit()
                return thread.thread_id

    async def get_threads_by_filter(
        self,
        user_id: str,
        content_ids: Optional[List[UUID]] = None,
        speaker_names: Optional[List[str]] = None
    ) -> List[dict]:
        async with get_session() as session:
            query = select(Thread).where(Thread.user_id == UUID(user_id))
            
            if content_ids:
                query = query.join(content_entity_association)
                query = query.where(
                    content_entity_association.c.content_id.in_(content_ids)
                )
            
            if speaker_names:
                query = query.join(thread_entity_association)
                query = query.join(Entity)
                query = query.where(
                    and_(
                        Entity.name.in_(speaker_names),
                        Entity.type == "speaker"
                    )
                )
            
            result = await session.execute(query)
            threads = result.scalars().all()
            
            return [{
                "thread_id": t.thread_id,
                "thread_name": t.thread_name,
                "timestamp": t.timestamp
            } for t in threads]

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
