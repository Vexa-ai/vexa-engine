import json
from datetime import datetime
from typing import List, Optional, Dict
import uuid
from pydantic import BaseModel, Field
from core import Msg
from psql_models import Thread, content_entity_association, thread_entity_association, Entity
from psql_helpers import get_session
from sqlalchemy import select, update, delete, insert, and_, func
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
                        .values(
                            messages=messages_json,
                            content_id=content_ids[0] if content_ids else None
                        )
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
                        messages=messages_json,
                        content_id=content_ids[0] if content_ids else None
                    )
                    session.add(thread)
                    await session.flush()
                
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
                meeting_id=thread.content_id,
                timestamp=thread.timestamp
            )

    async def get_user_threads(self, user_id: str, content_id: Optional[UUID] = None) -> List[dict]:
        async with get_session() as session:
            # Base query
            query = select(Thread).filter(Thread.user_id == UUID(user_id))
            
            # Filter by content_id if provided, otherwise get global threads
            if content_id:
                query = query.filter(Thread.content_id == content_id)
            else:
                query = query.filter(Thread.content_id.is_(None))
                
            # Get threads ordered by timestamp
            result = await session.execute(query.order_by(Thread.timestamp.desc()))
            threads = result.scalars().all()
            
            return [{
                "thread_id": thread.thread_id,
                "thread_name": thread.thread_name,
                "timestamp": thread.timestamp,
                "content_id": str(thread.content_id) if thread.content_id else None
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

    async def rename_thread(self, thread_id: str, new_name: str) -> bool:
        """Rename a thread with the given ID."""
        truncated_name = new_name[:125] if new_name else ""
        
        async with get_session() as session:
            async with session.begin():
                try:
                    await session.execute(
                        update(Thread)
                        .where(Thread.thread_id == thread_id)
                        .values(thread_name=truncated_name)
                    )
                    await session.commit()
                    return True
                except Exception as e:
                    print(f"Error renaming thread: {e}")
                    return False

    async def edit_message(
        self,
        thread_id: str,
        message_index: int,
        new_content: str
    ) -> Optional[List[Msg]]:
        """Edit a message in a thread and truncate subsequent messages.
        
        Args:
            thread_id: The ID of the thread containing the message
            message_index: The index of the message to edit (0-based)
            new_content: The new content for the message
            
        Returns:
            The updated list of messages if successful, None if failed
        """
        async with get_session() as session:
            async with session.begin():
                try:
                    # Get thread
                    result = await session.execute(
                        select(Thread).where(Thread.thread_id == thread_id)
                    )
                    thread = result.scalar_one_or_none()
                    if not thread:
                        return None
                    
                    # Parse messages
                    messages_dicts = json.loads(thread.messages)
                    if message_index >= len(messages_dicts):
                        return None
                        
                    # Convert to Msg objects up to the edit point
                    messages = [
                        Msg(
                            role=msg["role"],
                            content=msg["content"],
                            service_content=msg.get("service_content")
                        ) 
                        for msg in messages_dicts[:message_index]
                    ]
                    
                    # Add edited message
                    edited_msg = messages_dicts[message_index]
                    messages.append(Msg(
                        role=edited_msg["role"],
                        content=new_content,
                        service_content=edited_msg.get("service_content")
                    ))
                    
                    # Update thread with truncated messages
                    messages_json = json.dumps([asdict(msg) for msg in messages])
                    await session.execute(
                        update(Thread)
                        .where(Thread.thread_id == thread_id)
                        .values(messages=messages_json)
                    )
                    await session.commit()
                    
                    return messages
                    
                except Exception as e:
                    print(f"Error editing message: {e}")
                    return None

    async def get_threads_by_exact_entities(
        self,
        user_id: str,
        entity_names: List[str]
    ) -> List[dict]:
        async with get_session() as session:
            # Subquery to get thread_ids that have exactly these entities
            thread_count_subquery = (
                select(
                    thread_entity_association.c.thread_id,
                    func.count(thread_entity_association.c.entity_id).label('entity_count')
                )
                .join(Entity, thread_entity_association.c.entity_id == Entity.id)
                .where(Entity.name.in_(entity_names))
                .group_by(thread_entity_association.c.thread_id)
                .having(func.count(thread_entity_association.c.entity_id) == len(entity_names))
            ).subquery()
            
            # Main query to get threads
            query = (
                select(Thread)
                .join(thread_count_subquery, Thread.thread_id == thread_count_subquery.c.thread_id)
                .where(Thread.user_id == UUID(user_id))
                .order_by(Thread.timestamp.desc())
            )
            
            result = await session.execute(query)
            threads = result.scalars().all()
            
            return [{
                "thread_id": t.thread_id,
                "thread_name": t.thread_name,
                "timestamp": t.timestamp
            } for t in threads]
