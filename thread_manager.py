import json
from datetime import datetime
from typing import List, Optional, Dict
import uuid
from pydantic import BaseModel, Field
from core import Msg
from psql_models import Thread, content_entity_association, Entity
from psql_helpers import get_session
from sqlalchemy import select, update, delete, insert, and_, func
from uuid import UUID
from dataclasses import asdict

class SearchAssistantThread(BaseModel):
    thread_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id: str
    thread_name: str
    messages: List[Msg] = []
    content_id: Optional[UUID] = None
    entity_id: Optional[int] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    meta: Optional[Dict] = None

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
        content_id: Optional[UUID] = None,
        entity_id: Optional[int] = None,
        content_ids: Optional[List[UUID]] = None,
        entities: Optional[List[str]] = None,
        meta: Optional[Dict] = None
    ) -> str:
        truncated_thread_name = (thread_name or "")[:125] if thread_name else ""
        messages_json = json.dumps([asdict(msg) for msg in messages])
        
        thread_meta = meta or {}
        if content_ids:
            thread_meta['content_ids'] = [str(cid) for cid in content_ids]
        if entities:
            thread_meta['entities'] = entities
        
        async with get_session() as session:
            async with session.begin():
                if thread_id:
                    await session.execute(
                        update(Thread)
                        .where(Thread.thread_id == thread_id)
                        .values(
                            messages=messages_json,
                            thread_name=truncated_thread_name,
                            content_id=content_id,
                            entity_id=entity_id,
                            meta=thread_meta if thread_meta else None
                        )
                    )
                    thread = await session.execute(
                        select(Thread).where(Thread.thread_id == thread_id)
                    )
                    thread = thread.scalar_one()
                else:
                    thread = Thread(
                        thread_id=str(uuid.uuid4()),
                        user_id=UUID(user_id),
                        thread_name=truncated_thread_name,
                        messages=messages_json,
                        content_id=content_id,
                        entity_id=entity_id,
                        meta=thread_meta if thread_meta else None
                    )
                    session.add(thread)
                    await session.flush()
                
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
                content_id=thread.content_id,
                entity_id=thread.entity_id,
                timestamp=thread.timestamp,
                meta=thread.meta
            )

    async def get_user_threads(
        self, 
        user_id: str, 
        content_id: Optional[UUID] = None,
        entity_id: Optional[int] = None,
        only_archived: bool = False,
        limit: int = 50,
        offset: int = 0,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> dict:
        async with get_session() as session:
            query = select(Thread).filter(Thread.user_id == UUID(user_id))
            
            if content_id:
                query = query.filter(Thread.content_id == content_id)
            elif entity_id:
                query = query.filter(Thread.entity_id == entity_id)
            else:
                query = query.filter(Thread.content_id.is_(None), Thread.entity_id.is_(None))
            
            query = query.filter(Thread.is_archived == only_archived)
            
            # Apply date filters if provided
            if start_date:
                query = query.filter(Thread.timestamp >= start_date)
            if end_date:
                query = query.filter(Thread.timestamp <= end_date)
            
            # Get total count
            count_query = select(func.count()).select_from(query.subquery())
            total = await session.scalar(count_query)
            
            # Apply pagination
            query = query.order_by(Thread.timestamp.desc()).offset(offset).limit(limit)
            result = await session.execute(query)
            threads = result.scalars().all()
            
            return {
                "total": total,
                "threads": [{
                    "thread_id": thread.thread_id,
                    "thread_name": thread.thread_name,
                    "timestamp": thread.timestamp,
                    "content_id": str(thread.content_id) if thread.content_id else None,
                    "entity_id": thread.entity_id,
                    "meta": thread.meta,
                    "is_archived": thread.is_archived
                } for thread in threads]
            }

    async def get_messages_by_thread_id(self, thread_id: str) -> Optional[List[Msg]]:
        thread = await self.get_thread(thread_id)
        if thread:
            return thread.messages
        return None

    async def archive_thread(self, thread_id: str) -> bool:
        async with get_session() as session:
            try:
                result = await session.execute(
                    update(Thread)
                    .where(Thread.thread_id == thread_id)
                    .values(is_archived=True)
                )
                await session.commit()
                rows_affected = result.rowcount
                return rows_affected > 0
            except Exception as e:
                print(f"Error archiving thread {thread_id}: {str(e)}")
                return False

    async def unarchive_thread(self, thread_id: str) -> bool:
        async with get_session() as session:
            try:
                result = await session.execute(
                    update(Thread)
                    .where(Thread.thread_id == thread_id)
                    .values(is_archived=False)
                )
                await session.commit()
                rows_affected = result.rowcount
                return rows_affected > 0
            except Exception as e:
                print(f"Error unarchiving thread {thread_id}: {str(e)}")
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
                    result = await session.execute(
                        select(Thread).where(Thread.thread_id == thread_id)
                    )
                    thread = result.scalar_one_or_none()
                    if not thread:
                        return None
                    
                    messages_dicts = json.loads(thread.messages)
                    if message_index >= len(messages_dicts):
                        return None
                        
                    messages = [
                        Msg(
                            role=msg["role"],
                            content=msg["content"],
                            service_content=msg.get("service_content")
                        ) 
                        for msg in messages_dicts[:message_index]
                    ]
                    
                    edited_msg = messages_dicts[message_index]
                    messages.append(Msg(
                        role=edited_msg["role"],
                        content=new_content,
                        service_content=edited_msg.get("service_content")
                    ))
                    
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
        """Get threads that have exactly these entities in meta"""
        async with get_session() as session:
            query = (
                select(Thread)
                .where(
                    and_(
                        Thread.user_id == UUID(user_id),
                        Thread.meta['entities'].contains(entity_names)
                    )
                )
            )
            
            result = await session.execute(query)
            threads = result.scalars().all()
            
            return [{
                "thread_id": t.thread_id,
                "thread_name": t.thread_name,
                "timestamp": t.timestamp,
                "content_id": str(t.content_id) if t.content_id else None,
                "entity": t.entity,
                "meta": t.meta
            } for t in threads]
