from typing import List, Optional, Dict, Any, AsyncGenerator
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from psql_helpers import get_session
from chat import UnifiedChatManager, QdrantSearchEngine, ElasticsearchBM25
import logging

logger = logging.getLogger(__name__)

class ChatManager:
    def __init__(self, session: AsyncSession = None, qdrant_engine: Optional[QdrantSearchEngine] = None, 
                 es_engine: Optional[ElasticsearchBM25] = None):
        self.session = session
        self.unified_chat = UnifiedChatManager(
            session=session,
            qdrant_engine=qdrant_engine,
            es_engine=es_engine
        )
        
    @classmethod
    async def create(cls, session: AsyncSession = None, 
                    qdrant_engine: Optional[QdrantSearchEngine] = None,
                    es_engine: Optional[ElasticsearchBM25] = None):
        return cls(session=session, qdrant_engine=qdrant_engine, es_engine=es_engine)
    
    async def chat(
        self,
        user_id: str,
        query: str,
        content_id: Optional[UUID] = None,
        entity_id: Optional[int] = None,
        content_ids: Optional[List[UUID]] = None,
        entity_ids: Optional[List[int]] = None,
        thread_id: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        meta: Optional[Dict] = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        async for result in self.unified_chat.chat(
            user_id=user_id,
            query=query,
            content_id=content_id,
            entity_id=entity_id,
            content_ids=content_ids,
            entity_ids=entity_ids,
            thread_id=thread_id,
            model=model,
            temperature=temperature,
            meta=meta
        ):
            yield result
            
    async def edit_and_continue(
        self,
        user_id: str,
        thread_id: str,
        message_index: int,
        new_content: str,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        prompt: Optional[str] = None,
        **context_kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        async for result in self.unified_chat.edit_and_continue(
            user_id=user_id,
            thread_id=thread_id,
            message_index=message_index,
            new_content=new_content,
            model=model,
            temperature=temperature,
            prompt=prompt,
            **context_kwargs
        ):
            yield result 