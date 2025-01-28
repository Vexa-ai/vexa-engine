from typing import List, Optional, Dict, Any, AsyncGenerator
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from psql_helpers import get_session
from chat import UnifiedChatManager, QdrantSearchEngine, ElasticsearchBM25
import logging
from aiohttp.client_exceptions import ClientPayloadError

logger = logging.getLogger(__name__)

class ChatManager:
    def __init__(self, qdrant_engine: Optional[QdrantSearchEngine] = None, 
                 es_engine: Optional[ElasticsearchBM25] = None):
        self.qdrant_engine = qdrant_engine
        self.es_engine = es_engine
        
    @classmethod
    async def create(cls, qdrant_engine: Optional[QdrantSearchEngine] = None,
                    es_engine: Optional[ElasticsearchBM25] = None):
        return cls(qdrant_engine=qdrant_engine, es_engine=es_engine)
    
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
        meta: Optional[Dict] = None,
        session: AsyncSession = None
    ) -> AsyncGenerator[Dict[str, Any], None]:
        async with (session or get_session()) as session:
            unified_chat = UnifiedChatManager(
                session=session,
                qdrant_engine=self.qdrant_engine,
                es_engine=self.es_engine
            )
            try:
                async for result in unified_chat.chat(
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
            except ClientPayloadError as e:
                yield {"error": f"Chat streaming failed: {str(e)}"}
            except Exception as e:
                logger.error(f"Chat error: {str(e)}")
                yield {"error": f"Chat failed: {str(e)}"}
            
    async def edit_and_continue(
        self,
        user_id: str,
        thread_id: str,
        message_index: int,
        new_content: str,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        prompt: Optional[str] = None,
        session: AsyncSession = None,
        **context_kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        async with (session or get_session()) as session:
            unified_chat = UnifiedChatManager(
                session=session,
                qdrant_engine=self.qdrant_engine,
                es_engine=self.es_engine
            )
            try:
                async for result in unified_chat.edit_and_continue(
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
            except ClientPayloadError as e:
                yield {"error": f"Edit streaming failed: {str(e)}"}
            except Exception as e:
                logger.error(f"Edit error: {str(e)}")
                yield {"error": f"Edit failed: {str(e)}"} 