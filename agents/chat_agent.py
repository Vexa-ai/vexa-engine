from typing import List, Optional, Dict, Any
from datetime import datetime
from uuid import UUID
from dataclasses import dataclass
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from pydantic_ai import Agent, RunContext
from .tools.hybrid_search import HybridSearchTool
from qdrant_search import QdrantSearchEngine
from bm25_search import ElasticsearchBM25
from psql_access import get_session
from thread_manager import ThreadManager
from core import count_tokens, Msg
@dataclass
class ChatDependencies:
    user_id: str
    session: Optional[AsyncSession] = None
    thread_id: Optional[str] = None
class ChatResult(BaseModel):
    response: str = Field(description="The agent's response to the user")
    thread_id: Optional[str] = Field(None, description="ID of the conversation thread")
class ChatAgent(Agent):
    def __init__(self, qdrant_engine: QdrantSearchEngine, es_engine: ElasticsearchBM25):
        super().__init__(
            model="openai:gpt-4o-mini",
            deps_type=ChatDependencies,
            result_type=ChatResult,
            system_prompt="You are a helpful assistant with access to meeting transcripts. Use the hybrid_search tool to find relevant information."
        )
        self.thread_manager = ThreadManager()
        # Create tools after agent initialization
        self.hybrid_search = HybridSearchTool(self, qdrant_engine=qdrant_engine, es_engine=es_engine)
    async def chat(
        self,
        user_id: str,
        query: str,
        thread_id: Optional[str] = None,
        temperature: Optional[float] = None,
        session: AsyncSession = None
    ) -> ChatResult:
        # Get or create thread
        if thread_id:
            thread = await self.thread_manager.get_thread(thread_id)
            if not thread:
                raise ValueError(f"Thread with id {thread_id} not found")
            if thread.user_id != user_id:
                raise ValueError("Thread belongs to different user")
            messages = thread.messages
            thread_name = thread.thread_name
        else:
            messages = []
            thread_name = query[:50]
        # Setup dependencies
        deps = ChatDependencies(
            user_id=user_id,
            session=session,
            thread_id=thread_id
        )
        # Run agent
        result = await self.run(query, deps=deps)
        # Store messages
        messages.append(Msg(role="user", content=query))
        messages.append(Msg(role="assistant", content=result.data.response))
        # Update thread
        async with (session or get_session()) as session:
            thread_id = await self.thread_manager.upsert_thread(
                user_id=user_id,
                thread_name=thread_name,
                messages=messages,
                thread_id=thread_id
            )
        # Return result with thread_id
        return ChatResult(
            response=result.data.response,
            thread_id=thread_id
        ) 