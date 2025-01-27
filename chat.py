from typing import List, Optional, AsyncGenerator, Dict, Any
from datetime import datetime
from uuid import UUID
import re

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


from core import system_msg, user_msg, assistant_msg, generic_call_, Msg
from prompts import Prompts
from psql_models import Content, ContentType
from thread_manager import ThreadManager
from logger import logger
from content_resolver import resolve_content_ids


import pandas as pd

from core import count_tokens
from vexa import VexaAPI

from psql_access import get_meeting_token, get_accessible_content, get_user_name

from hybrid_search import hybrid_search


from qdrant_search import QdrantSearchEngine
from bm25_search import ElasticsearchBM25



class ChatResult(BaseModel):
    output: str
    messages: List[dict]
    thread_id: str
    thread_name: str
    service_content: Dict[str, Any]


class BaseContextProvider:
    """Base class for different context providers"""
    def __init__(self, max_tokens: int = 40000):
        self.max_tokens = max_tokens
        
    def _truncate_context(self, context: str, model: str = "gpt-4o-mini") -> str:
        tokens = count_tokens(context, model)
        if tokens <= self.max_tokens:
            return context
            
        # Truncate by paragraphs until under limit
        paragraphs = context.split('\n\n')
        result = []
        current_tokens = 0
        
        for p in paragraphs:
            p_tokens = count_tokens(p, model)
            if current_tokens + p_tokens > self.max_tokens:
                break
            result.append(p)
            current_tokens += p_tokens
            
        return '\n\n'.join(result)
        
    async def get_context(self, **kwargs) -> str:
        context = await self._get_raw_context(**kwargs)
        return context #self._truncate_context(context)
        
    async def _get_raw_context(self, **kwargs) -> str:
        raise NotImplementedError()


class UnifiedContextProvider(BaseContextProvider):
    def __init__(self, session: AsyncSession = None, qdrant_engine: Optional[QdrantSearchEngine] = None,
                 es_engine: Optional[ElasticsearchBM25] = None):
        super().__init__()
        self.session = session
        self.qdrant_engine = qdrant_engine
        self.es_engine = es_engine
        self.content_map = {}  # content_id -> int
        self.content_map_reverse = {}  # int -> content_id
        
    async def _get_raw_context(self, content_ids: List[UUID] = None, **kwargs) -> str:
        results = await hybrid_search(
            query=kwargs.get('query', ''),
            qdrant_engine=self.qdrant_engine,
            es_engine=self.es_engine,
            content_ids=[str(cid) for cid in content_ids] if content_ids else None,

            k=100
        )
        
        df = pd.DataFrame(results['results'])
        if df.empty:
            return "No relevant context found."
            
        # Create content maps
        unique_contents = df['content_id'].unique()
        self.content_map = {cid: idx+1 for idx, cid in enumerate(unique_contents)}
        self.content_map_reverse = {v: k for k, v in self.content_map.items()}
        
        contents = []
        for content_id, group in df.groupby('content_id'):
            int_content_id = self.content_map[content_id]
            timestamp = pd.to_datetime(group['timestamp'].iloc[0])
            date_header = f"##  [{int_content_id}] - {timestamp.strftime('%B %d, %Y %H:%M')}"
            
            content_items = []
            for _, row in group.iterrows():
                time_prefix = f"[{row['formatted_time']}]" if row['formatted_time'] else ''
                if 'contextualized_content' in row:
                    content_items.append(f"- {time_prefix} {row['contextualized_content']}")
                content_items.append(f"  > {row['content']}")
            
            content = "\n".join(content_items)
            contents.append(f"{date_header}\n\n{content}\n")
        
        return "\n".join(contents)
    
class ContentContextProvider(BaseContextProvider):
    """Provides context from content items"""
    def __init__(self, content_ids, max_tokens: int = 40000):
        super().__init__(max_tokens)
        self.content_ids = content_ids
        self.unified_fallback = None

    async def _get_raw_context(self, session: AsyncSession, **kwargs) -> str:
        combined_context = []
        total_tokens = 0

        # Try to get content for all IDs
        for content_id in self.content_ids:
            try:
                # Get coselecttype and access level
                query = select(Content.type, Content.text).where(Content.id == content_id)
                result = await session.execute(query)
                content = result.first()
                
                if not content:
                    logger.warning(f"No content found for ID {content_id}")
                    continue
                    
                content_type, text = content
                
                # For meetings, get transcript from Vexa
                if content_type == ContentType.MEETING.value:
                    token = await get_meeting_token(content_id)
                    vexa_api = VexaAPI(token=token)
                    transcription = await vexa_api.get_transcription(meeting_session_id=content_id, use_index=True)
                    
                    if not transcription:
                        logger.warning(f"No transcription found for meeting {content_id}")
                        continue
                        
                    df, formatted_input, start_time, _, transcript = transcription
                    text = formatted_input
                
                # Count tokens for this content
                content_tokens = count_tokens(text)
                
                # If adding this content would exceed the limit, use UnifiedContextProvider
                if total_tokens + content_tokens > self.max_tokens:
                    logger.info(f"Token limit reached at {total_tokens} tokens. Falling back to UnifiedContextProvider.")
                    if not self.unified_fallback:
                        self.unified_fallback = UnifiedContextProvider(
                            session=session,
                            qdrant_engine=kwargs.get('qdrant_engine'),
                            es_engine=kwargs.get('es_engine')
                        )
                    return await self.unified_fallback._get_raw_context(
                        content_ids=self.content_ids,
                        **kwargs
                    )

                total_tokens += content_tokens
                combined_context.append(text)

            except Exception as e:
                logger.error(f"Error getting content for ID {content_id}: {str(e)}")
                continue

        if not combined_context:
            return "No content found"

        return "\n\n---\n\n".join(combined_context)



class ChatManager:
    def __init__(self):
        self.thread_manager = ThreadManager()
        self.model = "gpt-4o-mini"
        self.prompts = Prompts()
        self.messages = []  # Add messages as instance attribute
        
    async def chat(
        self,
        user_id: str,
        query: str,
        context_provider: BaseContextProvider,
        thread_id: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        prompt: Optional[str] = None,
        content_id: Optional[UUID] = None,
        entity_id: Optional[int] = None,
        meta: Optional[Dict] = None,
        **context_kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        # Get user name
        user_name = await get_user_name(user_id, self.session) or "User"
        
        if thread_id:
            thread = await self.thread_manager.get_thread(thread_id)
            if not thread:
                raise ValueError(f"Thread with id {thread_id} not found")
            if thread.user_id != user_id:
                raise ValueError("Thread belongs to different user")
            self.messages = thread.messages  # Update instance messages
            thread_name = thread.thread_name
        else:
            self.messages = []  # Reset messages for new thread
            thread_name = query[:50]

        # Combine historical queries with current query
        historical_queries = " ".join([msg.content for msg in self.messages if msg.role == 'user'])
        combined_query = f"{historical_queries} {query}".strip()
        
        # Get context using combined query
        context = await context_provider.get_context(
            user_id=user_id,
            query=combined_query,  # Using combined query instead of just current query
            **context_kwargs
        )
        
        prompt = prompt or self.prompts.chat_december
        prompt = prompt.format(user_name=user_name)
        prompt += "\n\n" + 'now is ' + datetime.now().strftime('%B %d, %Y %H:%M')
        
        context_msg = system_msg(f"Context: {context}")
        messages_context = [
            system_msg(prompt),
            context_msg,
            *self.messages,
            user_msg(f'User request: {query}')
        ]

        output = ""
        async for chunk in generic_call_(messages_context, streaming=True):
            output += chunk
            yield {"chunk": chunk}

        self.messages.append(user_msg(query))
        
        # Create linked_output if using UnifiedChatManager
        final_output = output
        if isinstance(self, UnifiedChatManager) and not context_kwargs.get('content_ids'):
            final_output = await self._create_linked_output(
                output, 
                self.unified_context_provider.content_map_reverse
            )
            
        service_content = {
            'output': final_output,  # Using linked version as output
            'context': context
        }
        self.messages.append(assistant_msg(msg=final_output, service_content=service_content))

        # Use new thread manager methods with content and entity associations
        if not thread_id:
            thread_id = await self.thread_manager.upsert_thread(
                user_id=user_id,
                thread_name=thread_name,
                messages=self.messages,
                content_id=content_id,
                entity_id=entity_id,
                meta=meta
            )
        else:
            await self.thread_manager.upsert_thread(
                user_id=user_id,
                messages=self.messages,
                thread_id=thread_id,
                content_id=content_id,
                entity_id=entity_id,
                meta=meta
            )

        yield {
            "thread_id": thread_id,
            "output": final_output,
            "service_content": service_content
        }
        
    async def edit_and_continue(
        self,
        user_id: str,
        thread_id: str,
        message_index: int,
        new_content: str,
        context_provider: BaseContextProvider,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        prompt: Optional[str] = None,
        **context_kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Edit a message and continue the chat from that point.
        
        Args:
            user_id: The ID of the user
            thread_id: The ID of the thread
            message_index: The index of the message to edit
            new_content: The new content for the message
            context_provider: Provider for chat context
            model: Optional model override
            temperature: Optional temperature override
            prompt: Optional prompt override
        """
        # Get user name
        user_name = await get_user_name(user_id, self.session) or "User"
        
        # Edit message in thread
        updated_messages = await self.thread_manager.edit_message(
            thread_id=thread_id,
            message_index=message_index,
            new_content=new_content
        )
        if not updated_messages:
            yield {
                "error": "Failed to edit message",
                "service_content": {"error": "Failed to edit message"}
            }
            return
            
        self.messages = updated_messages
        
        # Get context using all user messages up to edit point
        historical_queries = " ".join([
            msg.content 
            for msg in self.messages[:message_index+1] 
            if msg.role == 'user'
        ])
        
        context = await context_provider.get_context(
            user_id=user_id,
            query=historical_queries,
            **context_kwargs
        )
        
        prompt = prompt or self.prompts.chat_december
        prompt = prompt.format(user_name=user_name)
        prompt += "\n\n" + 'now is ' + datetime.now().strftime('%B %d, %Y %H:%M')
        
        context_msg = system_msg(f"Context: {context}")
        messages_context = [
            system_msg(prompt),
            context_msg,
            *self.messages
        ]

        output = ""
        async for chunk in generic_call_(messages_context, streaming=True):
            output += chunk
            yield {"chunk": chunk}
            
        # Create final response
        service_content = {
            'output': output,
            'context': context
        }
        self.messages.append(assistant_msg(msg=output, service_content=service_content))
        
        # Update thread with new messages
        thread_id = await self.thread_manager.upsert_thread(
            user_id=user_id,
            messages=self.messages,
            thread_id=thread_id
        )
        
        yield {
            "thread_id": thread_id,
            "output": output,
            "service_content": service_content
        }


class UnifiedChatManager(ChatManager):
    def __init__(self, session: AsyncSession = None, qdrant_engine: Optional[QdrantSearchEngine] = None, 
                 es_engine: Optional[ElasticsearchBM25] = None):
        super().__init__()
        self.session = session
        self.unified_context_provider = UnifiedContextProvider(session, qdrant_engine, es_engine)
        self.allowed_models = {'gpt-4o-mini', 'gpt-4o', 'claude-3-5-sonnet-20240620'}

    async def _create_linked_output(self, output: str, content_map_reverse: dict) -> str:
        # Add space between consecutive reference numbers
        output = re.sub(r'\]\[', '] [', output)
        
        # Replace content references with links
        for int_id, content_id in content_map_reverse.items():
            url = f"/content/{content_id}"
            # Replace both "Content X" and "[X]" patterns
            output = re.sub(
                f'Content {int_id}(?!\])',
                f'[Content {int_id}]({url})',
                output
            )
            output = output.replace(f'[{int_id}]', f'[{int_id}]({url})')
            
        return output

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
        # Validate model
        if model and model not in self.allowed_models:
            model = 'gpt-4o-mini'  # Fallback to default
        
        # Resolve all content IDs
        all_content_ids = await resolve_content_ids(
            session=self.session,
            content_id=content_id,
            entity_id=entity_id,
            content_ids=content_ids,
            entity_ids=entity_ids
        )
        
        # Validate access
        contents, _ = await get_accessible_content(
            session=self.session,
            user_id=UUID(user_id),
            limit=1000
        )
        accessible_content_ids = {str(c['content_id']) for c in contents}
        
        if len(all_content_ids) > 0:
            # Filter to accessible content
            accessible_content_ids = [
                cid for cid in all_content_ids 
                if str(cid) in accessible_content_ids
            ]
        
        if not accessible_content_ids:
            yield {
                "error": "No access to any requested content",
                "service_content": {"error": "No access to any requested content"}
            }
            return
        
        output = ""
        
        # Choose appropriate context provider
        # context_provider = (
        #     ContentContextProvider(accessible_content_ids) if accessible_content_ids 
        #     else self.unified_context_provider
        # )
        
        context_provider = self.unified_context_provider
        
        # Prepare meta for thread storage
        thread_meta = meta or {}
        if content_ids or entity_ids:
            thread_meta['content_ids'] = [str(cid) for cid in accessible_content_ids]
        if entity_ids:
            thread_meta['entity_ids'] = entity_ids
        
        context_kwargs = {
            "content_ids": accessible_content_ids,
            "session": self.session,
            "qdrant_engine": self.unified_context_provider.qdrant_engine,
            "es_engine": self.unified_context_provider.es_engine
        }
        
        async for result in super().chat(
            user_id=user_id,
            query=query,
            context_provider=context_provider,
            thread_id=thread_id,
            model=model or self.model,
            temperature=temperature,
            prompt=None,
            content_id=content_id,
            entity_id=entity_id,
            meta=thread_meta,
            **context_kwargs
        ):
            if 'chunk' in result:
                output += result['chunk']
                yield {"chunk": result['chunk']}
            else:
                linked_output = await self._create_linked_output(
                    output, 
                    self.unified_context_provider.content_map_reverse
                ) if not content_ids else output
                
                # Update service content to include linked_output
                result['service_content']['output'] = linked_output
                
                yield {
                    "thread_id": result['thread_id'],
                    "output": output,
                    "linked_output": linked_output,
                    "service_content": result['service_content']
                }
        
        
        
        
