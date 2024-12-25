from typing import List, Optional, AsyncGenerator, Dict, Any
from datetime import datetime
from uuid import UUID
import re

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession


from core import system_msg, user_msg, assistant_msg, generic_call_, Msg
from prompts import Prompts
from thread_manager import ThreadManager
from logger import logger


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
        self.meeting_map = {}  # meeting_id -> int
        self.meeting_map_reverse = {}  # int -> meeting_id
        
    async def _get_raw_context(self, meeting_ids: List[UUID] = None, speakers: List[str] = None, **kwargs) -> str:
        results = await hybrid_search(
            query=kwargs.get('query', ''),
            qdrant_engine=self.qdrant_engine,
            es_engine=self.es_engine,
            meeting_ids=[str(mid) for mid in meeting_ids] if meeting_ids else None,
            speakers=speakers,
            k=100
        )
        
        df = pd.DataFrame(results['results'])
        if df.empty:
            return "No relevant context found."
            
        # Create meeting maps
        unique_meetings = df['meeting_id'].unique()
        self.meeting_map = {mid: idx+1 for idx, mid in enumerate(unique_meetings)}
        self.meeting_map_reverse = {v: k for k, v in self.meeting_map.items()}
        
        meetings = []
        for meeting_id, group in df.groupby('meeting_id'):
            int_meeting_id = self.meeting_map[meeting_id]
            timestamp = pd.to_datetime(group['timestamp'].iloc[0])
            date_header = f"##  [{int_meeting_id}] - {timestamp.strftime('%B %d, %Y %H:%M')}"
            
            content_items = []
            for _, row in group.iterrows():
                time_prefix = f"[{row['formatted_time']}]" if row['formatted_time'] else ''
                if 'contextualized_content' in row:
                    content_items.append(f"- {time_prefix} {row['contextualized_content']}")
                content_items.append(f"  > {row['content']}")
            
            content = "\n".join(content_items)
            meetings.append(f"{date_header}\n\n{content}\n")
        
        return "\n".join(meetings)
    
class MeetingContextProvider(BaseContextProvider):
    """Provides context from meeting transcripts"""
    def __init__(self, meeting_ids, max_tokens: int = 40000):
        super().__init__(max_tokens)
        self.meeting_ids = meeting_ids
        self.unified_fallback = None

    async def _get_raw_context(self, session: AsyncSession, **kwargs) -> str:
        combined_context = []
        total_tokens = 0

        # Try to get transcripts for all meetings
        for meeting_id in self.meeting_ids:
            try:
                token = await get_meeting_token(meeting_id)
                vexa_api = VexaAPI(token=token)
                user_id = (await vexa_api.get_user_info())['id']
                transcription = await vexa_api.get_transcription(meeting_session_id=meeting_id, use_index=True)
                
                if not transcription:
                    logger.warning(f"No transcription found for meeting {meeting_id}")
                    continue

                df, formatted_input, start_time, _, transcript = transcription
                
                # Count tokens for this meeting's context
                meeting_tokens = count_tokens(formatted_input)
                
                # If adding this meeting would exceed the limit, stop here
                if total_tokens + meeting_tokens > self.max_tokens:
                    logger.info(f"Token limit reached at {total_tokens} tokens. Falling back to UnifiedContextProvider.")
                    # Initialize UnifiedContextProvider for fallback
                    if not self.unified_fallback:
                        self.unified_fallback = UnifiedContextProvider(
                            session=session,
                            qdrant_engine=kwargs.get('qdrant_engine'),
                            es_engine=kwargs.get('es_engine')
                        )
                    # Use UnifiedContextProvider with all meeting_ids
                    return await self.unified_fallback._get_raw_context(
                        meeting_ids=self.meeting_ids,
                        **kwargs
                    )

                total_tokens += meeting_tokens
                combined_context.append(formatted_input)

            except Exception as e:
                logger.error(f"Error getting transcript for meeting {meeting_id}: {str(e)}")
                continue

        if not combined_context:
            return "No meeting transcripts found"

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
        content_ids: Optional[List[UUID]] = None,
        speaker_names: Optional[List[str]] = None,
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
        print(combined_query)
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
        if isinstance(self, UnifiedChatManager) and not context_kwargs.get('meeting_ids'):
            final_output = await self._create_linked_output(
                output, 
                self.unified_context_provider.meeting_map_reverse
            )
            
        service_content = {
            'output': final_output,  # Using linked version as output
            'context': context
        }
        self.messages.append(assistant_msg(msg=final_output, service_content=service_content))

        # Use new thread manager methods with content and speaker associations
        if not thread_id:
            thread_id = await self.thread_manager.upsert_thread(
                user_id=user_id,
                thread_name=thread_name,
                messages=self.messages,
                content_ids=content_ids,
                speaker_names=speaker_names
            )
        else:
            await self.thread_manager.upsert_thread(
                user_id=user_id,
                messages=self.messages,
                thread_id=thread_id,
                content_ids=content_ids,
                speaker_names=speaker_names
            )

        yield {
            "thread_id": thread_id,
            "output": final_output,
            "service_content": service_content
        }
        
    

class UnifiedChatManager(ChatManager):
    def __init__(self, session: AsyncSession = None, qdrant_engine: Optional[QdrantSearchEngine] = None, 
                 es_engine: Optional[ElasticsearchBM25] = None):
        super().__init__()
        self.session = session
        self.unified_context_provider = UnifiedContextProvider(session, qdrant_engine, es_engine)
        self.allowed_models = {'gpt-4o-mini', 'gpt-4o', 'claude-3-5-sonnet-20240620'}

    async def _create_linked_output(self, output: str, meeting_map_reverse: dict) -> str:
        # Add space between consecutive reference numbers
        output = re.sub(r'\]\[', '] [', output)
        
        # Replace meeting references with links
        for int_id, meeting_id in meeting_map_reverse.items():
            url = f"/meeting/{meeting_id}"
            # Replace both "Meeting X" and "[X]" patterns
            output = re.sub(
                f'Meeting {int_id}(?!\])',
                f'[Meeting {int_id}]({url})',
                output
            )
            output = output.replace(f'[{int_id}]', f'[{int_id}]({url})')
            
        return output

    async def chat(self, user_id: str, query: str, meeting_ids: Optional[List[UUID]] = None,
                  entities: Optional[List[str]] = None, thread_id: Optional[str] = None,
                  model: Optional[str] = None, temperature: Optional[float] = None,
                  prompt: Optional[str] = None) -> AsyncGenerator[Dict[str, Any], None]:
        # Validate model
        if model and model not in self.allowed_models:
            model = 'gpt-4o-mini'  # Fallback to default
        
        # Validate access if meeting_ids provided
        if meeting_ids:
            meetings, _ = await get_accessible_content(
                session=self.session,
                user_id=UUID(user_id),
                limit=1000
            )
            accessible_meeting_ids = {str(m['content_id']) for m in meetings}
            
            # Check access for all provided meeting IDs
            for meeting_id in meeting_ids:
                if str(meeting_id) not in accessible_meeting_ids:
                    yield {
                        "error": f"No access to meeting {meeting_id}",
                        "service_content": {"error": f"No access to meeting {meeting_id}"}
                    }
                    return

        output = ""
        
        # Choose appropriate context provider
        context_provider = (
            MeetingContextProvider(meeting_ids) if meeting_ids 
            else self.unified_context_provider
        )
        
        context_kwargs = {
            "meeting_ids": meeting_ids if meeting_ids else None,
            "speakers": entities if entities else None,
            "session": self.session  # Add session for MeetingContextProvider
        }
        
        async for result in super().chat(
            user_id=user_id,
            query=query,
            context_provider=context_provider,
            thread_id=thread_id,
            model=model or self.model,
            temperature=temperature,
            prompt=prompt,
            content_ids=meeting_ids,
            speaker_names=entities,
            **context_kwargs
        ):
            if 'chunk' in result:
                output += result['chunk']
                yield {"chunk": result['chunk']}
            else:
                linked_output = await self._create_linked_output(
                    output, 
                    self.unified_context_provider.meeting_map_reverse
                ) if not meeting_ids else output
                
                # Update service content to include linked_output
                result['service_content']['output'] = linked_output
                
                yield {
                    "thread_id": result['thread_id'],
                    "linked_output": linked_output
                }
        
        
        
        
