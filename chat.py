from typing import List, Optional, AsyncGenerator, Dict, Any
from datetime import datetime
from uuid import UUID
import re

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from psql_models import Meeting

from core import system_msg, user_msg, assistant_msg, generic_call_, Msg
from prompts import Prompts
from thread_manager import ThreadManager
from psql_helpers import get_meeting_by_id, get_accessible_meetings
from logger import logger
from search import SearchAssistant
from pydantic_models import ParsedSearchRequest
import pandas as pd
import numpy as np


class ChatResult(BaseModel):
    output: str
    messages: List[dict]
    thread_id: str
    thread_name: str
    service_content: Dict[str, Any]


class BaseContextProvider:
    """Base class for different context providers"""
    async def get_context(self, **kwargs) -> str:
        raise NotImplementedError()


class MeetingContextProvider(BaseContextProvider):
    """Provides context from meeting transcripts"""
    async def get_context(self, session: AsyncSession, meeting_id: UUID, **kwargs) -> str:
        meeting = await get_meeting_by_id(session, meeting_id)
        if not meeting:
            return "No meeting found"
        return meeting.transcript


class SearchContextProvider(BaseContextProvider):
    """Provides context from search results"""
    def __init__(self, search_assistant: SearchAssistant):
        self.search_assistant = search_assistant

    async def get_context(self, user_id: str, query: str, **kwargs) -> str:
        # Parse search queries
        r = await ParsedSearchRequest.parse_request(query)
        queries = [q.query for q in r.search_queries]
        
        # Get search results
        search_results = [
            await self.search_assistant.search(q, user_id=user_id) 
            for q in queries
        ]
        search_results = pd.concat(search_results)
        search_results = search_results.drop(columns=['vector_scores', 'exact_matches'])\
            .drop_duplicates(subset=['topic_name', 'speaker_name', 'summary', 'details', 'meeting_id'])

        # Prepare context using search assistant's methods
        prepared_df, indexed_meetings = self.search_assistant.prep_context(search_results)
        
        # Calculate meeting relevance scores
        meeting_groups = search_results.sort_values('relevance_score', ascending=False)\
            .groupby('meeting_id').agg({
                'relevance_score': lambda x: np.average(x, weights=np.exp2(x)),
                'summary': 'first',
                'speaker_name': set,
                'timestamp': 'first'
            })\
            .sort_values('relevance_score', ascending=False)\
            .reset_index()\
            .head(20)

        # Format context
        columns_to_drop = ['timestamp', 'vector_scores', 'exact_matches', 'source', 'score', 'meeting_id']
        existing_columns = [col for col in columns_to_drop if col in prepared_df.columns]
        context_prepared = prepared_df.drop(columns=existing_columns)
        
        context = context_prepared.to_markdown(index=False) if not prepared_df.empty else "No relevant context found."
        
        # Store meeting URLs for later use
        self.url_dict = indexed_meetings.items()
        
        return context

    async def post_process_output(self, output: str) -> str:
        """Post-process the output to add hyperlinks"""
        # First, add a space between consecutive reference numbers
        output = re.sub(r'\]\[', '] [', output)
        
        # Then replace each reference with its link
        for key, url in self.url_dict:
            output = output.replace(f'[{key}]', f'[{key}]({url})')
        return output


class MeetingListContextProvider(BaseContextProvider):
    def __init__(self, meeting_ids: List[UUID]):
        self.meeting_ids = meeting_ids
        
    async def get_context(self, session: AsyncSession, **kwargs) -> str:
        # Query meetings and their transcripts
        meetings_query = (
            select(Meeting)
            .where(Meeting.meeting_id.in_(self.meeting_ids))
            .order_by(Meeting.timestamp.desc())
        )
        
        result = await session.execute(meetings_query)
        meetings = result.scalars().all()
        
        # Format context with meeting information
        context_parts = []
        for meeting in meetings:
            context_parts.append(f"Meeting: {meeting.meeting_name}")
            context_parts.append(f"Date: {meeting.timestamp}")
            context_parts.append(f"Transcript:\n{meeting.transcript}\n")
            if meeting.meeting_summary:
                context_parts.append(f"Summary:\n{meeting.meeting_summary}\n")
            context_parts.append("-" * 80 + "\n")
            
        return "\n".join(context_parts)


class ChatManager:
    def __init__(self):
        logger.info("Initializing ChatManager")
        self.thread_manager = ThreadManager()
        self.prompts = Prompts()
        self.model = "gpt-4o-mini"
        
    async def initialize(self):
        self.thread_manager = await ThreadManager.create()

    async def chat(
        self,
        user_id: str,
        query: str,
        context_provider: BaseContextProvider,
        thread_id: Optional[str] = None,
        meeting_id: Optional[UUID] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        prompt: Optional[str] = None,
        **context_kwargs
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Generic chat method that works with different context providers
        
        Args:
            user_id: User's ID
            query: User's question
            context_provider: Instance of BaseContextProvider
            thread_id: Optional thread ID for conversation continuity
            model: Optional model override
            temperature: Optional temperature override
            prompt: Optional system prompt override
            **context_kwargs: Additional arguments passed to context provider
        """
        if thread_id:
            thread = await self.thread_manager.get_thread(thread_id)
            if not thread:
                raise ValueError(f"Thread with id {thread_id} not found")
            if thread.user_id != user_id:
                raise ValueError("Thread belongs to different user")
            if meeting_id and thread.meeting_id != meeting_id:
                raise ValueError("Thread does not belong to specified meeting")
            # Convert stored dicts back to Msg objects
            messages = [Msg(**msg) for msg in thread.messages]
            thread_name = thread.thread_name
        else:
            messages = []
            thread_name = query[:50]

        context = await context_provider.get_context(**context_kwargs)
        
        context_msg = system_msg(f"Context: {context}")
        messages_context = [
            system_msg(prompt or self.prompts.search2),
            *messages,
            context_msg,
            user_msg(f'User request: {query}')
        ]

        output = ""
        async for chunk in generic_call_(messages_context, streaming=True):
            output += chunk
            yield {"chunk": chunk}

        messages.append(user_msg(query))
        service_content = {
            'output': output,
            'context': context
        }
        messages.append(assistant_msg(msg=output, service_content=service_content))

        if not thread_id:
            thread_id = await self.thread_manager.upsert_thread(
                user_id=user_id,
                thread_name=thread_name,
                messages=messages,  # Pass Msg objects directly
                meeting_id=meeting_id
            )
        else:
            await self.thread_manager.upsert_thread(
                user_id=user_id,
                messages=messages,  # Pass Msg objects directly
                thread_id=thread_id,
                meeting_id=meeting_id
            )

        yield {
            "thread_id": thread_id,
            "output": output,
            "service_content": service_content
        }
        
        
class SearchChatManager(ChatManager):
    """Enhanced chat manager with search-specific context handling"""
    
    async def chat(
        self,
        user_id: str,
        query: str,
        search_assistant: SearchAssistant,
        thread_id: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        prompt: Optional[str] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Chat specifically using search results as context"""
        
        # Create search context provider
        context_provider = SearchContextProvider(search_assistant)
        
        # Use base chat method with search context
        async for result in super().chat(
            user_id=user_id,
            query=query,
            context_provider=context_provider,
            thread_id=thread_id,
            model=model or self.model,
            temperature=temperature,
            prompt=prompt
        ):
            # Post-process output to add hyperlinks if available
            if "output" in result:
                result["output"] = await context_provider.post_process_output(result["output"])
                
                # Add context metadata
                if 'service_content' not in result:
                    result['service_content'] = {}
                result['service_content']['context_source'] = 'search'
                
            yield result
            

            
            
class MeetingChatManager(ChatManager):
    def __init__(self, session: AsyncSession):
        super().__init__()
        self.session = session
        self.model = "gpt-4o-mini"

    async def chat(
        self,
        user_id: str,
        query: str,
        meeting_ids: Optional[List[UUID]] = None,
        thread_id: Optional[str] = None,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        # Get accessible meetings for user
        meetings, _ = await get_accessible_meetings(
            session=self.session,
            user_id=UUID(user_id),
            limit=1000
        )
        
        if meeting_ids:
            # Filter to only accessible meetings
            accessible_meeting_ids = {str(m.meeting_id) for m in meetings}
            authorized_meeting_ids = [
                mid for mid in meeting_ids 
                if str(mid) in accessible_meeting_ids
            ]
            
            if not authorized_meeting_ids:
                yield {
                    "error": "No access to specified meetings",
                    "service_content": {
                        "context_source": "meeting",
                        "error": "No access to specified meetings"
                    }
                }
                return
        else:
            authorized_meeting_ids = [m.meeting_id for m in meetings]
            
        # Get thread info and messages
        messages = []
        if thread_id:
            thread = await self.thread_manager.get_thread(thread_id)
            if not thread:
                raise ValueError(f"Thread with id {thread_id} not found")
            messages = thread.messages
            thread_name = thread.thread_name
        else:
            thread_name = query

        # Create context from meetings
        context_provider = MeetingListContextProvider(authorized_meeting_ids)
        context = await context_provider.get_context(session=self.session)
        
        # Build message list maintaining Msg class structure
        messages_context = [
            system_msg(self.prompts.meeting_context),
            *messages,
            system_msg(f"Context: {context}"),
            user_msg(query)
        ]

        # Generate response
        output = ""
        async for chunk in generic_call_(
            messages=messages_context,
            model=model or self.model,
            temperature=temperature or 0.7,
            streaming=True
        ):
            output += chunk
            yield chunk

        # Update thread
        messages.append(user_msg(query))
        messages.append(assistant_msg(output))
        
        # Update thread with meeting_id for single meeting chats
        meeting_id = authorized_meeting_ids[0] if len(authorized_meeting_ids) == 1 else None
        
        if thread_id:
            await self.thread_manager.upsert_thread(
                user_id=user_id,
                messages=messages,
                thread_id=thread_id,
                meeting_id=meeting_id
            )
        else:
            thread_id = await self.thread_manager.upsert_thread(
                user_id=user_id,
                thread_name=thread_name,
                messages=messages,
                meeting_id=meeting_id
            )

        yield {
            "thread_id": thread_id,
            "service_content": {
                "context_source": "meeting",
                "meeting_count": len(authorized_meeting_ids),
                "meeting_id": str(authorized_meeting_ids[0]) if len(authorized_meeting_ids) == 1 else None
            }
        }