import asyncio
import re
from typing import List, Optional
from datetime import datetime

from fastapi import Depends
from pydantic import BaseModel, Field

from vexa import VexaAPI
from vector_search import VectorSearch
from core import system_msg, user_msg, assistant_msg, generic_call_stream, count_tokens, BaseCall
from prompts import Prompts
from pydantic_models import ThreadName, ContextQualityCheck
from thread_manager import ThreadManager
from core import generic_call_

class SearchResult(BaseModel):
    output: str
    messages: List[dict]
    meeting_ids: List[str]
    full_context: str
    thread_id: str
    thread_name: str
    indexed_meetings: dict
    linked_output: str


class SearchAssistant:
    def __init__(self):
        self.analyzer = VectorSearch()
        self.thread_manager = None  # Initialize to None
        self.prompts = Prompts()
        self.model = "gpt-4o-mini"
        self.indexing_jobs = {}

    async def initialize(self):
        self.thread_manager = await ThreadManager.create()  # Use the async create method

    async def chat(self, user_id: str, query: str, user_name: str='', thread_id: Optional[str] = None, model: Optional[str] = None, temperature: Optional[float] = None, debug: bool = False):
        if thread_id:
            thread = await self.thread_manager.get_thread(thread_id)
            if not thread:
                raise ValueError(f"Thread with id {thread_id} not found")
            messages = thread.messages
            thread_name = thread.thread_name
        else:
            messages = []
            thread_name = None

        query_ = ' '.join([m.content for m in messages]) + ' ' + query
        queries = await self.analyzer.generate_search_queries(query_, user_id=user_id, user_name=user_name)
        
        summaries = await self.analyzer.get_summaries(user_id=user_id, user_name=user_name)
        full_context, meeting_ids = await self.analyzer.build_context(queries, summaries, include_all_summaries=False, user_id=user_id, user_name=user_name, k=40)

        pref = "Based on the following context, answer the question:" if len(messages) == 0 else "Follow-up request:"
        user_info = f"The User is {user_name}"
        messages_context = [
            system_msg(self.prompts.perplexity + f'. {user_info}'), 
            user_msg(f"Context:\n{full_context}"),
        ] + messages + [user_msg(f"{pref} {query}. Always supply references to meetings as [1][2][3] etc.")]

        model_to_use = model or self.model

        output = ""
        async for chunk in generic_call_(messages_context, model=model_to_use, temperature=temperature, streaming=True):
            output += chunk
            yield chunk
        
        indexed_meetings = await self.get_indexed_meetings(meeting_ids, await self.parse_refs(output))
        url_dict = {k: f'https://dashboard.vexa.ai/#{v}' for k, v in indexed_meetings.items()}
        linked_output = await self.embed_links(output, url_dict)
        
        messages.append(user_msg(query))
        messages.append(assistant_msg(msg=linked_output, service_content=output))

        if not thread_id:
            messages_str = ';'.join([m.content for m in messages if m.role == 'user'])
            thread_name = await ThreadName.call([user_msg(messages_str)])
            thread_name = thread_name[0].thread_name
            thread_id = await self.thread_manager.upsert_thread(user_id=user_id, thread_name=thread_name, messages=messages)
        else:
            await self.thread_manager.upsert_thread(user_id=user_id, messages=messages, thread_id=thread_id)

        result = {
            "thread_id": thread_id,
            "linked_output": linked_output
        }

        if debug:
            result.update({
                "output": output,
                "summaries": summaries,
                "full_context": full_context,
                "meeting_ids": meeting_ids,
                "queries": queries,
            })

        yield result

    async def get_thread(self, thread_id: str):
        return await self.thread_manager.get_thread(thread_id)

    async def get_user_threads(self, user_id: str):
        return await self.thread_manager.get_user_threads(user_id)

    async def count_documents(self, user_id: str):
        return await self.analyzer.count_documents(user_id=user_id)

    async def get_messages_by_thread_id(self, thread_id: str):
        return await self.thread_manager.get_messages_by_thread_id(thread_id)

    async def delete_thread(self, thread_id: str) -> bool:
        return await self.thread_manager.delete_thread(thread_id)

    async def is_indexing(self, user_id: str) -> bool:
        return self.indexing_jobs.get(user_id, False)

    async def remove_user_data(self, user_id: str) -> int:
        return await self.analyzer.remove_user_data(user_id)

    # The following methods should be updated to be async if they involve I/O operations
    async def parse_refs(self, text):
        pattern = r'\[(\d+)\]'
        return list(set(re.findall(pattern, text)))

    async def get_indexed_meetings(self, meeting_ids, refs):
        indexed_meetings = {}
        for i, meeting_id in enumerate(meeting_ids):
            if str(i + 1) in refs:
                indexed_meetings[str(i + 1)] = meeting_id
        return indexed_meetings

    async def embed_links(self, text, url_dict):
        for key, url in url_dict.items():
            text = text.replace(f'[{key}]', f'[{key}]({url})')
        return text
