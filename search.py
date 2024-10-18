import asyncio
import re
from typing import List, Optional
from datetime import datetime

from fastapi import Depends
from pydantic import BaseModel, Field

from vexa import VexaAPI
from meeting_search import VectorSearch
from core import system_msg, user_msg, assistant_msg, generic_call_stream, count_tokens, BaseCall
from prompts import Prompts
from pydantic_models import ThreadName, ContextQualityCheck
from assistant_thread import ThreadManager
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
    def __init__(self, user_token: Optional[str] = None, model: str = "gpt-4o-mini"):
        self.vexa = VexaAPI(user_token) if user_token else VexaAPI()
        self.analyzer = VectorSearch()
        self.thread_manager = ThreadManager()
        self.prompts = Prompts()
        self.model = model

    async def chat(self, query: str, thread_id: Optional[str] = None, model: Optional[str] = None, temperature: Optional[float] = None):
        user_id = self.vexa.user_id
        user_name = self.vexa.user_name

        if thread_id:
            thread = self.thread_manager.get_thread(thread_id)
            if not thread:
                raise ValueError(f"Thread with id {thread_id} not found")
            messages = thread.messages
            thread_name = thread.thread_name
        else:
            messages = []
            thread_name = None

        query_ = ' '.join([m.content for m in messages]) + ' ' + query
        queries = await self.analyzer.generate_search_queries(query_, user_id=user_id, user_name=user_name)
        
        
        summaries = self.analyzer.get_summaries(user_id=user_id, user_name=user_name)
        full_context, meeting_ids = await self.analyzer.build_context(queries, summaries, include_all_summaries=False, user_id=user_id, user_name=user_name,k=40)

        pref = "Based on the following context, answer the question:" if len(messages) == 0 else "Follow-up request:"
        user_info = f"The User is {user_name}"
        messages_context = [
            system_msg(self.prompts.perplexity + f'. {user_info}'), 
            user_msg(f"Context:\n{full_context}"),
        ] + messages + [user_msg(f"{pref} {query}. ALways suplay references to meetings as [1][2][3] etc.")]

        # Use the provided model if given, otherwise use the default model
        model_to_use = model or self.model

        output = ""
        async for chunk in generic_call_(messages_context, model=model_to_use, temperature=temperature, streaming=True):
            output += chunk
            yield chunk
        
        messages.append(user_msg(query))
        messages.append(assistant_msg(output))

        # New code for indexing meetings and embedding links
        indexed_meetings = self.get_indexed_meetings(meeting_ids, self.parse_refs(output))
        url_dict = {k: f'https://dashboard.vexa.ai/#{v}' for k, v in indexed_meetings.items()}
        linked_output = self.embed_links(output, url_dict)

        if not thread_id:
            messages_str = ';'.join([m.content for m in messages if m.role == 'user'])
            thread_name = await ThreadName.call([user_msg(messages_str)])
            thread_name = thread_name[0].thread_name
            thread_id = self.thread_manager.create_thread(user_id=user_id, thread_name=thread_name, messages=messages)
        else:
            self.thread_manager.update_thread(thread_id, messages)

        yield SearchResult(
            output=output,
            messages=[m.__dict__ for m in messages],
            meeting_ids=meeting_ids,
            full_context=full_context,
            thread_id=thread_id,
            thread_name=thread_name,
            indexed_meetings=indexed_meetings,
            linked_output=linked_output
        )

    def get_thread(self, thread_id: str):
        return self.thread_manager.get_thread(thread_id)

    def get_user_threads(self):
        return self.thread_manager.get_user_threads(self.vexa.user_id)

    def count_documents(self):
        return self.analyzer.count_documents(user_id=self.vexa.user_id)

    def get_messages_by_thread_id(self, thread_id: str):
        return self.thread_manager.get_messages_by_thread_id(thread_id)

    def parse_refs(self, text: str) -> List[int]:
        return [int(num) for num in re.findall(r'\[(\d+)\]', text)]

    def get_indexed_meetings(self, meeting_ids: List[str], indexes: List[int]) -> dict:
        return {i: meeting_ids[i-1] for i in indexes if i <= len(meeting_ids)}

    def embed_links(self, text: str, url_dict: dict) -> str:
        def replace_link(match):
            numbers = match.group(1).split('][')
            linked_numbers = []
            for number in numbers:
                if int(number) in url_dict:
                    linked_numbers.append(f'[{number}]({url_dict[int(number)]})')
                else:
                    linked_numbers.append(f'[{number}]')
            return ' '.join(linked_numbers)
        
        pattern = r'\[(\d+(?:\]\[\d+)*)\]'
        return re.sub(pattern, replace_link, text)
