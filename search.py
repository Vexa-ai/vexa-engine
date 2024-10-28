import asyncio
import re
from typing import List, Optional
from datetime import datetime

import pandas as pd
import numpy as np
from pydantic import BaseModel, Field

from qdrant_search import QdrantSearchEngine
from core import system_msg, user_msg, assistant_msg, generic_call_stream, count_tokens, BaseCall, generic_call_
from prompts import Prompts
from pydantic_models import ThreadName
from thread_manager import ThreadManager


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
        self.search_engine = QdrantSearchEngine()
        self.thread_manager = None
        self.prompts = Prompts()
        self.model = "gpt-4o-mini"
        self.indexing_jobs = {}
        
    async def initialize(self):
        self.thread_manager = await ThreadManager.create()

    # Thread management methods
    async def get_thread(self, thread_id: str):
        return await self.thread_manager.get_thread(thread_id)

    async def get_user_threads(self, user_id: str):
        return await self.thread_manager.get_user_threads(user_id)

    async def get_messages_by_thread_id(self, thread_id: str):
        return await self.thread_manager.get_messages_by_thread_id(thread_id)

    async def delete_thread(self, thread_id: str) -> bool:
        return await self.thread_manager.delete_thread(thread_id)

    # Job management method
    async def is_indexing(self, user_id: str) -> bool:
        return self.indexing_jobs.get(user_id, False)

    def normalize_series(self, series: pd.Series) -> pd.Series:
        min_value = series.min()
        max_value = series.max()
        normalized = (series - min_value) / (max_value - min_value)
        return normalized * 0.5 + series.min() # Scale to range [0.5, 1]

    async def search(self, query: str, limit: int = 200, min_score: float = 0.4) -> pd.DataFrame:
        # Get search results
        main_results = await self.search_engine.search(
            query_text=query,
            limit=limit,
            min_score=min_score,
        )
        

        speaker_results = await self.search_engine.search_by_speaker(
            speaker_query=query,
            limit=limit,
            min_score=min_score
        )

        # Process results into DataFrames
        main_df = pd.DataFrame(main_results) if main_results else pd.DataFrame()
        speaker_df = pd.DataFrame(speaker_results) if speaker_results else pd.DataFrame()
    

        # Select relevant columns and combine results
        columns = ['topic_name', 'speaker_name', 'summary', 'details', 'meeting_id', 'timestamp']
        score_columns = ['score', 'vector_scores', 'exact_matches']

        if len(main_df) > 0:
            main_df = main_df[columns + score_columns]
            main_df['source'] = 'main'
        else:
            main_df = pd.DataFrame(columns=columns + score_columns + ['source'])

        if len(speaker_df) > 0:
            speaker_df = speaker_df[columns + ['score']]  # Speaker search has simpler scoring
            speaker_df['source'] = 'speaker'
        else:
            speaker_df = pd.DataFrame(columns=columns + ['score', 'source'])

        # Combine, deduplicate and sort results
        results = pd.concat([main_df, speaker_df]).drop_duplicates(subset=columns).reset_index(drop=True)
        if not results.empty:
            results = results.sort_values('score', ascending=False)
            
        return results

    def prep_context(self, search_results: pd.DataFrame) -> tuple[str, dict]:
        search_results['relevance_score'] = self.normalize_series(search_results['score']).round(2)
        search_results = search_results.sort_values('timestamp').reset_index(drop=True)
        search_results['datetime'] = pd.to_datetime(search_results['timestamp'], format='mixed').dt.strftime('%A %Y-%m-%d %H:%M')

        meetinds_df = search_results[['meeting_id']].drop_duplicates().reset_index(drop=True)
        meetinds_df['meeting_index'] = meetinds_df.index + 1
        prepared_df = search_results.merge(meetinds_df, on='meeting_id')
        
        meetings = meetinds_df.to_dict(orient='records')
        
        return prepared_df,{meeting['meeting_index']: meeting['meeting_id'] for meeting in meetings}

    async def embed_links(self, text: str, url_dict: dict) -> str:
        # First, add a space between consecutive reference numbers
        text = re.sub(r'\]\[', '] [', text)
        
        # Then replace each reference with its link
        for key, url in url_dict.items():
            text = text.replace(f'[{key}]', f'[{key}]({url})')
        return text

    async def chat(self, user_id: str, query: str, user_name: str='', thread_id: Optional[str] = None, model: Optional[str] = None, temperature: Optional[float] = None):
        # Get thread info
        if thread_id:
            thread = await self.thread_manager.get_thread(thread_id)
            if not thread:
                raise ValueError(f"Thread with id {thread_id} not found")
            messages = thread.messages
            thread_name = thread.thread_name
        else:
            messages = []
            thread_name = None

        # Get search results and yield them immediately
        search_results = await self.search(query)
        
        # Prepare context
        
        prepared_df, indexed_meetings = self.prep_context(search_results)
    
        
        # Calculate weighted average score for each meeting, giving more weight to higher scores
        meeting_groups = search_results.sort_values('relevance_score', ascending=False).groupby('meeting_id').agg({
            'relevance_score': lambda x: np.average(x, weights=np.exp2(x)),  # Exponential weighting
            'topic_name': 'first',
            'speaker_name': set,
            'timestamp': 'first'
        }).sort_values('relevance_score', ascending=False).reset_index().head(20)
        meeting_groups['url'] = meeting_groups['meeting_id'].apply(lambda meeting_id: f'https://dashboard.vexa.ai/#{meeting_id}')
        meeting_groups = meeting_groups.drop(columns=['meeting_id'])
        meeting_groups['speaker_name'] = meeting_groups['speaker_name'].apply(list)
        meeting_groups = meeting_groups.to_dict(orient='records')
        
        yield {"search_results": meeting_groups}
        
        context_prepared = prepared_df.drop(columns=['timestamp', 'vector_scores', 'exact_matches', 'source', 'score','meeting_id'])
        context = context_prepared.to_markdown(index=False) if not prepared_df.empty else "No relevant context found."
        url_dict = {k: f'https://dashboard.vexa.ai/#{v}' for k, v in indexed_meetings.items()}

        # Build messages
        context_msg = system_msg(f"Context: {context}")
        messages_context = [
            system_msg(self.prompts.perplexity),
            *messages,
            context_msg,
            user_msg(query)
        ]

        # Generate response
        output = ""
        async for chunk in generic_call_(messages_context, streaming=True):
            output += chunk
            yield chunk
            
        linked_output = await self.embed_links(output, url_dict)
        messages.append(user_msg(query))
        service_content = {
            'output': output,
            'search_results': meeting_groups
        }
        messages.append(assistant_msg(msg=linked_output, service_content=service_content))

        # Handle thread creation/update
        if not thread_id:
            thread_name = query
            thread_id = await self.thread_manager.upsert_thread(user_id=user_id, thread_name=thread_name, messages=messages)
        else:
            await self.thread_manager.upsert_thread(user_id=user_id, messages=messages, thread_id=thread_id)

        result = {
            "thread_id": thread_id,
            "linked_output": linked_output
        }   
        yield result
