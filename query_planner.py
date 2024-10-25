from typing import List, Optional, AsyncGenerator
from datetime import datetime
from pydantic import Field
from core import BaseCall, Msg
import instructor
from openai import AsyncOpenAI
import pandas as pd
from typing import Dict
from sampling import WeightedSampler
class SearchQuery(BaseCall):
    """at least one of query or topic_focus must be provided"""
    query: str = Field(
        description="""
        A specific, focused search query to find semantically relevant content. 
        Must be a natural language query that clearly describes what information to find.
        Should be detailed enough to capture the intended search goal.
        """
    )
    
    include_speakers: List[str] = Field(
        description="""
        List of specific speakers whose input should be prioritized in the search results.
        Used to focus on particular perspectives or sources of information.
        """
    )
    
    exclude_speakers: List[str] = Field(
        description="""
        List of speakers whose input should be filtered out from the search results.
        Used to remove irrelevant or unwanted perspectives.
        """
    )
    
    topic_focus: List[str] = Field(
        description="""
        List of specific topic areas to narrow down the search scope. Choose from the list. Should align with 
        available topic categories in the data.
        """
    )
    
    topic_type: List[str] = Field(
        description="""
        List of topic types to filter by. Choose from the list.  Should match the available topic type categories 
        in the dataset
        """
    )

class QueryPlan(BaseCall):
    """A structured plan for executing multiple targeted searches"""
    queries: List[SearchQuery] = Field(description="List of search queries to execute, at least 3")

async def generate_search_plan_stream(topics: List[str], topic_types: List[str], speakers: List[str], user_query: str, context: str) -> AsyncGenerator[QueryPlan, None]:
    """Generate a structured search plan that breaks down complex queries into multiple targeted searches"""
    
    messages = [
        Msg(role="system", content="""Break down complex search requests into multiple targeted queries."""),
        
        Msg(role="user", content=f"""
        {context}
        
        User Query: {user_query}
        
        Break this down into multiple targeted search queries.""")
    ]
    
    # Use BaseCall.call with streaming
    async for partial_plan in QueryPlan.call(
        messages=messages,
        model="default",
        temperature=0,
        stream=True
    ):
        if partial_plan is not None:
            yield partial_plan

async def plan_search_queries(df,user_query: str,sampler: WeightedSampler) -> AsyncGenerator[QueryPlan, None]:
    """Create a search plan by breaking down a complex query into targeted searches"""
    
    # Get sample of recent data
    sampled_df = sampler.sample(
        query=user_query,
        n_samples=100,  # Sample 100 recent entries
        mode='recency',  # Focus on recency for initial context
        recency_weight=1.0,
        similarity_weight=0.0
    )
    
    # Get available metadata for filtering from sampled data
    topics = df['topic_name'].unique().tolist()
    topic_types = df['topic_type'].unique().tolist()
    speakers = df['speaker_name'].unique().tolist()
    
    # Create context from sampled data
    context = f"""
    Sample Context (from {len(sampled_df)} recent entries):
    - Topics: {', '.join(topics)}
    - Topic Types: {', '.join(topic_types)}
    - Speakers: {', '.join(speakers)}
    - Date Range: {sampled_df['meeting_timestamp'].min().date()} to {sampled_df['meeting_timestamp'].max().date()}
    
    Sample Entries:
    {sampled_df[['topic_name', 'topic_type', 'summary']].head(3).to_string()}
    """
    
    async for plan in generate_search_plan_stream(topics, topic_types, speakers, user_query, context):
        yield plan



async def execute_search_plan(df: pd.DataFrame, plan: QueryPlan) -> Dict[str, pd.DataFrame]:
    """Execute the search plan and return filtered DataFrames for each query"""
    results = []
    
    for query in plan.queries:
        # Create a copy of the DataFrame to apply filters
        filtered_df = df.copy()
        
        # Apply topic focus filter if specified
        if query.topic_focus:
            topic_conditions = [
                filtered_df['topic_name'].str.contains(topic, case=False, na=False) |
                filtered_df['topic_type'].str.contains(topic, case=False, na=False)
                for topic in query.topic_focus
            ]
            if topic_conditions:
                filtered_df = filtered_df[pd.concat(topic_conditions, axis=1).any(axis=1)]
        
        # Apply topic type filter if specified
        if query.topic_type:
            type_conditions = [
                filtered_df['topic_type'].str.contains(topic_type, case=False, na=False)
                for topic_type in query.topic_type
            ]
            if type_conditions:
                filtered_df = filtered_df[pd.concat(type_conditions, axis=1).any(axis=1)]
        
        # Apply speaker inclusion filter if specified
        if query.include_speakers:
            filtered_df = filtered_df[filtered_df['speaker_name'].isin(query.include_speakers)]
        
        # Apply speaker exclusion filter if specified
        if query.exclude_speakers:
            filtered_df = filtered_df[~filtered_df['speaker_name'].isin(query.exclude_speakers)]
        
        # Only use sampler if we have a valid query string
        if query.query and query.query.strip():
            sampler = WeightedSampler(
                df=filtered_df,
                text_columns=['topic_name', 'topic_type', 'summary'],
                date_column='meeting_timestamp',
                decay_factor=0.1
            )
            
            filtered_df = sampler.sample(
                query=query.query,
                n_samples=50,
                mode='combined',
                recency_weight=0.3,
                similarity_weight=0.7
            )
        
        # Store results with query rationale as key
        results.append({'df':filtered_df,'query':query})
    
    return results

# Usage example
async def process_and_execute_query(df: pd.DataFrame, user_query: str):
    async for plan in plan_search_queries(df, user_query):
        if hasattr(plan, 'strategy') and plan.strategy:
            if hasattr(plan, 'queries') and plan.queries:
                print(plan.model_dump())
                results = await execute_search_plan(df, plan)
                return results
