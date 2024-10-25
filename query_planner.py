from typing import List, Optional, AsyncGenerator
from datetime import datetime
from pydantic import Field
from core import BaseCall, Msg, generic_call_
import instructor
from openai import OpenAI

class SearchQuery(BaseCall):
    query: str = Field(description="Vector search query to find semantically relevant content")
    rationale: str = Field(description="Explanation of why this query is relevant")
    include_speakers: List[str] = Field(default_factory=list)
    exclude_speakers: List[str] = Field(default_factory=list)
    topic_focus: Optional[str] = Field(default=None)
    topic_type: Optional[str] = Field(default=None)

class QueryPlan(BaseCall):
    """A structured plan for executing multiple targeted searches"""
    strategy: str = Field(description="Overall search strategy explanation")
    queries: List[SearchQuery] = Field(description="List of search queries to execute")

# Create a Partial version of QueryPlan for streaming
PartialQueryPlan = instructor.Partial[QueryPlan]

async def generate_search_plan_stream(topics: List[str], topic_types: List[str], speakers: List[str], user_query: str) -> AsyncGenerator[QueryPlan, None]:
    """Generate a structured search plan that breaks down complex queries into multiple targeted searches"""
    
    messages = [
        Msg(role="system", content="""Break down complex search requests into multiple targeted queries.
        Return a JSON object with:
        {
            "strategy": "Overall search strategy explanation",
            "queries": [
                {
                    "query": "vector search query",
                    "rationale": "why this query is needed",
                    "include_speakers": ["speaker1"],
                    "exclude_speakers": [],
                    "topic_focus": "specific topic",
                    "topic_type": "topic type"
                }
            ]
        }"""),
        
        Msg(role="user", content=f"""
        Available Context:
        - Topics: {', '.join(topics)}
        - Topic Types: {', '.join(topic_types)}
        - Speakers: {', '.join(speakers)}
        
        User Query: {user_query}
        
        Break this down into multiple targeted search queries.""")
    ]
    
    # Use instructor's streaming with Partial model
    client = instructor.patch(OpenAI())
    async for partial_plan in client.chat.completions.create(

        response_model=PartialQueryPlan,
        messages=[msg.__dict__ for msg in messages],
        stream=True
    ):
        yield partial_plan

async def plan_search_queries(df, user_query: str, stream: bool = False) -> AsyncGenerator[QueryPlan, None]:
    """Create a search plan by breaking down a complex query into targeted searches"""
    
    # Get available metadata for filtering
    topics = df['topic_name'].unique().tolist()
    topic_types = df['topic_type'].unique().tolist()
    speakers = df['speaker_name'].unique().tolist()
    
    async for plan in generate_search_plan_stream(topics, topic_types, speakers, user_query):
        yield plan
