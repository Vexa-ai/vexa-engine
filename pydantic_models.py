from pydantic import BaseModel, Field
from typing import List, Optional
from core import BaseCall
from datetime import datetime

class Point(BaseModel):
    c: str = Field(..., description="Point content mentioning speakers, facts, main ideas, and concise information")
    s: str = Field(..., description="Start index")
    e: str = Field(..., description="End index")

class Summary(BaseCall):
    """points must cover all the qoutes in the transcript"""
    thinking: str = Field(..., description="full log of your thought process about how you are to create a summary and points that cover all the quotes in the transcript")
    meeting_name: str = Field(..., max_length=50, description="Explanotory consice dense name of the meeting, no generic words, 50 char max")
    summary: str = Field(..., max_length=500, description="Concise summary of the text with attention to company facts, names, people, dates, numbers, and facts")
    points: List[Point] = Field(..., description="Main points as bullets, each covering a specific qoute range in the transcript")

class Query(BaseCall):
    """make specifc vector_search_queries based on general context"""
    start: Optional[datetime] = Field(None, description="Start time")
    end: Optional[datetime] = Field(None, description="End time")
    vector_search_query: Optional[str] = Field(..., description="""specific search query that contained in the generalcontext directly""")
    
class QueryPlan(BaseCall):
    queries: List[Query] = Field(..., description="List of requests, aim for at least 2 queries, all aparms optional")
    
class ThreadName(BaseCall):
    thread_name: str = Field(..., max_length=50, description="Condence into explanatory concise dense name of the meeting, 50 char max reusing same wording")

