from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from pydantic_ai import Agent, RunContext, Tool
from hybrid_search import hybrid_search as _hybrid_search
from qdrant_search import QdrantSearchEngine
from bm25_search import ElasticsearchBM25
class HybridSearchInput(BaseModel):
    query: str = Field(..., description="The search query to find relevant content")
    meeting_ids: Optional[List[str]] = Field(None, description="Optional list of meeting IDs to filter search results")
    speakers: Optional[List[str]] = Field(None, description="Optional list of speaker names to filter results")
    k: int = Field(100, description="Number of results to return")
class HybridSearchResult(BaseModel):
    results: List[Dict[str, Any]] = Field(..., description="Search results containing relevant content")
    total_found: int = Field(..., description="Total number of results found")
class HybridSearchTool:
    def __init__(self, agent: Agent, qdrant_engine: QdrantSearchEngine, es_engine: ElasticsearchBM25):
        self.agent = agent
        self.qdrant_engine = qdrant_engine
        self.es_engine = es_engine
        # Register the tool with the agent
        self.setup_tool()
    def setup_tool(self):
        @self.agent.tool
        async def hybrid_search(ctx: RunContext, input: HybridSearchInput) -> HybridSearchResult:
            """Search through meeting content using both semantic and keyword search."""
            results = await _hybrid_search(
                query=input.query,
                qdrant_engine=self.qdrant_engine,
                es_engine=self.es_engine,
                meeting_ids=input.meeting_ids,
                speakers=input.speakers,
                k=input.k
            )
            return HybridSearchResult(
                results=results["results"],
                total_found=len(results["results"])
            ) 