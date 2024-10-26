from typing import List, Optional, Set
from pydantic import BaseModel, Field
from core import BaseCall, system_msg, user_msg, assistant_msg
import pandas as pd
from datetime import datetime

class FollowUpSearch(BaseModel):
    query: str = Field(..., description="Follow-up search query")
    rationale: str = Field(..., description="Reason for this follow-up search")
    priority: int = Field(..., ge=1, le=5, description="Priority level (1-5)")
    focus_areas: List[str] = Field(..., description="Specific areas or topics to focus on")

class ReportSection(BaseModel):
    title: str = Field(..., description="Section title")
    content: str = Field(..., description="Section content")
    confidence: float = Field(..., ge=0, le=1, description="Confidence score for this section")
    sources: List[int] = Field(..., description="Source indices that support this section")

class SearchReport(BaseCall):
    """Generates structured report and follow-up searches from search results"""
    
    report_sections: List[ReportSection] = Field(
        ..., 
        description="Organized sections of the report"
    )
    
    key_findings: List[str] = Field(
        ..., 
        description="Main insights extracted from the search results"
    )
    
    information_gaps: List[str] = Field(
        ..., 
        description="Identified gaps in current information"
    )
    
    follow_up_searches: List[FollowUpSearch] = Field(
        ..., 
        max_items=5,
        description="Suggested follow-up searches to fill information gaps"
    )
    
    context_quality: float = Field(
        ..., 
        ge=0, 
        le=1,
        description="Overall quality score of available context"
    )

    @classmethod
    async def extract(cls, 
                     df: pd.DataFrame,
                     original_query: str,
                     previous_findings: List[str] = None,
                     explored_queries: Set[str] = None,
                     use_cache: bool = False) -> "SearchReport":
        
        # Initialize tracking of explored queries
        if explored_queries is None:
            explored_queries = set()
        if previous_findings is None:
            previous_findings = []
            
        # Format DataFrame for context
        context = df[['meeting_timestamp', 'speaker_name', 'topic_name', 'summary', 'details']].to_string()
        
        # Build cumulative context from previous findings
        cumulative_context = "\n\nPrevious Findings:\n" + "\n".join(previous_findings) if previous_findings else ""
        
        messages = [
            system_msg("""You are an analytical search assistant conducting an iterative research process. Your task is to:
            1. Analyze search results and create a structured report
            2. Identify information gaps
            3. Suggest strategic follow-up searches that build upon previous findings
            4. Rate information quality and confidence
            5. Ensure follow-up searches explore new angles while avoiding redundancy
            
            Be precise, evidence-based, and never speculate beyond available data."""),
            
            user_msg(f"""Original Query: {original_query}

Search Results Context:
{context}
{cumulative_context}

Previously Explored Queries: {', '.join(explored_queries) if explored_queries else 'None'}

Generate a comprehensive report with strategic follow-up searches that build upon our current understanding.""")
        ]
        
        return await cls.call(messages, model="default", use_cache=use_cache)

async def conduct_iterative_research(
    search_engine,
    initial_query: str,
    max_iterations: int = 5,
    min_context_quality: float = 0.8,
    verbose: bool = True
) -> List[SearchReport]:
    """
    Conduct iterative research by following up on gaps and building context
    
    Args:
        search_engine: Search engine instance
        initial_query: Starting search query
        max_iterations: Maximum number of search iterations
        min_context_quality: Minimum context quality score to achieve
        verbose: Whether to print detailed progress
    """
    reports = []
    explored_queries = set([initial_query])
    cumulative_findings = []
    todo_queries = [initial_query]
    
    if verbose:
        print("\n=== Starting Iterative Research ===")
        print(f"Initial Query: {initial_query}")
        print(f"Max Iterations: {max_iterations}")
        print(f"Target Context Quality: {min_context_quality}\n")
    
    while len(reports) < max_iterations and todo_queries:
        current_query = todo_queries.pop(0)
        
        if verbose:
            print(f"\n--- Iteration {len(reports) + 1}/{max_iterations} ---")
            print(f"Current Query: {current_query}")
            print("Executing search...")
        
        # Execute search
        results = search_engine.search(
            query=current_query,
            k=200,
            min_similarity=0.49,
            exact_match_boost=0.3,
            return_scores=True
        )
        
        if verbose:
            print(f"Found {len(results)} results")
            print("Generating report...")
        
        # Generate report with cumulative context
        report = await SearchReport.extract(
            df=results,
            original_query=current_query,
            previous_findings=cumulative_findings,
            explored_queries=explored_queries
        )
        
        reports.append(report)
        
        if verbose:
            print("\nReport Summary:")
            print(f"- Context Quality: {report.context_quality:.2f}")
            print(f"- Key Findings: {len(report.key_findings)}")
            print(f"- Information Gaps: {len(report.information_gaps)}")
            print(f"- Follow-up Searches: {len(report.follow_up_searches)}")
            
            print("\nKey Findings:")
            for i, finding in enumerate(report.key_findings, 1):
                print(f"{i}. {finding}")
        
        # Update cumulative findings
        new_findings = [f for f in report.key_findings if f not in cumulative_findings]
        cumulative_findings.extend(new_findings)
        
        if verbose and new_findings:
            print(f"\nAdded {len(new_findings)} new findings to cumulative context")
        
        # Add new follow-up searches to todo list, avoiding duplicates
        new_queries = [
            search.query for search in report.follow_up_searches
            if search.query not in explored_queries
            and search.priority >= 3  # Only follow high-priority leads
        ]
        
        if verbose and new_queries:
            print("\nNew High-Priority Follow-up Queries:")
            for i, query in enumerate(new_queries, 1):
                matching_search = next(s for s in report.follow_up_searches if s.query == query)
                print(f"{i}. Query: {query}")
                print(f"   Priority: {matching_search.priority}")
                print(f"   Rationale: {matching_search.rationale}")
        
        todo_queries.extend(new_queries)
        explored_queries.update(new_queries)
        
        # Check if we've reached sufficient context quality
        if report.context_quality >= min_context_quality:
            if verbose:
                print(f"\n✓ Reached target context quality: {report.context_quality:.2f}")
            break
            
        if verbose:
            print(f"\nRemaining queries to explore: {len(todo_queries)}")
            print("-" * 50)
    
    if verbose:
        print("\n=== Research Summary ===")
        print(f"Completed Iterations: {len(reports)}")
        print(f"Total Queries Explored: {len(explored_queries)}")
        print(f"Final Context Quality: {reports[-1].context_quality:.2f}")
        print(f"Total Findings: {len(cumulative_findings)}")
        print("=" * 50)
    
    return reports
