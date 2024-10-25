from typing import List, Optional, Dict, Set
from pydantic import BaseModel, Field
from datetime import datetime
from sampling import fetch_joined_data, WeightedSampler
from core import BaseCall, system_msg, user_msg, generic_call_stream
import pandas as pd
import numpy as np

class SearchResult(BaseModel):
    answer: str = Field(..., description="Direct answer to the query")
    confidence: float = Field(..., ge=0, le=1, description="Confidence score (0-1)")
    sources: List[Dict] = Field(..., description="Source information for the answer")

class Conversation:
    def __init__(self, query: str):
        self.original_query = query
        self.history: List[Dict] = []
        self.sampled_data: Optional[pd.DataFrame] = None
        
    def add_interaction(self, query: str, result: SearchResult):
        self.history.append({
            'query': query,
            'answer': result.answer,
            'timestamp': datetime.now().isoformat()
        })
    
    def get_context(self) -> str:
        context = f"Original Query: {self.original_query}\n\n"
        if self.history:
            context += "Recent History:\n"
            for interaction in self.history[-2:]:
                context += f"Q: {interaction['query']}\n"
                context += f"A: {interaction['answer']}\n\n"
        return context

class QueryAgent:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.conversations: Dict[str, Conversation] = {}
        
        # Analyze speaker patterns to identify internal team members
        self.internal_speakers = self._identify_internal_speakers(df)
        
        # Initialize sampler
        self.sampler = WeightedSampler(
            df=df,
            text_columns=['summary', 'details', 'topic_name', 'topic_type', 'referenced_text'],
            date_column='meeting_time',
            decay_factor=0.05
        )
    
    def _identify_internal_speakers(self, df: pd.DataFrame) -> Set[str]:
        """Identify internal team members based on participation patterns"""
        speaker_stats = pd.DataFrame({
            # Frequency of participation
            'meeting_count': df.groupby('speaker_name').size(),
            # Time span of participation
            'participation_span': df.groupby('speaker_name').agg({
                'meeting_time': lambda x: (pd.to_datetime(x.max()) - pd.to_datetime(x.min())).days
            })['meeting_time'],
            # Diversity of topics discussed
            'topic_diversity': df.groupby('speaker_name')['topic_name'].nunique(),
            # Initiative in discussions (starting topics)
            'topic_initiations': df.groupby('speaker_name')['topic_type'].apply(
                lambda x: (x == 'initiative').sum()
            )
        })
        
        # Calculate composite score for "internality"
        speaker_stats['internal_score'] = (
            # Normalize each metric
            (speaker_stats['meeting_count'] / speaker_stats['meeting_count'].max()) * 0.4 +
            (speaker_stats['participation_span'] / speaker_stats['participation_span'].max()) * 0.3 +
            (speaker_stats['topic_diversity'] / speaker_stats['topic_diversity'].max()) * 0.2 +
            (speaker_stats['topic_initiations'] / speaker_stats['topic_initiations'].max().clip(1)) * 0.1
        )
        
        # Identify internal speakers using statistical threshold
        threshold = speaker_stats['internal_score'].mean() + speaker_stats['internal_score'].std()
        internal_speakers = set(speaker_stats[speaker_stats['internal_score'] > threshold].index)
        
        return internal_speakers

    async def search(self, query: str, conversation_id: Optional[str] = None) -> tuple[SearchResult, str]:
        conversation = self._get_or_create_conversation(query, conversation_id)
        
        # 1. Iterative Search Phase
        search_results = await self._iterative_search(query, conversation)
        
        # 2. Evidence Collection
        evidence = self._collect_evidence(search_results)
        
        # 3. Answer Generation
        result = await self._generate_answer(query, evidence, conversation)
        
        conversation.add_interaction(query, result)
        return result, conversation_id

    async def _iterative_search(self, query: str, conversation: Conversation) -> pd.DataFrame:
        """Improved search strategy focusing on relevance over frequency"""
        results = []
        
        # 1. Initial semantic search with high similarity weight
        initial_results = self.sampler.sample(
            query=query,
            n_samples=50,
            mode='similarity',  # Pure similarity for initial search
            similarity_weight=1.0
        )
        results.append(initial_results)
        
        # 2. Search specifically for user feedback and issues
        if any(term in query.lower() for term in ['user', 'feedback', 'issue', 'problem', 'bug']):
            # Look for discussions from external users or about user experience
            user_feedback_mask = (
                ~self.df['speaker_name'].isin(['Dmitriy Grankin', 'Olga Nemirovskaya']) |  # External speakers
                self.df['topic_type'].str.contains('feedback|issue|bug', case=False, na=False) |  # Feedback topics
                self.df['summary'].str.contains('user|customer|client', case=False, na=False)  # User-related content
            )
            
            # Create a new sampler for the filtered data
            user_feedback_sampler = WeightedSampler(
                df=self.df[user_feedback_mask],
                text_columns=['summary', 'details', 'topic_name', 'topic_type', 'referenced_text'],
                date_column='meeting_time',
                decay_factor=0.05
            )
            
            feedback_results = user_feedback_sampler.sample(
                query=query,
                n_samples=30,
                mode='combined',
                recency_weight=0.3,
                similarity_weight=0.7
            )
            results.append(feedback_results)
        
        # 3. Topic-based search
        found_topics = set(initial_results['topic_name'].unique())
        for topic in found_topics:
            if pd.notna(topic):
                topic_results = self.sampler.sample(
                    query=f"{query} {topic}",
                    n_samples=20,
                    mode='similarity',
                    similarity_weight=1.0
                )
                results.append(topic_results)
        
        # Combine results and remove duplicates
        combined_df = pd.concat(results).drop_duplicates(subset=['meeting_id', 'summary_index'])
        
        # Sort by relevance and ensure diversity of speakers
        combined_df['speaker_frequency'] = combined_df['speaker_name'].map(
            combined_df['speaker_name'].value_counts()
        )
        combined_df['final_score'] = (
            combined_df['combined_score'] * 
            (1 / np.log2(combined_df['speaker_frequency'] + 2))  # Boost less frequent speakers
        )
        
        return combined_df.sort_values('final_score', ascending=False)

    def _collect_evidence(self, search_results: pd.DataFrame) -> List[Dict]:
        """Collect and structure evidence from search results"""
        evidence = []
        
        # Group by topic for better context
        for topic in search_results['topic_name'].unique():
            topic_data = search_results[search_results['topic_name'] == topic]
            
            # Collect all relevant points for the topic
            topic_evidence = {
                'topic': topic,
                'points': [],
                'speakers': set(),
                'dates': set(),
                'relevance': topic_data['combined_score'].mean()
            }
            
            for _, row in topic_data.iterrows():
                topic_evidence['points'].append({
                    'summary': row['summary'],
                    'details': row['details'],
                    'speaker': row['speaker_name'],
                    'date': row['meeting_time'],
                    'score': row['combined_score']
                })
                topic_evidence['speakers'].add(row['speaker_name'])
                topic_evidence['dates'].add(row['meeting_time'])
            
            evidence.append(topic_evidence)
        
        return sorted(evidence, key=lambda x: x['relevance'], reverse=True)

    async def _generate_answer(self, query: str, evidence: List[Dict], conversation: Conversation) -> SearchResult:
        """Generate comprehensive answer using collected evidence"""
        context = conversation.get_context()
        
        # Group evidence by speaker type
        external_feedback = []
        internal_discussions = []
        
        for topic_evidence in evidence:
            for point in topic_evidence['points']:
                if point['speaker'] not in self.internal_speakers:
                    external_feedback.append(point)
                else:
                    internal_discussions.append(point)
        
        # Format evidence for LLM
        evidence_text = self._format_evidence(external_feedback, internal_discussions)
        
        output = await generic_call_stream([
            system_msg("""You are a helpful AI assistant that provides detailed answers about meeting discussions.
                      Focus on synthesizing information across multiple topics and sources.
                      Prioritize feedback from external users and customers.
                      Highlight agreements and disagreements between different perspectives."""),
            user_msg(f"""Previous Context:
{context}

Current Query: {query}

{evidence_text}

Provide a comprehensive answer that:
1. Prioritizes external user feedback and experiences
2. Synthesizes information across different sources
3. Notes temporal patterns if relevant
4. Cites specific sources and speakers""")
        ])
        
        # Format sources
        formatted_sources = self._format_sources(evidence)
        
        return SearchResult(
            answer=output,
            confidence=np.mean([e['relevance'] for e in evidence]),
            sources=formatted_sources
        )

    def _format_evidence(self, external_feedback: List[Dict], internal_discussions: List[Dict]) -> str:
        """Format evidence for LLM consumption"""
        evidence_text = "Evidence by Category:\n\n"
        
        if external_feedback:
            evidence_text += "External User Feedback:\n"
            for point in external_feedback:
                evidence_text += f"- {point['date']} | {point['speaker']}: {point['summary']}\n"
                if point.get('details'):
                    evidence_text += f"  Details: {point['details']}\n"
            evidence_text += "\n"
        
        if internal_discussions:
            evidence_text += "Related Internal Discussions:\n"
            for point in internal_discussions[:5]:
                evidence_text += f"- {point['date']} | {point['speaker']}: {point['summary']}\n"
        
        return evidence_text

    def _format_sources(self, evidence: List[Dict]) -> List[Dict]:
        """Format sources with speaker type information"""
        formatted_sources = []
        for e in evidence:
            source_info = {
                'topic': e['topic'],
                'speaker_type': 'external' if any(s not in self.internal_speakers for s in e['speakers']) else 'internal',
                'speakers': list(e['speakers']),
                'dates': list(e['dates']),
                'date': min(e['dates']),
                'summary': e.get('summary', ''),
                'details': e.get('details', ''),
                'relevance': e['relevance']
            }
            formatted_sources.append(source_info)
        return formatted_sources

    def _get_or_create_conversation(self, query: str, conversation_id: Optional[str]) -> Conversation:
        """Get existing conversation or create new one"""
        if conversation_id is None:
            conversation_id = f"conv_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            self.conversations[conversation_id] = Conversation(query)
        elif conversation_id not in self.conversations:
            raise ValueError(f"Conversation {conversation_id} not found")
        return self.conversations[conversation_id]

async def init_agent() -> QueryAgent:
    """Initialize the query agent with data"""
    df = await fetch_joined_data()
    df['meeting_time'] = pd.to_datetime(df['meeting_timestamp']).dt.strftime('%Y-%m-%d')
    return QueryAgent(df)

# Example usage in notebook:
"""
from query_agent import init_agent

# Initialize
agent = await init_agent()

# First query
result, conv_id = await agent.search("what do users say about vexa?")
print(result.answer)

# Follow-up query
follow_up, _ = await agent.search("what specific issues were mentioned?", conv_id)
print(follow_up.answer)
"""
