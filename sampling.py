from sentence_transformers import SentenceTransformer
import numpy as np
import pandas as pd
import faiss
from typing import List, Union, Optional

class VectorStore:
    def __init__(self, model_name: str = 'all-MiniLM-L6-v2'):
        """Initialize vector store with specified model"""
        self.model = SentenceTransformer(model_name)
        self.index: Optional[faiss.IndexFlatL2] = None
        self.embeddings: Optional[np.ndarray] = None
        
    def create_embeddings(self, texts: List[str], batch_size: int = 32) -> np.ndarray:
        """Create embeddings for a list of texts"""
        return self.model.encode(
            texts,
            show_progress_bar=True,
            batch_size=batch_size
        )
    
    def build_index(self, texts: List[str]):
        """Build FAISS index from texts"""
        self.embeddings = self.create_embeddings(texts)
        self.index = faiss.IndexFlatL2(self.embeddings.shape[1])
        self.index.add(self.embeddings)
        
    def get_similarities(self, query: str) -> np.ndarray:
        """Get similarity scores for query against indexed texts"""
        if self.index is None or self.embeddings is None:
            raise ValueError("Index not built. Call build_index first.")
            
        query_embedding = self.model.encode([query])
        similarities = np.dot(self.embeddings, query_embedding.T).squeeze()
        
        # Normalize to [0, 1]
        similarities = (similarities - similarities.min()) / (similarities.max() - similarities.min())
        return similarities

class RecencyCalculator:
    def __init__(self, decay_factor: float = 0.1):
        """Initialize recency calculator with decay factor"""
        self.decay_factor = decay_factor
        
    def calculate_recency_scores(self, dates: Union[pd.Series, List]) -> np.ndarray:
        """Calculate recency scores using exponential decay"""
        dates = pd.to_datetime(dates)
        most_recent = dates.max()
        days_ago = (most_recent - dates).dt.days
        
        scores = np.exp(-self.decay_factor * days_ago)
        return scores / scores.sum()

class WeightedSampler:
    def __init__(self, 
                 df: pd.DataFrame,
                 text_columns: Optional[List[str]] = None,
                 date_column: str = 'meeting_time',
                 model_name: str = 'all-MiniLM-L6-v2',
                 decay_factor: float = 0.1):
        """Initialize sampler with DataFrame and parameters"""
        # Reset index to ensure alignment
        self.df = df.reset_index(drop=True)
        self.text_columns = text_columns
        self.date_column = date_column
        
        # Initialize components
        self.recency_calc = RecencyCalculator(decay_factor)
        
        # Only initialize vector store if text columns are provided
        self.vector_store = None
        if text_columns is not None:
            self.vector_store = VectorStore(model_name)
            print("Building vector index...")
            texts = self.df[text_columns].fillna('').agg(' '.join, axis=1).tolist()
            self.vector_store.build_index(texts)
            print("Index built successfully!")

    def sample(self,
              query: str = '',
              n_samples: int = 10,
              mode: str = 'recency',
              recency_weight: float = 0.5,
              similarity_weight: float = 0.5) -> pd.DataFrame:
        """Sample rows using recency, similarity, or combined weights"""
        # Calculate recency scores
        recency_scores = self.recency_calc.calculate_recency_scores(self.df[self.date_column])
        
        # Initialize similarity scores
        similarity_scores = np.zeros(len(self.df))
        
        # Calculate weights based on mode
        if mode == 'recency':
            combined_weights = recency_scores
        elif mode == 'similarity':
            if self.vector_store is None:
                raise ValueError("Text columns required for similarity mode")
            similarity_scores = self.vector_store.get_similarities(query)
            combined_weights = similarity_scores
        elif mode == 'combined':
            if self.vector_store is None:
                raise ValueError("Text columns required for combined mode")
            similarity_scores = self.vector_store.get_similarities(query)
            combined_weights = (
                recency_weight * recency_scores +
                similarity_weight * similarity_scores
            )
        else:
            raise ValueError("Mode must be 'recency', 'similarity', or 'combined'")
            
        # Ensure valid weights
        combined_weights = np.nan_to_num(combined_weights, 0)
        if combined_weights.sum() == 0:
            combined_weights = np.ones(len(combined_weights)) / len(combined_weights)
        else:
            combined_weights = combined_weights / combined_weights.sum()
        
        # Sample using combined weights
        n_samples = min(n_samples, len(self.df))
        sampled_indices = np.random.choice(
            len(self.df),  # Use length instead of index
            size=n_samples,
            replace=False,
            p=combined_weights
        )
        
        # Get results with scores
        result_df = self.df.iloc[sampled_indices].copy()  # Use iloc instead of loc
        result_df['recency_score'] = recency_scores[sampled_indices]
        result_df['similarity_score'] = similarity_scores[sampled_indices]
        result_df['combined_score'] = combined_weights[sampled_indices]
        
        return result_df.sort_values('combined_score', ascending=False)  # Sort descending

def sample_by_recency_and_similarity(df: pd.DataFrame,
                                   query: str = '',
                                   text_columns: Optional[List[str]] = None,
                                   date_column: str = 'meeting_time',
                                   n_samples: int = 10,
                                   mode: str = 'recency',
                                   recency_weight: float = 0.5,
                                   similarity_weight: float = 0.5,
                                   decay_factor: float = 0.1) -> pd.DataFrame:
    """
    Convenience function for one-off sampling
    
    Args:
        df: Input DataFrame
        query: Search query (required for similarity/combined modes)
        text_columns: Optional columns for text similarity (required for similarity/combined modes)
        mode: Sampling mode ('recency', 'similarity', or 'combined')
        ... other args remain the same ...
    """
    sampler = WeightedSampler(
        df=df,
        text_columns=text_columns,
        date_column=date_column,
        decay_factor=decay_factor
    )
    
    return sampler.sample(
        query=query,
        n_samples=n_samples,
        mode=mode,
        recency_weight=recency_weight,
        similarity_weight=similarity_weight
    )
    
    
    # ... existing code ...

from sqlalchemy import select
from psql_models import async_session, DiscussionPoint, Meeting, Speaker

async def fetch_joined_data():
    async with async_session() as session:
        # Original query remains the same
        query = select(DiscussionPoint, Meeting, Speaker).join(
            Meeting, DiscussionPoint.meeting_id == Meeting.meeting_id
        ).join(
            Speaker, DiscussionPoint.speaker_id == Speaker.id
        )
        
        result = await session.execute(query)
        rows = result.fetchall()
        
        # Create a dictionary to store all speakers per meeting
        meeting_speakers = {}
        for dp, meeting, speaker in rows:
            if meeting.meeting_id not in meeting_speakers:
                meeting_speakers[meeting.meeting_id] = set()
            meeting_speakers[meeting.meeting_id].add(speaker.name)
        
        # Convert the result to a list of dictionaries with other speakers
        data = []
        for dp, meeting, speaker in rows:
            # Get all speakers except the current one
            other_speakers = list(meeting_speakers[meeting.meeting_id] - {speaker.name})
            
            data.append({
                # Original fields remain the same
                'summary_index': dp.summary_index,
                'summary': dp.summary,
                'details': dp.details,
                'referenced_text': dp.referenced_text,
                'topic_name': dp.topic_name,
                'topic_type': dp.topic_type,
                'meeting_id': meeting.meeting_id,
                'meeting_timestamp': meeting.timestamp,
                'speaker_name': speaker.name,
                # Add new field
                'other_speakers': other_speakers
            })

        df = pd.DataFrame(data)
        return df
