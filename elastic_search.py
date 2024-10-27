from sentence_transformers import SentenceTransformer
import numpy as np
import pandas as pd
import faiss
import torch
from typing import List, Dict, Optional
from tqdm.auto import tqdm  # Added tqdm import

class VectorSearchEngine:
    def __init__(self, 
             model_name: str = 'all-MiniLM-L6-v2',
             device: Optional[int] = None):
        """Initialize the search engine with a sentence transformer model"""
        self.model = SentenceTransformer(model_name)
        if device is None or not torch.cuda.is_available():
            self.device = 'cpu'
        else:
            self.device = f'cuda:{device}'
        print(f"Using device: {self.device}")
        self.model = self.model.to(self.device)  # Store the moved model
        self.df: Optional[pd.DataFrame] = None
        self.field_indices: Dict[str, faiss.IndexFlatL2] = {}  # Store separate indices for each field
        self.field_weights = {
            'topic_name': 1.0,
            'summary': 0.7,
            'details': 0.5
        }
        
    def create_index(self, df: pd.DataFrame, batch_size: int = 8):
        """Create separate FAISS indices for each field"""
        self.df = df.copy()
        
        print("Creating field-specific indices...")
        for field in ['topic_name', 'summary', 'details']:
            print(f"Processing {field}...")
            text_data = df[field].fillna('').astype(str).tolist()
            
            # Create embeddings for this field
            embeddings = self.encode_batch(text_data, batch_size=batch_size)
            
            # Create FAISS index for this field
            dimension = embeddings.shape[1]
            field_index = faiss.IndexFlatL2(dimension)
            field_index.add(embeddings.astype('float32'))
            self.field_indices[field] = field_index
            
    def search(self, 
              query: str, 
              k: int = 5,
              fields: Optional[List[str]] = None,
              weights: Optional[Dict[str, float]] = None,
              return_scores: bool = False,
              exclude_speakers: Optional[List[str]] = None,
              min_similarity: float = 0.3,
              verbose: bool = False,
              exact_match_boost: float = 0.3) -> pd.DataFrame:  # Added boost parameter
        """Search across specified fields with custom weights"""
        
        if not self.field_indices or self.df is None:
            raise ValueError("Indices not created. Call create_index first.")
            
        # Use default fields and weights if not specified
        fields = fields or list(self.field_weights.keys())
        weights = weights or self.field_weights
        
        if verbose:
            print(f"\nSearch query: '{query}'")
            print(f"Fields searched: {fields}")
            print(f"Weights used: {weights}")
            print(f"Minimum similarity: {min_similarity}")
            print(f"Exact match boost: {exact_match_boost}")
        
        # Create query embedding
        with torch.no_grad():
            query_vector = self.model.encode([query])
        
        # Clear GPU memory if using CUDA
        if 'cuda' in self.device:
            torch.cuda.empty_cache()
            
        # Combine results from all fields
        all_scores = np.zeros(len(self.df))
        total_weight = 0
        
        # Semantic search
        for field in fields:
            if field not in self.field_indices:
                continue
                
            weight = weights.get(field, self.field_weights.get(field, 1.0))
            total_weight += weight
            
            if verbose:
                print(f"\nSearching field: {field} (weight: {weight})")
            
            # Search in this field's index
            distances, indices = self.field_indices[field].search(
                query_vector.astype('float32'),
                len(self.df)  # Search all documents
            )
            
            # Convert distances to scores and weight them
            field_scores = 1 / (1 + distances[0])
            all_scores[indices[0]] += field_scores * weight
            
            if verbose:
                # Show top 3 matches for this field
                top_3_indices = indices[0][:3]
                top_3_scores = field_scores[:3]
                print(f"\nTop 3 semantic matches for {field}:")
                for idx, score in zip(top_3_indices, top_3_scores):
                    text = str(self.df.iloc[idx][field])
                    print(f"Score: {score:.3f} | Text: {text[:100]}...")
        
        # Normalize semantic scores
        if total_weight > 0:
            all_scores /= total_weight
        
        # Add exact match boost (safely)
        try:
            query_lower = query.lower()
            exact_match_scores = np.zeros(len(self.df))
            
            for field in fields:
                field_texts = self.df[field].fillna('').astype(str).str.lower()
                exact_matches = field_texts.str.contains(query_lower, regex=False)
                if exact_matches.any():
                    exact_match_scores[exact_matches] += (weights.get(field, self.field_weights[field]) * exact_match_boost)
                    
                    if verbose:
                        print(f"\nExact matches found in {field}: {exact_matches.sum()}")
                        sample_matches = self.df[exact_matches].head(2)
                        for _, row in sample_matches.iterrows():
                            print(f"Sample match: {str(row[field])[:100]}...")
            
            # Combine scores (exact match boost is additive)
            all_scores += exact_match_scores
            
        except Exception as e:
            if verbose:
                print(f"Warning: Exact matching failed with error: {str(e)}")
                print("Continuing with semantic search results only...")
        
        # Create results DataFrame
        results_df = self.df.copy()
        results_df['similarity_score'] = all_scores
        
        # Filter by similarity threshold
        before_threshold = len(results_df)
        results_df = results_df[results_df['similarity_score'] >= min_similarity]
        after_threshold = len(results_df)
        
        if verbose:
            print(f"\nFiltering results:")
            print(f"Before similarity threshold: {before_threshold} results")
            print(f"After similarity threshold ({min_similarity}): {after_threshold} results")
        
        # Filter out excluded speakers if specified
        if exclude_speakers:
            before_speakers = len(results_df)
            results_df = results_df[~results_df['speaker_name'].isin(exclude_speakers)]
            after_speakers = len(results_df)
            
            if verbose:
                print(f"After speaker exclusion: {after_speakers} results")
                print(f"Excluded speakers: {exclude_speakers}")
        
        # Sort by similarity score and get top k
        results = results_df.nlargest(k, 'similarity_score')
        
        if verbose:
            print(f"\nFinal top {k} results:")
            for _, row in results.iterrows():
                print(f"\nScore: {row['similarity_score']:.3f}")
                print(f"Speaker: {row['speaker_name']}")
                print(f"Topic: {row['topic_name']}")
                print(f"Summary: {row['summary'][:100]}...")
        
        if not return_scores:
            results = results.drop(columns=['similarity_score'])
                
        return results
    
    def encode_batch(self, texts: List[str], batch_size: int = 8) -> np.ndarray:
        """Encode texts in batches to manage memory"""
        all_embeddings = []
        
        for i in tqdm(range(0, len(texts), batch_size)):
            batch = texts[i:i + batch_size]
            
            with torch.no_grad():
                embeddings = self.model.encode(
                    batch,
                    show_progress_bar=False,
                    convert_to_numpy=True,
                    device=self.device
                )
            
            all_embeddings.append(embeddings)
            if self.device.startswith('cuda'):
                torch.cuda.empty_cache()
                    
        return np.vstack(all_embeddings)
    
    def get_search_hints(self, 
                    partial_query: str, 
                    max_hints: int = 5,
                    min_chars: int = 3,
                    field: str = 'topic_name') -> List[str]:
        """Generate search hints/continuations based on partial query"""
        if not self.df is None and len(partial_query) >= min_chars:
            # Get all unique values from the specified field
            all_texts = self.df[field].fillna('').astype(str).unique()
            
            # Filter texts that contain the partial query (case-insensitive)
            partial_query = partial_query.lower()
            matches = [
                text for text in all_texts 
                if partial_query in text.lower()
            ]
            
            # Sort by length (shorter suggestions first) and then alphabetically
            matches.sort(key=lambda x: (len(x), x))
            
            return matches[:max_hints]
        
        return []

    def get_semantic_hints(self,
                        partial_query: str,
                        max_hints: int = 5,
                        min_chars: int = 3,
                        threshold: float = 0.7,
                        field: str = 'topic_name') -> List[str]:
        """Generate semantically similar search hints"""
        if not self.df is None and len(partial_query) >= min_chars:
            # Encode the partial query
            with torch.no_grad():
                query_vector = self.model.encode([partial_query])
            
            # Search in the specified field's index
            if field in self.field_indices:
                distances, indices = self.field_indices[field].search(
                    query_vector.astype('float32'),
                    len(self.df)
                )
                
                # Convert distances to similarity scores
                scores = 1 / (1 + distances[0])
                
                # Get unique suggestions above threshold
                suggestions = []
                seen = set()
                
                for idx, score in zip(indices[0], scores):
                    text = str(self.df.iloc[idx][field])
                    if score >= threshold and text not in seen:
                        suggestions.append((text, score))
                        seen.add(text)
                    
                    if len(suggestions) >= max_hints:
                        break
                
                # Sort by similarity score
                suggestions.sort(key=lambda x: x[1], reverse=True)
                
                return [text for text, _ in suggestions]
        
        return []
    
    def search_by_meeting(self,
                        query: str,
                        k: int = 5,
                        fields: Optional[List[str]] = None,
                        weights: Optional[Dict[str, float]] = None) -> pd.DataFrame:
        """Search and group results by meeting_id with mean similarity scores"""
        # Get all results with scores
        results = self.search(
            query=query,
            k=len(self.df),  # Get all results to ensure we don't miss any from same meeting
            fields=fields,
            weights=weights,
            return_scores=True
        )
        
        def create_markdown(group_df):
            """Create markdown content from group of topics"""
            if isinstance(group_df, pd.Series):
                return f"### {group_df}\n"
                
            topics = []
            for _, row in group_df.reset_index().iterrows():
                topic = f"### {row['topic_name']}\n"
                if pd.notna(row['summary']):
                    topic += f"**Summary:** {row['summary']}\n"
                if pd.notna(row['details']):
                    topic += f"\n**Details:**\n{row['details']}\n"
                topics.append(topic)
            return '\n\n'.join(topics)
        
        # Group by meeting_id and calculate mean score
        meeting_scores = (results
            .groupby('meeting_id')
            .agg({
                'similarity_score': 'mean',
                'topic_name': create_markdown,  # Combine all topics as markdown
                'meeting_timestamp': 'first',
                'speaker_name': lambda x: list(set(x)),  # Get unique speakers
                'summary': lambda x: '\n'.join(filter(None, x)),  # Combine all summaries
                'details': lambda x: '\n'.join(filter(None, x))  # Combine all details
            })
            .sort_values('similarity_score', ascending=False)
            .head(k)
        )
        
        # Rename column to reflect its new content
        meeting_scores = meeting_scores.rename(columns={'topic_name': 'content'})
        
        return meeting_scores
    
    def search_by_speaker(self,
                         speaker_name: str,
                         k: int = 5,
                         min_similarity: float = 0.3,
                         return_scores: bool = False) -> pd.DataFrame:
        """
        Search for topics by a specific speaker name (supports partial matches)
        
        Args:
            speaker_name: Full or partial name of the speaker
            k: Number of results to return
            min_similarity: Minimum similarity score for fuzzy name matching
            return_scores: Whether to include similarity scores in output
        """
        if self.df is None:
            raise ValueError("No data loaded. Call create_index first.")
        
        # Create query embedding for speaker name
        with torch.no_grad():
            query_vector = self.model.encode([speaker_name])
        
        # Get all unique speaker names and their embeddings
        unique_speakers = self.df['speaker_name'].unique()
        
        with torch.no_grad():
            speaker_embeddings = self.model.encode(
                unique_speakers.tolist(),
                show_progress_bar=False,
                convert_to_numpy=True,
                device=self.device
            )
        
        # Calculate similarity scores for speaker names
        speaker_distances = faiss.pairwise_distances(
            query_vector.astype('float32'),
            speaker_embeddings.astype('float32')
        )
        speaker_scores = 1 / (1 + speaker_distances[0])
        
        # Create speaker similarity dictionary
        speaker_similarities = {
            speaker: score 
            for speaker, score in zip(unique_speakers, speaker_scores)
            if score >= min_similarity
        }
        
        if not speaker_similarities:
            return pd.DataFrame()  # Return empty DataFrame if no matches
        
        # Filter DataFrame for matching speakers
        matching_speakers = list(speaker_similarities.keys())
        results_df = self.df[self.df['speaker_name'].isin(matching_speakers)].copy()
        
        # Add speaker similarity scores
        results_df['speaker_similarity'] = results_df['speaker_name'].map(speaker_similarities)
        
        # Sort by speaker similarity and get top k
        results = results_df.nlargest(k, 'speaker_similarity')
        
        # Format output
        if return_scores:
            return results[['speaker_name', 'topic_name', 'summary', 'details', 
                           'meeting_timestamp', 'speaker_similarity']]
        else:
            return results[['speaker_name', 'topic_name', 'summary', 'details', 
                           'meeting_timestamp']]
    



