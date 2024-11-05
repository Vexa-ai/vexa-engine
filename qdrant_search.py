import uuid
from typing import List, Dict, Any, Optional
from qdrant_client import AsyncQdrantClient, models
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
    Filter,
    FieldCondition,
    MatchValue,
    MatchText,
    Range,
    SearchParams,
    TextIndexParams,
    TokenizerType,
    MatchAny
)




from sqlalchemy.ext.asyncio import AsyncSession

import pandas as pd


from fastembed.embedding import TextEmbedding

from psql_models import DiscussionPoint, Meeting, Speaker
from sqlalchemy import select

import os

import asyncio

from difflib import SequenceMatcher
import re

from psql_helpers import get_session

import time
import math
import logging
logger = logging.getLogger(__name__)

QDRANT_HOST = os.getenv('QDRANT_HOST', '127.0.0.1')
QDRANT_PORT = os.getenv('QDRANT_PORT', '6333')




class QdrantSearchEngine:
    def __init__(self):
        # Initialize with async client
        self.client = AsyncQdrantClient(QDRANT_HOST, port=QDRANT_PORT)
        # Set up both dense and sparse models
        self.client.set_model("sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2")
        self.client.set_sparse_model("Qdrant/bm25")
        self.collection_name = "discussion_points"

        # Weights for different field types with normalized values
        self.weights = {
            "topic_name": 1.0,    # Most important
            "summary": 0.7,       # Medium importance
            "details": 0.5,       # Lower importance
            "speaker": 0.8,       # High importance for speaker matches
            "transcript": 0.6     # Medium-low importance for transcript matches
        }
        
        # Cache configuration
        self._cache = {}
        self._cache_ttl = 300  # 5 minutes
        self._last_cache_cleanup = time.time()

    async def drop_collection(self):
        """Drop the collection if it exists"""
        try:
            await self.client.delete_collection(
                collection_name=self.collection_name 
            )
            print(f"Collection {self.collection_name} dropped successfully")
        except Exception as e:
            print(f"Error dropping collection: {e}")

    async def initialize(self):
        """Initialize collection with both dense and sparse vector configurations"""
        await self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=self.client.get_fastembed_vector_params(),
            sparse_vectors_config=self.client.get_fastembed_sparse_vector_params()
        )


    async def sync_from_postgres(self, session_factory=None):
        """Sync both discussion points and transcripts from PostgreSQL to Qdrant"""
        session_factory = session_factory or get_session
        async with session_factory() as session:
            # Get existing meeting_ids from Qdrant
            existing_points = await self.client.scroll(
                collection_name=self.collection_name,
                limit=10000000,
                with_payload=["meeting_id"]
            )
            existing_meeting_ids = set()
            if existing_points[0]:
                existing_meeting_ids = {
                    point.payload.get("meeting_id") 
                    for point in existing_points[0] 
                    if point.payload.get("meeting_id")
                }

            # Query meetings with discussion points
            query = select(DiscussionPoint, Meeting, Speaker).join(
                Meeting, DiscussionPoint.meeting_id == Meeting.meeting_id
            ).join(
                Speaker, DiscussionPoint.speaker_id == Speaker.id
            )
            
            result = await session.execute(query)
            rows = result.fetchall()
            
            # Prepare batches for documents and metadata
            documents = []
            metadata = []
            ids = []
            
            for dp, meeting, speaker in rows:
                meeting_id_str = str(meeting.meeting_id)
                if meeting_id_str in existing_meeting_ids:
                    continue
                    
                discussion_id = str(uuid.uuid4())
                base_payload = {
                    "meeting_id": meeting_id_str,
                    "speaker_name": speaker.name,
                    "topic_type": dp.topic_type,
                    "discussion_id": discussion_id,
                    "timestamp": meeting.timestamp,
                    "vector_source": "discussion"
                }
                
                # Add topic_name
                if dp.topic_name:
                    documents.append(dp.topic_name)
                    metadata.append({
                        **base_payload,
                        "topic_name": dp.topic_name,
                        "vector_type": "topic_name"
                    })
                    ids.append(str(uuid.uuid4()))
                
                # Add summary
                if dp.summary:
                    documents.append(dp.summary)
                    metadata.append({
                        **base_payload,
                        "summary": dp.summary,
                        "vector_type": "summary"
                    })
                    ids.append(str(uuid.uuid4()))
                
                # Add details
                if dp.details:
                    documents.append(dp.details)
                    metadata.append({
                        **base_payload,
                        "details": dp.details,
                        "vector_type": "details"
                    })
                    ids.append(str(uuid.uuid4()))
                
                # Add speaker
                documents.append(speaker.name)
                metadata.append({
                    **base_payload,
                    "speaker_name": speaker.name,
                    "vector_type": "speaker"
                })
                ids.append(str(uuid.uuid4()))

            # Add all documents in one batch
            if documents:
                await self.client.add(
                    collection_name=self.collection_name,
                    documents=documents,
                    metadata=metadata,
                    ids=ids
                )

            # Process transcripts
            for meeting in set(row[1] for row in rows):
                if meeting.transcript:
                    df = pd.DataFrame(eval(meeting.transcript)).reset_index()
                    group_id = df.index // 5
                    grouped_df = df.groupby(group_id)
                    
                    grouped_text = grouped_df.agg({
                        'speaker': 'first',
                        'content': ' '.join,
                        'timestamp': 'first',
                        'index': list
                    })
                    
                    # Prepare transcript batches
                    transcript_documents = []
                    transcript_metadata = []
                    transcript_ids = []
                    
                    for idx, row in grouped_text.iterrows():
                        timestamp = row['timestamp'].isoformat() if hasattr(row['timestamp'], 'isoformat') else str(row['timestamp'])
                        
                        transcript_documents.append(row['content'])
                        transcript_metadata.append({
                            'meeting_id': str(meeting.meeting_id),
                            'content': row['content'],
                            'speaker': row['speaker'],
                            'timestamp': timestamp,
                            'indices': row['index'],
                            'vector_type': 'transcript',
                            'vector_source': 'transcript',
                            'chunk_index': idx
                        })
                        transcript_ids.append(str(uuid.uuid4()))
                    
                    # Add all transcript documents in one batch
                    if transcript_documents:
                        await self.client.add(
                            collection_name=self.collection_name,
                            documents=transcript_documents,
                            metadata=transcript_metadata,
                            ids=transcript_ids
                        )

    async def encode_text(self, text):
        """Modified to use FastEmbed's async-friendly embedding"""
        if not text:
            text = ""
        embeddings = list(self.client.model.embed([text]))
        return embeddings[0]  # Return the first (and only) embedding

    async def search(self, query_text: str, meeting_ids: List[str], limit: int = 10, min_score: float = 0.7):
        """Improved hybrid search with better score aggregation"""
        cache_key = f"{query_text}:{','.join(sorted(meeting_ids))}:{limit}:{min_score}"
        
        # Check cache first
        cached_result = self._get_from_cache(cache_key)
        if cached_result:
            return cached_result

        try:
            # Single query for all field types
            must_conditions = [
                models.FieldCondition(
                    key="meeting_id",
                    match=models.MatchAny(any=meeting_ids)
                )
            ]

            search_filter = models.Filter(must=must_conditions)

            # Get all results in one query with higher limit
            results = await self.client.query(
                collection_name=self.collection_name,
                query_text=query_text,
                query_filter=search_filter,
                limit=limit * 4,  # Get more results to account for all field types
                score_threshold=min_score * 0.7,  # Lower initial threshold
                search_params=models.SearchParams(
                    hnsw_ef=128,  # Increase search accuracy
                    exact=False   # Use approximate search for speed
                )
            )

            # Process and normalize scores
            processed_results = {}
            for hit in results:
                field_type = hit.metadata.get("vector_type")
                discussion_id = hit.metadata.get("discussion_id")
                
                if not discussion_id or not field_type:
                    continue
                
                # Apply field weight and normalize score
                weighted_score = hit.score * self.weights.get(field_type, 0.5)
                
                if discussion_id not in processed_results:
                    processed_results[discussion_id] = {
                        "scores": {},
                        "metadata": hit.metadata,
                        "document": hit.document,
                        "matched_fields": set()
                    }
                
                processed_results[discussion_id]["scores"][field_type] = weighted_score
                processed_results[discussion_id]["matched_fields"].add(field_type)

            # Calculate final scores with improved aggregation
            final_results = []
            for discussion_id, data in processed_results.items():
                # Calculate weighted average of top scores
                scores = sorted(data["scores"].values(), reverse=True)
                if not scores:
                    continue
                
                # Use exponential decay for score combination
                final_score = sum(
                    score * math.exp(-0.5 * i) 
                    for i, score in enumerate(scores[:3])
                ) / sum(math.exp(-0.5 * i) for i in range(min(3, len(scores))))
                
                # Boost score based on field coverage
                field_coverage = len(data["matched_fields"]) / len(self.weights)
                final_score *= (1 + 0.2 * field_coverage)  # Up to 20% boost for full coverage
                
                if final_score >= min_score:
                    result = {
                        "score": final_score,
                        "document": data["document"],
                        "matched_fields": list(data["matched_fields"]),
                        **data["metadata"]
                    }
                    final_results.append(result)

            # Sort and limit results
            final_results.sort(key=lambda x: x["score"], reverse=True)
            final_results = final_results[:limit]
            
            # Cache results
            self._add_to_cache(cache_key, final_results)
            
            return final_results

        except Exception as e:
            logger.error(f"Error in search: {str(e)}")
            return []

    def _get_from_cache(self, key: str) -> Optional[List[Dict]]:
        """Get results from cache if not expired"""
        now = time.time()
        if now - self._last_cache_cleanup > 60:  # Cleanup every minute
            self._cleanup_cache()
            self._last_cache_cleanup = now
            
        cached = self._cache.get(key)
        if cached and cached["expires"] > now:
            return cached["data"]
        return None

    def _add_to_cache(self, key: str, data: List[Dict]):
        """Add results to cache with expiration"""
        self._cache[key] = {
            "data": data,
            "expires": time.time() + self._cache_ttl
        }

    def _cleanup_cache(self):
        """Remove expired cache entries"""
        now = time.time()
        self._cache = {
            k: v for k, v in self._cache.items() 
            if v["expires"] > now
        }

    async def search_by_speaker(self, speaker_query: str, meeting_ids: List[str], limit: int = 5, min_score: float = 0.7):
        """Hybrid search for speakers with name matching boost"""
        try:
            # Create filter for speakers and meeting_ids
            must_conditions = [
                models.FieldCondition(
                    key="vector_type",
                    match=models.MatchValue(value="speaker")
                ),
                models.FieldCondition(
                    key="meeting_id",
                    match=models.MatchAny(any=meeting_ids)
                )
            ]

            search_filter = models.Filter(must=must_conditions)

            # Perform hybrid search
            results = await self.client.query(
                collection_name=self.collection_name,
                query_text=speaker_query,
                query_filter=search_filter,
                limit=limit * 2,  # Get more results for filtering
                score_threshold=min_score * 0.8  # Lower threshold for initial search
            )

            # Process results with name matching boost
            processed_results = []
            for hit in results:
                score = hit.score * self.weights["speaker"]
                
                # Add exact name match boost
                speaker_name = hit.metadata["speaker_name"].lower()
                query_lower = speaker_query.lower()
                if query_lower in speaker_name:
                    score += 0.2
                if query_lower == speaker_name:
                    score += 0.3

                if score >= min_score:
                    processed_results.append({
                        "score": score,
                        "speaker_name": hit.metadata["speaker_name"],
                        "topic_name": hit.metadata["topic_name"],
                        "summary": hit.metadata["summary"],
                        "details": hit.metadata["details"],
                        "meeting_id": hit.metadata["meeting_id"],
                        "timestamp": hit.metadata.get("timestamp"),
                        "topic_type": hit.metadata.get("topic_type")
                    })

            # Sort by final score and return top results
            return sorted(
                processed_results,
                key=lambda x: x["score"],
                reverse=True
            )[:limit]

        except Exception as e:
            print(f"Error in search_by_speaker: {e}")
            return []

    async def sync_meeting_discussions(self, meeting_id: str, session: AsyncSession = None):
        """Sync discussion points with both dense and sparse embeddings"""
        if session is None:
            async with get_session() as session:
                return await self._sync_meeting_discussions(meeting_id, session)
        return await self._sync_meeting_discussions(meeting_id, session)

    async def _sync_meeting_discussions(self, meeting_id: str, session: AsyncSession):
        query = select(DiscussionPoint, Meeting, Speaker).join(
            Meeting, DiscussionPoint.meeting_id == Meeting.meeting_id
        ).join(
            Speaker, DiscussionPoint.speaker_id == Speaker.id
        ).where(Meeting.meeting_id == meeting_id)
        
        result = await session.execute(query)
        rows = result.fetchall()
        
        # Prepare batches for encoding
        topics = [dp.topic_name if dp.topic_name else "" for dp, _, _ in rows]
        summaries = [dp.summary if dp.summary else "" for dp, _, _ in rows]
        details = [dp.details if dp.details else "" for dp, _, _ in rows]
        speakers = [speaker.name for _, _, speaker in rows]
        
        # Batch encode all texts
        topic_vectors = list(self.client.model.embed(topics))
        summary_vectors = list(self.client.model.embed(summaries))
        details_vectors = list(self.client.model.embed(details))
        speaker_vectors = list(self.client.model.embed(speakers))
        
        points = []
        for idx, (dp, meeting, speaker) in enumerate(rows):
            discussion_id = str(uuid.uuid4())
            
            base_payload = {
                "topic_name": dp.topic_name,
                "summary": dp.summary,
                "details": dp.details,
                "meeting_id": str(meeting.meeting_id),
                "speaker_name": speaker.name,
                "topic_type": dp.topic_type,
                "discussion_id": discussion_id,
                "timestamp": meeting.timestamp,
                "vector_source": "discussion"
            }
            
            points.extend([
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=topic_vectors[idx].tolist(),
                    payload={**base_payload, "vector_type": "topic_name"}
                ),
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=summary_vectors[idx].tolist(),
                    payload={**base_payload, "vector_type": "summary"}
                ),
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=details_vectors[idx].tolist(),
                    payload={**base_payload, "vector_type": "details"}
                ),
                PointStruct(
                    id=str(uuid.uuid4()),
                    vector=speaker_vectors[idx].tolist(),
                    payload={**base_payload, "vector_type": "speaker"}
                )
            ])
        
        if points:
            await self.client.upsert(
                collection_name=self.collection_name,
                points=points
            )

    async def sync_meeting_transcript(self, meeting_id: str, session: AsyncSession):
        """Sync transcript chunks for a specific meeting to Qdrant"""
        # Get meeting transcript
        meeting = await session.execute(
            select(Meeting).where(Meeting.meeting_id == meeting_id)
        )
        meeting = meeting.scalar_one()
        
        if not meeting.transcript:
            return
        
        # Process transcript into grouped format
        df = pd.DataFrame(eval(meeting.transcript)).reset_index()
        
        # Create groups of 5 entries
        group_id = df.index // 5
        grouped_df = df.groupby(group_id)
        
        # Aggregate the groups
        grouped_text = grouped_df.agg({
            'speaker': 'first',
            'content': ' '.join,
            'timestamp': 'first',
            'index': list
        })
        
        # Generate embeddings for transcript chunks
        transcript_points = []
        for idx, row in grouped_text.iterrows():
            vector = await self.encode_text(row['content'])
            
            # Convert timestamp to string if it's a datetime object
            timestamp = row['timestamp'].isoformat() if hasattr(row['timestamp'], 'isoformat') else str(row['timestamp'])
            
            # Use UUID instead of formatted string for point ID
            point_id = str(uuid.uuid4())  # Generate a proper UUID
            
            transcript_points.append(PointStruct(
                id=point_id,  # Use UUID instead of formatted string
                vector=vector.tolist(),
                payload={
                    'meeting_id': str(meeting_id),
                    'content': row['content'],
                    'speaker': row['speaker'],
                    'timestamp': timestamp,
                    'indices': row['index'],
                    'vector_type': 'transcript',
                    'vector_source': 'transcript',
                    'chunk_index': idx  # Keep track of order if needed
                }
            ))

        if transcript_points:
            await self.client.upsert(
                collection_name=self.collection_name,
                points=transcript_points
            )

    async def sync_meeting(self, meeting_id: str, session: AsyncSession):
        """Sync both discussion points and transcript for a meeting"""
        await self.sync_meeting_discussions(meeting_id, session)
        await self.sync_meeting_transcript(meeting_id, session)

    def calculate_text_match_score(self, query: str, content: str) -> float:
        """
        Calculate text match score using a combination of exact matching and sequence similarity.
        
        Args:
            query: str - The search query
            content: str - The content to match against
            
        Returns:
            float - Score between 0 and 1
        """
        # Normalize text for comparison
        query = query.lower().strip()
        content = content.lower().strip()
        
        # Calculate exact match score
        query_terms = set(re.findall(r'\w+', query))
        content_terms = set(re.findall(r'\w+', content))
        
        if not query_terms:
            return 0.0
            
        # Calculate word match ratio
        matching_terms = query_terms.intersection(content_terms)
        exact_match_score = len(matching_terms) / len(query_terms)
        
        # Calculate sequence similarity
        sequence_score = SequenceMatcher(None, query, content).ratio()
        
        # Combine scores (giving more weight to exact matches)
        combined_score = (0.7 * exact_match_score) + (0.3 * sequence_score)
        
        return combined_score

    async def search_transcripts(self, query_text: str, meeting_ids: List[str] | str = None, limit: int = 20, min_score: float = 0.5):
        """Hybrid search for transcripts with context weighting"""
        try:
            # Convert single meeting_id to list if necessary
            if meeting_ids and not isinstance(meeting_ids, list):
                meeting_ids = [str(meeting_ids)]
            elif meeting_ids:
                meeting_ids = [str(mid) for mid in meeting_ids]

            # Create filter for transcripts and meeting_ids
            must_conditions = [
                models.FieldCondition(
                    key="vector_type",
                    match=models.MatchValue(value="transcript")
                )
            ]
            
            if meeting_ids:
                must_conditions.append(
                    models.FieldCondition(
                        key="meeting_id",
                        match=models.MatchAny(any=meeting_ids)
                    )
                )

            search_filter = models.Filter(must=must_conditions)

            # Get more results initially to account for context
            results = await self.client.query(
                collection_name=self.collection_name,
                query_text=query_text,
                query_filter=search_filter,
                limit=limit * 2,  # Get more results for context
                score_threshold=min_score * 0.5  # Lower threshold for context
            )

            # Process results with context weighting
            processed_results = []
            for i, hit in enumerate(results):
                score = hit.score
                
                # Apply context boost for sequential matches
                if i > 0 and i < len(results) - 1:
                    prev_score = results[i-1].score
                    next_score = results[i+1].score
                    context_boost = (prev_score + next_score) * 0.1
                    score += context_boost

                processed_results.append({
                    "content": hit.metadata.get("content", ""),
                    "speaker": hit.metadata.get("speaker", ""),
                    "timestamp": hit.metadata.get("timestamp", ""),
                    "similarity": score,
                    "meeting_id": hit.metadata.get("meeting_id", ""),
                    "indices": hit.metadata.get("indices", []),
                    "chunk_index": hit.metadata.get("chunk_index")
                })

            # Create DataFrame and filter by adjusted scores
            df = pd.DataFrame(processed_results)
            if not df.empty:
                df = df[df['similarity'] >= min_score]
                df = df.sort_values('similarity', ascending=False)
                
            return df.head(limit).drop_duplicates('timestamp')

        except Exception as e:
            print(f"Error in search_transcripts: {e}")
            return pd.DataFrame(columns=[
                "content", "speaker", "timestamp", "similarity",
                "meeting_id", "indices", "chunk_index"
            ])

    async def search_transcripts_with_context(self, 
                                            query_text: str, 
                                            meeting_ids: List[str] | str, 
                                            session_factory=None, 
                                            limit: int = 20, 
                                            min_score: float = 0.4):
        """Return all transcript items with search matches highlighted in bold"""
        try:
            # Use provided session_factory or get_session
            session_factory = session_factory or get_session
            
            # Convert meeting_ids to list if needed
            if not isinstance(meeting_ids, list):
                meeting_ids = [str(meeting_ids)]
            else:
                meeting_ids = [mid for mid in meeting_ids]

            # Get matching chunks using search_transcripts
            matching_df = await self.search_transcripts(
                query_text=query_text,
                meeting_ids=meeting_ids,
                limit=20000,
                min_score=min_score
            )
            
            # Get all matching indices
            matching_indices = set()
            if not matching_df.empty:
                for indices in matching_df['indices']:
                    matching_indices.update(indices)

            async with session_factory() as session:
                query = select(Meeting).where(
                    Meeting.meeting_id.in_([mid for mid in meeting_ids])
                )
                result = await session.execute(query)
                meetings = result.scalars().all()

                all_results = []
                for meeting in meetings:
                    if meeting.transcript:
                        trans_df = pd.DataFrame(eval(meeting.transcript))
                        
                        for idx, row in trans_df.iterrows():
                            content = f"<b>{row['content']}</b>" if idx in matching_indices else row['content']
                            all_results.append({
                                "content": content,
                                "speaker": row['speaker'],
                                "timestamp": row['timestamp'],
                                "meeting_id": str(meeting.meeting_id),
                                "is_match": idx in matching_indices
                            })

                return sorted(all_results, key=lambda x: x['timestamp'])

        except Exception as e:
            print(f"Error in search_transcripts_with_context: {e}")
            return []

    async def show_collection_stats(self):
        """Show statistics about vectors in the collection"""
        try:
            # Get collection info
            collection_info = await self.client.get_collection(self.collection_name)
            
            # Get points count
            points_count = await self.client.count(
                collection_name=self.collection_name
            )
            
            # Get counts by vector type
            vector_types = ['topic_name', 'summary', 'details', 'speaker', 'transcript']
            type_counts = {}
            
            for vector_type in vector_types:
                count = await self.client.count(
                    collection_name=self.collection_name,
                    count_filter=Filter(
                        must=[
                            FieldCondition(
                                key="vector_type",
                                match=MatchValue(value=vector_type)
                            )
                        ]
                    )
                )
                type_counts[vector_type] = count.count
            
            # Get counts by vector source
            source_counts = {}
            for source in ['discussion', 'transcript']:
                count = await self.client.count(
                    collection_name=self.collection_name,
                    count_filter=Filter(
                        must=[
                            FieldCondition(
                                key="vector_source",
                                match=MatchValue(value=source)
                            )
                        ]
                    )
                )
                source_counts[source] = count.count
            
            # Get unique meeting count
            unique_meetings = await self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=None,
                limit=1,
                with_payload=["meeting_id"],
                with_vectors=False
            )
            unique_meeting_ids = set()
            if unique_meetings[0]:
                unique_meeting_ids = {
                    point.payload.get("meeting_id") 
                    for point in unique_meetings[0] 
                    if point.payload.get("meeting_id")
                }
            
            # Get vector dimension from collection config
            vector_size = collection_info.config.params.vectors.size
            
            return {
                "total_vectors": points_count.count,
                "vector_dimension": vector_size,  # Updated to use correct path
                "vectors_by_type": type_counts,
                "vectors_by_source": source_counts,
                "unique_meetings": len(unique_meeting_ids),
                "collection_name": self.collection_name
            }
            
        except Exception as e:
            return {
                "error": f"Failed to get collection stats: {str(e)}"
            }
