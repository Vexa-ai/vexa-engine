import uuid
from typing import List, Dict, Any
from qdrant_client import AsyncQdrantClient
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
    TokenizerType
)
from sentence_transformers import SentenceTransformer
import numpy as np

from psql_models import DiscussionPoint, Meeting, Speaker, get_session
from sqlalchemy import select

class QdrantSearchEngine:
    def __init__(self):
        # Initialize with async client
        self.client = AsyncQdrantClient("127.0.0.1", port=6333)
        # Change to multilingual model
        self.model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
        self.collection_name = "discussion_points"
        # Update vector size for new model
        self.vector_size = 384  # Verify this matches the new model's output size
        
        # Refined weights for different field types
        self.weights = {
            "topic_name": 1.0,  # Most important
            "summary": 0.7,     # Medium importance
            "details": 0.5      # Lower importance
        }

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
        """Initialize collection with optimized search parameters"""
        await self.client.recreate_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(
                size=self.vector_size,
                distance=Distance.COSINE
            )
        )
        
        # Create text indices for each searchable field
        for field_name in self.weights.keys():
            await self.client.create_payload_index(
                collection_name=self.collection_name,
                field_name=field_name,
                field_schema=TextIndexParams(
                    type="text",
                    tokenizer=TokenizerType.WORD,  # Split on words
                    min_token_len=2,               # Minimum token length
                    max_token_len=15,              # Maximum token length
                    lowercase=True                 # Case-insensitive matching
                )
            )

    async def sync_from_postgres(self, session_factory):
        """Sync data from PostgreSQL to Qdrant"""
        # First create text indices for better text search
        await self.create_text_indices()
        
        async with session_factory() as session:
            # Get existing meeting_ids from Qdrant
            existing_points = await self.client.scroll(
                collection_name=self.collection_name,
                limit=10000,
                with_payload=["meeting_id"]
            )
            existing_meeting_ids = set()
            if existing_points[0]:
                existing_meeting_ids = {
                    point.payload.get("meeting_id") 
                    for point in existing_points[0] 
                    if point.payload.get("meeting_id")
                }

            query = select(DiscussionPoint, Meeting, Speaker).join(
                Meeting, DiscussionPoint.meeting_id == Meeting.meeting_id
            ).join(
                Speaker, DiscussionPoint.speaker_id == Speaker.id
            )
            
            result = await session.execute(query)
            rows = result.fetchall()
            
            points = []
            for dp, meeting, speaker in rows:
                if str(meeting.meeting_id) in existing_meeting_ids:
                    continue
                
                # Generate vectors for each field
                topic_vector = self.model.encode(dp.topic_name if dp.topic_name else "")
                summary_vector = self.model.encode(dp.summary if dp.summary else "")
                details_vector = self.model.encode(dp.details if dp.details else "")
                speaker_vector = self.model.encode(speaker.name)  # Add speaker vector
                
                discussion_id = str(uuid.uuid4())
                
                # Base payload
                base_payload = {
                    "topic_name": dp.topic_name,
                    "summary": dp.summary,
                    "details": dp.details,
                    "meeting_id": str(meeting.meeting_id),
                    "speaker_name": speaker.name,
                    "topic_type": dp.topic_type,
                    "discussion_id": discussion_id,
                    "timestamp": meeting.timestamp
                }
                
                # Create points for each vector type
                points.extend([
                    PointStruct(
                        id=str(uuid.uuid4()),
                        vector=topic_vector.tolist(),
                        payload={**base_payload, "vector_type": "topic_name"}
                    ),
                    PointStruct(
                        id=str(uuid.uuid4()),
                        vector=summary_vector.tolist(),
                        payload={**base_payload, "vector_type": "summary"}
                    ),
                    PointStruct(
                        id=str(uuid.uuid4()),
                        vector=details_vector.tolist(),
                        payload={**base_payload, "vector_type": "details"}
                    ),
                    PointStruct(
                        id=str(uuid.uuid4()),
                        vector=speaker_vector.tolist(),
                        payload={**base_payload, "vector_type": "speaker"}
                    )
                ])
                
                # Batch upload
                if len(points) >= 100:
                    await self.client.upsert(
                        collection_name=self.collection_name,
                        points=points
                    )
                    points = []
            
            # Upload remaining points
            if points:
                await self.client.upsert(
                    collection_name=self.collection_name,
                    points=points
                )

    async def create_text_indices(self):
        """Create text indices for each field type"""
        for field_name in self.weights.keys():  # topic_name, summary, details
            try:
                await self.client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name=field_name,
                    field_schema=TextIndexParams(
                        type="text",
                        tokenizer=TokenizerType.WORD,  # Split on words
                        min_token_len=2,               # Minimum token length
                        max_token_len=15,              # Maximum token length
                        lowercase=True                 # Case-insensitive matching
                    )
                )
                print(f"Created text index for field: {field_name}")
            except Exception as e:
                # Index might already exist, which is fine
                print(f"Note: {e} for field {field_name}")

    async def search(self, query_text: str, limit: int = 10, min_score: float = 0.7):
        """Enhanced hybrid search combining vector similarity with text matching"""
        query_vector = self.model.encode(query_text)
        all_results = []
        
        # Search each field type
        for vector_type, weight in self.weights.items():
            search_filter = Filter(
                should=[  # Use should instead of must for more lenient matching
                    FieldCondition(
                        key="vector_type",
                        match=MatchValue(value=vector_type)
                    ),
                    FieldCondition(
                        key=vector_type,
                        match=MatchText(text=query_text)
                    )
                ]
            )
            
            # Perform vector search with text filter
            results = await self.client.search(
                collection_name=self.collection_name,
                query_vector=query_vector.tolist(),
                query_filter=search_filter,
                limit=limit,
                score_threshold=min_score,
                search_params=SearchParams(
                    hnsw_ef=128  # Increase search accuracy
                ),
                with_payload=True,
                with_vectors=True
            )
            
            # Process results
            for hit in results:
                vector_score = hit.score * weight
                field_text = hit.payload.get(vector_type, "").lower()
                query_lower = query_text.lower()
                
                # Calculate text match boost
                text_match_score = 0
                
                # Exact match (highest boost)
                if query_lower in field_text:
                    text_match_score += 0.3
                
                # Word-level matching
                query_words = set(query_lower.split())
                field_words = set(field_text.split())
                word_overlap = len(query_words.intersection(field_words))
                if word_overlap > 0:
                    text_match_score += 0.1 * word_overlap
                
                # Combine vector similarity with text matching score
                hit.score = vector_score + (text_match_score * weight)
                all_results.append(hit)
        
        # Group and process results
        grouped_results = {}
        for hit in all_results:
            discussion_id = hit.payload["discussion_id"]
            if discussion_id not in grouped_results:
                grouped_results[discussion_id] = {
                    "score": 0,
                    "topic_name": hit.payload["topic_name"],
                    "summary": hit.payload["summary"],
                    "details": hit.payload["details"],
                    "meeting_id": hit.payload["meeting_id"],
                    "speaker_name": hit.payload["speaker_name"],
                    "topic_type": hit.payload.get("topic_type"),
                    "timestamp": hit.payload.get("timestamp"),
                    "vector_scores": {},
                    "exact_matches": []
                }
            
            # Store vector scores and track matches
            grouped_results[discussion_id]["vector_scores"][hit.payload["vector_type"]] = hit.score
            if query_text.lower() in hit.payload.get(hit.payload["vector_type"], "").lower():
                grouped_results[discussion_id]["exact_matches"].append(hit.payload["vector_type"])
            
            # Use max score across all field matches
            grouped_results[discussion_id]["score"] = max(
                grouped_results[discussion_id]["score"],
                hit.score
            )
        
        # Sort and filter results
        sorted_results = sorted(
            [r for r in grouped_results.values() if r["score"] >= min_score],
            key=lambda x: x["score"],
            reverse=True
        )
        
        return sorted_results[:limit]

    async def search_by_speaker(self, speaker_query: str, limit: int = 5, min_score: float = 0.7):
        """Search for discussions by speaker similarity"""
        query_vector = self.model.encode(speaker_query)
        
        hits = await self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector.tolist(),  # Convert numpy array to list
            query_filter=Filter(
                must=[
                    FieldCondition(
                        key="vector_type",
                        match=MatchValue(value="speaker")
                    )
                ]
            ),
            limit=limit,
            score_threshold=min_score
        )
        
        # Format results with scores
        results = []
        for hit in hits:
            results.append({
                "score": hit.score,
                "speaker_name": hit.payload["speaker_name"],
                "topic_name": hit.payload["topic_name"],
                "summary": hit.payload["summary"],
                "details": hit.payload["details"],
                "meeting_id": hit.payload["meeting_id"],
                "timestamp": hit.payload.get("timestamp"),
                "topic_type": hit.payload.get("topic_type")
            })
        
        return results
