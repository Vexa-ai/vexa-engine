import uuid
from typing import List, Dict, Any
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
    Filter,
    FieldCondition,
    MatchValue
)
from sentence_transformers import SentenceTransformer

from psql_models import DiscussionPoint, Meeting, Speaker, get_session
from sqlalchemy import select

class QdrantSearchEngine:
    def __init__(self):
        self.client = AsyncQdrantClient("127.0.0.1", port=6333)
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        self.collection_name = "discussion_points"
        self.vector_size = 384
        # Add weights attribute
        self.weights = {
            "topic_name": 1.0,
            "summary": 0.7,
            "details": 0.5
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
        """Initialize the collection"""
        # First drop the existing collection
        await self.drop_collection()
        
        # Then create a new collection
        await self.client.recreate_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(
                size=self.vector_size,
                distance=Distance.COSINE
            )
        )

    async def sync_from_postgres(self, session_factory):
        """Sync data from PostgreSQL to Qdrant"""
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

    async def search(self, query_text: str, limit: int = 10, min_score: float = 0.7, exact_match_boost: float = 0.3, verbose: bool = False):
        """Search across all vector types with weights and exact match boosting"""
        if verbose:
            print(f"\nSearch query: '{query_text}'")
            print(f"Fields searched: {list(self.weights.keys())}")
            print(f"Weights used: {self.weights}")
        
        query_vector = self.model.encode(query_text)
        all_results = []
        total_weight = sum(self.weights.values())
        
        # First pass: Get all semantic search results
        for vector_type, weight in self.weights.items():
            results = await self.client.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                query_filter=Filter(
                    must=[
                        FieldCondition(
                            key="vector_type",
                            match=MatchValue(value=vector_type)
                        )
                    ]
                ),
                limit=limit,
                score_threshold=min_score
            )
            
            # Normalize scores by total weight
            for hit in results:
                hit.score = (hit.score * weight) / total_weight
                all_results.append(hit)
        
        # Second pass: Add exact match boosts
        try:
            query_lower = query_text.lower()
            for hit in all_results:
                vector_type = hit.payload["vector_type"]
                field_text = hit.payload.get(vector_type, "").lower()
                
                if query_lower in field_text:
                    weight = self.weights[vector_type]
                    boost = exact_match_boost * (weight / total_weight)
                    hit.score += boost
                    
                    if verbose:
                        print(f"\nExact match in {vector_type}:")
                        print(f"Text: {field_text[:100]}...")
                        print(f"Added boost: {boost:.3f}")
        except Exception as e:
            if verbose:
                print(f"Warning: Exact matching failed with error: {str(e)}")
                print("Continuing with semantic search results only...")
        
        # Group and combine scores
        grouped_results = {}
        for hit in all_results:
            discussion_id = hit.payload["discussion_id"]
            if discussion_id not in grouped_results:
                grouped_results[discussion_id] = {
                    "score": 0,  # Will store max score
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
            
            # Store individual vector scores
            grouped_results[discussion_id]["vector_scores"][hit.payload["vector_type"]] = hit.score
            
            # Track exact matches
            if query_text.lower() in hit.payload.get(hit.payload["vector_type"], "").lower():
                grouped_results[discussion_id]["exact_matches"].append(hit.payload["vector_type"])
            
            # Use max score instead of sum
            grouped_results[discussion_id]["score"] = max(
                grouped_results[discussion_id]["score"],
                hit.score
            )
        
        # Filter and sort results
        sorted_results = sorted(
            [r for r in grouped_results.values() if r["score"] >= min_score],
            key=lambda x: x["score"],
            reverse=True
        )
        
        if verbose:
            print(f"\nResults after filtering: {len(sorted_results)}")
            for r in sorted_results[:3]:
                print(f"\nScore: {r['score']:.3f}")
                print(f"Topic: {r['topic_name']}")
                print(f"Exact matches in: {r['exact_matches']}")
        
        return sorted_results[:limit]

    async def search_by_speaker(self, speaker_query: str, limit: int = 5, min_score: float = 0.7):
        """Search for discussions by speaker similarity"""
        query_vector = self.model.encode(speaker_query)
        
        hits = await self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector,
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
