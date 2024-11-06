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
    TokenizerType,
    MatchAny
)


from sqlalchemy.ext.asyncio import AsyncSession

from psql_helpers import get_session
import pandas as pd


from sentence_transformers import SentenceTransformer
import numpy as np

from psql_models import DiscussionPoint, Meeting, Speaker, UserMeeting
from sqlalchemy import select

import os

import asyncio

QDRANT_HOST = os.getenv('QDRANT_HOST', '127.0.0.1')
QDRANT_PORT = os.getenv('QDRANT_PORT', '6333')



class QdrantSearchEngine:
    def __init__(self):
        # Initialize with async client
        self.client = AsyncQdrantClient(QDRANT_HOST, port=QDRANT_PORT)
        # Change to multilingual model
        self.model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2', device='cuda:3')
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

    async def create_collection(self):
        """Initialize collection with optimized search parameters"""
        await self.client.create_collection(
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
                
                # Adjust batch size to prevent memory issues
                if len(points) >= 500:  # Increased from 100
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

    async def encode_text(self, text):
        print(f"Model device: {self.model.device}")
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.model.encode, text)

    async def search(self, query_text: str, meeting_ids: List[str], limit: int = 10, min_score: float = 0.7):
        """Enhanced hybrid search combining vector similarity with text matching"""
        query_vector = await self.encode_text(query_text)
        all_results = []
        
        # Search each field type
        for vector_type, weight in self.weights.items():
            should_conditions = [
                FieldCondition(
                    key="vector_type",
                    match=MatchValue(value=vector_type)
                ),
                FieldCondition(
                    key=vector_type,
                    match=MatchText(text=query_text)
                )
            ]
            
            must_conditions = [
                FieldCondition(
                    key="meeting_id",
                    match={'any': meeting_ids}
                )
            ]
            
            search_filter = Filter(
                should=should_conditions,
                must=must_conditions
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

    async def search_by_speaker(self, speaker_query: str, meeting_ids: List[str], limit: int = 5, min_score: float = 0.7):
        """Search for discussions by speaker similarity"""
        query_vector = await self.encode_text(speaker_query)
        
        must_conditions = [
            FieldCondition(
                key="vector_type",
                match=MatchValue(value="speaker")
            ),
            FieldCondition(
                key="meeting_id",
                match=MatchAny(any=meeting_ids)
            )
        ]
        
        hits = await self.client.search(
            collection_name=self.collection_name,
            query_vector=query_vector.tolist(),
            query_filter=Filter(must=must_conditions),
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

    async def sync_meeting_discussions(self, meeting_id: str, session: AsyncSession):
        """Sync discussion points for a specific meeting to Qdrant"""
        query = select(DiscussionPoint, Meeting, Speaker).join(
            Meeting, DiscussionPoint.meeting_id == Meeting.meeting_id
        ).join(
            Speaker, DiscussionPoint.speaker_id == Speaker.id
        ).where(Meeting.meeting_id == meeting_id)
        
        result = await session.execute(query)
        rows = result.fetchall()
        
        points = []
        for dp, meeting, speaker in rows:
            # Generate vectors for each field
            topic_vector = await self.encode_text(dp.topic_name if dp.topic_name else "")
            summary_vector = await self.encode_text(dp.summary if dp.summary else "")
            details_vector = await self.encode_text(dp.details if dp.details else "")
            speaker_vector = await self.encode_text(speaker.name)
            
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
                "timestamp": meeting.timestamp,
                "vector_source": "discussion"  # Added to distinguish source
            }
            
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
        
        # Create groups based on speaker changes
        speaker_change = (df['speaker'] != df['speaker'].shift())
        group_id = (speaker_change | (df.groupby((speaker_change).cumsum())['speaker'].transform('size') > 5)).cumsum()
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

    async def search_transcripts(self, query_text: str, meeting_ids: List[str] = None, limit: int = 20, min_score: float = 0.3):
        """
        Search through meeting transcripts using semantic similarity
        
        Args:
            query_text: str - The text to search for
            meeting_ids: Optional[List[str]] - List of meeting IDs to search in. If None, searches all meetings
            limit: int - Maximum number of results to return
            min_score: float - Minimum similarity score threshold
        """
        try:
            # Debug print
            print(f"Searching for: {query_text}")
            if meeting_ids:
                print(f"Filtering for meetings: {meeting_ids}")
            
            query_vector = await self.encode_text(query_text)
            
            # Create base filter for transcripts
            must_conditions = [
                FieldCondition(
                    key="vector_type",
                    match=MatchValue(value="transcript")
                )
            ]
            
            # Add meeting filter if meeting_ids provided
            if meeting_ids:
                must_conditions.append(
                    FieldCondition(
                        key="meeting_id",
                        match=MatchAny(any=meeting_ids)
                    )
                )
            
            search_filter = Filter(must=must_conditions)
            
            # Debug: Check if there are any transcript vectors
            count = await self.client.count(
                collection_name=self.collection_name,
                count_filter=search_filter
            )
            print(f"Found {count.count} transcript vectors to search")
            
            # Perform vector search
            results = await self.client.search(
                collection_name=self.collection_name,
                query_vector=query_vector.tolist(),
                query_filter=search_filter,
                limit=limit,
                score_threshold=min_score,
                search_params=SearchParams(
                    hnsw_ef=128  # Increase search accuracy
                ),
                with_payload=True
            )
            
            # Debug print
            print(f"Search returned {len(results)} results")
            
            # Format results
            formatted_results = []
            for hit in results:
                formatted_results.append({
                    "content": hit.payload.get("content", ""),
                    "speaker": hit.payload.get("speaker", ""),
                    "timestamp": hit.payload.get("timestamp", ""),
                    "similarity": hit.score,
                    "meeting_id": hit.payload.get("meeting_id", ""),
                    "indices": hit.payload.get("indices", [])
                })
            
            df = pd.DataFrame(formatted_results)
            
            # Debug print
            print(f"Created DataFrame with {len(df)} rows")
            if not df.empty:
                print("DataFrame columns:", df.columns.tolist())
                print("Sample row:", df.iloc[0].to_dict())
            
            return df

        except Exception as e:
            print(f"Error in search_transcripts: {e}")
            # Return empty DataFrame in case of error
            return pd.DataFrame(columns=[
                "content", "speaker", "timestamp", 
                "similarity", "meeting_id", "indices"
            ])

    async def search_transcripts_with_context(self, 
                                            query_text: str, 
                                            meeting_ids: List[str], 
                                            limit: int = 20, 
                                            min_score: float = 0.3):
        """Return all transcript items with search matches highlighted in bold"""
        # First get the search results to know which content to highlight
        results_df = await self.search_transcripts(query_text, meeting_ids, limit, min_score)
        
        # Create a set of content that matched the search for efficient lookup
        matching_content = set()
        if not results_df.empty:
            matching_content = set(results_df['content'].tolist())
        
        # Get ALL transcript items for these meetings
        all_results = []
        for meeting_id in meeting_ids:
            # Get all transcript chunks for this meeting
            response = await self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=Filter(
                    must=[
                        FieldCondition(
                            key="meeting_id",
                            match=MatchValue(value=meeting_id)
                        ),
                        FieldCondition(
                            key="vector_type",
                            match=MatchValue(value="transcript")
                        )
                    ]
                ),
                with_payload=True,
                limit=10000  # Make sure we get all chunks
            )
            
            if response[0]:  # If we have results
                # Sort chunks by timestamp to maintain conversation flow
                chunks = sorted(response[0], 
                              key=lambda x: x.payload.get('timestamp', ''))
                
                # Process each chunk
                for chunk in chunks:
                    content = chunk.payload['content']
                    # If this content was in our search results, wrap it in bold tags
                    if content in matching_content:
                        content = f"<b>{content}</b>"
                    
                    all_results.append({
                        "content": content,
                        "speaker": chunk.payload['speaker'],
                        "timestamp": chunk.payload['timestamp'],
                        "meeting_id": meeting_id
                    })
        
        # Sort final results by timestamp if needed
        all_results.sort(key=lambda x: x['timestamp'])
        
        return all_results

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

    async def sample_user_discussions(self, user_id: str, session: AsyncSession = None, sample_size: int = 100) -> pd.DataFrame:
        # Use context manager for session if not provided
        async with (session or get_session()) as session:
            # Get meeting IDs for user through UserMeeting table
            query = select(UserMeeting.meeting_id).where(UserMeeting.user_id == user_id)
            result = await session.execute(query)
            meeting_ids = [str(row[0]) for row in result.fetchall()]
            
            if not meeting_ids:
                return pd.DataFrame()

            # Create filter for meeting_ids
            scroll_filter = Filter(
                must=[
                    FieldCondition(
                        key="meeting_id", 
                        match=MatchAny(any=meeting_ids)
                    )
                ]
            )

            # Get all points for these meetings
            response = await self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=scroll_filter,
                limit=20000,
                with_payload=True
            )

            if not response[0]:
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame([hit.payload for hit in response[0]])
            
            # Calculate recency weights
            current_time = pd.Timestamp.now(tz='UTC')
            df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed').dt.tz_localize('UTC')
            time_diff = (current_time - df['timestamp']).dt.total_seconds()
            recency_weights = 1 / (1 + time_diff)

            # Take weighted sample
            sample_size = min(sample_size, len(df))
            sampled_df = df.sample(
                n=sample_size,
                weights=recency_weights,
                random_state=42
            )

            return sampled_df.sort_values(by='timestamp', ascending=False)
