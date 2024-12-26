import uuid
from typing import List, Dict, Any
from uuid import UUID
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,

    Filter,
    FieldCondition,

    SearchParams,
    TextIndexParams,
    TokenizerType,
    MatchAny
)



import os



from voyageai import Client as VoyageClient


QDRANT_HOST = os.getenv('QDRANT_HOST')
QDRANT_PORT = os.getenv('QDRANT_PORT')



class QdrantSearchEngine:
    def __init__(self, voyage_api_key: str):
        self.client = AsyncQdrantClient(QDRANT_HOST, port=QDRANT_PORT)
        self.voyage = VoyageClient(voyage_api_key)
        self.collection_name = "meeting_chunks"
        self.vector_size = 1024  # Voyage embedding size
        
    async def create_collection(self):
        await self.client.create_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(
                size=self.vector_size,
                distance=Distance.COSINE
            )
        )
        
        # Single text index for content which includes speaker and topic
        await self.client.create_payload_index(
            collection_name=self.collection_name,
            field_name='content',
            field_schema=TextIndexParams(
                type="text",
                tokenizer=TokenizerType.WORD,
                min_token_len=2,
                lowercase=True
            )
        )

    async def search(self, query: str, meeting_ids: List[str] = None,
                    speakers: List[str] = None, limit: int = 10, 
                    min_score: float = 0.2) -> List[Dict[str, Any]]:
        query_embedding = self.voyage.embed(texts=[query], model='voyage-3')
        
        must_conditions = []
        if meeting_ids:
            must_conditions.append(
                FieldCondition(
                    key="meeting_id",
                    match=MatchAny(any=meeting_ids)
                )
            )
        
        if speakers:
            must_conditions.append(
                FieldCondition(
                    key="speaker",
                    match=MatchAny(any=speakers)
                )
            )
        
        results = await self.client.search(
            collection_name=self.collection_name,
            query_vector=query_embedding.embeddings[0],
            query_filter=Filter(must=must_conditions) if must_conditions else None,
            limit=limit,
            score_threshold=min_score,
            search_params=SearchParams(hnsw_ef=128),
            with_payload=True
        )
        
        return [
            {
                "score": hit.score,
                "content": hit.payload.get("content", ""),
                "contextualized_content": hit.payload.get("contextualized_content", ""),
                "topic": hit.payload.get("topic", ""),
                "meeting_id": hit.payload.get("meeting_id", ""),
                "formatted_time": hit.payload.get("formatted_time", ""),
                "timestamp": hit.payload.get("timestamp", ""),
                "speaker": hit.payload.get("speaker", ""),
                "speakers": hit.payload.get("speakers", []),
                "chunk_index": hit.payload.get("chunk_index", 0)
            }
            for hit in results
        ]

    async def drop_collection(self):
        try:
            await self.client.delete_collection(collection_name=self.collection_name)
            return True
        except Exception as e:

            return False
