import uuid
from typing import List, Dict, Any
from uuid import UUID
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


from fastembed import TextEmbedding,SparseTextEmbedding
import numpy as np

from psql_models import DiscussionPoint, Meeting, Speaker, UserMeeting
from sqlalchemy import select

import os

import asyncio


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

    async def search(self, 
                    query: str, 
                    meeting_ids: List[str] = None, 
                    limit: int = 10, 
                    min_score: float = 0.2):
        query_embedding = self.voyage.embed(texts=[query], model='voyage-3')
        
        must_conditions = []
        if meeting_ids:
            must_conditions.append(
                FieldCondition(
                    key="meeting_id",
                    match=MatchAny(any=meeting_ids)
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
                'score': hit.score,
                'content': hit.payload['content'],
                'contextualized_content': hit.payload['contextualized_content'],
                'topic': hit.payload['topic'],
                'formatted_time': hit.payload['formatted_time'],
                'meeting_id': hit.payload['meeting_id'],
                'timestamp': hit.payload['timestamp']
            }
            for hit in results
        ]
