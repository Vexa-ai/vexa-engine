from typing import List, Dict, Any, Tuple, Optional
import pandas as pd
from datetime import datetime
import uuid
from qdrant_client.models import PointStruct
from sqlalchemy.ext.asyncio import AsyncSession
from vexa import VexaAPI
from elasticsearch import AsyncElasticsearch
from qdrant_client import AsyncQdrantClient

from core import generic_call, system_msg, user_msg
from psql_models import Content, ContentType
from .content_relations import update_child_content

from pydantic_models import TopicsExtraction

import asyncio

import logging
logger = logging.getLogger(__name__)

DOCUMENT_CONTEXT_PROMPT = """
<document>
{doc_content}
</document>
"""

CHUNK_CONTEXT_PROMPT = """
Here is the chunk we want to situate within the whole document
<chunk>
{chunk_content}
</chunk>

Please give a short succinct context to situate this chunk within the overall document for the purposes of improving search retrieval of the chunk.
Answer only with the succinct context and nothing else.
"""

class ProcessingError(Exception):
    pass

class ContentProcessor:
    def __init__(self, qdrant_engine, es_engine):
        self.qdrant_engine = qdrant_engine
        self.es = es_engine
        self.vexa = VexaAPI()
        
    async def process_content(
        self, 
        content_id: str,
        user_id: str,
        token: str,
        session: AsyncSession
    ) -> None:
        """Main processing entry point"""
        try:
            # 1. Get transcription data
            self.vexa.token = token
            df, formatted_output, start_datetime, speakers, transcript = \
                await self.vexa.get_transcription(meeting_session_id=content_id)
            
            if df.empty:
                raise ProcessingError("No transcription data available")

            # 2. Create or update main content
            content = await self._ensure_content(
                session, content_id, ContentType.MEETING.value,
                start_datetime, user_id
            )

            # 3. Process and store chunks
            es_documents, qdrant_points = await self._process_chunks(
                df, content_id, start_datetime, speakers
            )
            
            # 4. Index to search engines
            await self._index_to_search_engines(es_documents, qdrant_points)
            
            # 5. Mark content as indexed
            content.is_indexed = True
            await session.commit()

        except Exception as e:
            logger.error(f"Processing error for content {content_id}: {str(e)}")
            raise ProcessingError(f"Content processing failed: {str(e)}")

    async def _ensure_content(
        self,
        session: AsyncSession,
        content_id: str,
        content_type: str,
        timestamp: datetime,
        user_id: str
    ) -> Content:
        """Ensure content exists and return it"""
        # Convert string to UUID before querying
        content_id = uuid.UUID(content_id)
        content = await session.get(Content, content_id)
        if not content:
            content = Content(
                id=content_id,
                content_id=content_id,
                type=content_type,
                timestamp=timestamp,
                created_by=user_id,
                is_indexed=False
            )
            session.add(content)
            await session.flush()
        return content

    async def _process_chunks(
        self,
        df: pd.DataFrame,
        content_id: str,
        start_datetime: datetime,
        speakers: List[str]
    ) -> Tuple[List[Dict], List[PointStruct]]:
        """Process content into searchable chunks"""
        
        # 1. Extract topics
        input_text = df[['formatted_time','speaker', 'content']].to_markdown()
        topics_result = await TopicsExtraction.call([
            system_msg(f"Extract topics from the following text: {input_text}"),
            user_msg(input_text)
        ])
        
        # 2. Convert topics to DataFrame and merge
        topics_df = pd.DataFrame([
            {"formatted_time": m.formatted_time, "topic": m.topic} 
            for m in topics_result.mapping
        ])
        
        # 3. Merge and forward fill topics
        df = df.merge(topics_df, on='formatted_time', how='left')
        df = df[['formatted_time','topic','speaker','content']].ffill()
        
        # 4. Create groups based on speaker + topic changes
        df['speaker_shift'] = (df['speaker']+df['topic'] != df['speaker']+df['topic'].shift(1)).cumsum()
        
        # 5. Group content into chunks
        df_grouped = df.groupby('speaker_shift').agg({
            'formatted_time': 'first',
            'speaker': 'first',
            'topic': 'first',  # Added topic
            'content': ' '.join
        }).reset_index()
        
        # 6. Create chunks with speaker prefix
        chunks = (df_grouped['speaker'] + ': ' + df_grouped['content']).tolist()
        
        # 7. Contextualize chunks
        doc_content = '\n'.join(chunks)
        contextualized_chunks = await self._contextualize_chunks(chunks, doc_content)
        
        # 8. Get embeddings using Voyage client from qdrant engine
        embeddings_response = self.qdrant_engine.voyage.embed(texts=contextualized_chunks, model='voyage-3')
        embeddings = embeddings_response.embeddings
        
        # 9. Prepare documents for both engines
        es_documents = []
        qdrant_points = []
        
        for i, (chunk, contextualized_chunk, row) in enumerate(zip(chunks, contextualized_chunks, df_grouped.itertuples())):
            # Prepare Elasticsearch document
            es_doc = {
                'meeting_id': content_id,
                'timestamp': start_datetime.isoformat(),
                'formatted_time': row.formatted_time,
                'content': chunk,
                'contextualized_content': contextualized_chunk,
                'chunk_index': i,
                'topic': row.topic,
                'speaker': row.speaker,
                'speakers': speakers  # Add full speakers list
            }
            es_documents.append(es_doc)
            
            # Prepare Qdrant point
            qdrant_point = PointStruct(
                id=str(uuid.uuid4()),
                vector=embeddings[i],
                payload={
                    'meeting_id': content_id,
                    'timestamp': start_datetime.isoformat(),
                    'formatted_time': row.formatted_time,
                    'content': chunk,
                    'contextualized_content': contextualized_chunk,
                    'chunk_index': i,
                    'topic': row.topic,
                    'speaker': row.speaker,
                    'speakers': speakers,
                }
            )
            qdrant_points.append(qdrant_point)
        
        return es_documents, qdrant_points

    async def _contextualize_chunks(
        self,
        chunks: List[str],
        doc_content: str
    ) -> List[str]:
        """Add context to chunks using AI"""
        contextualized_chunks = []
        
        # Process first chunk to warm up cache
        messages = [
            system_msg(DOCUMENT_CONTEXT_PROMPT.format(doc_content=doc_content)),
            user_msg(CHUNK_CONTEXT_PROMPT.format(chunk_content=chunks[0]))
        ]
        first_context = await generic_call(messages)
        contextualized_chunks = [first_context]  # Don't concatenate with chunk
        
        # Process remaining chunks concurrently
        async def get_context(chunk):
            messages = [
                system_msg(DOCUMENT_CONTEXT_PROMPT.format(doc_content=doc_content)),
                user_msg(CHUNK_CONTEXT_PROMPT.format(chunk_content=chunk))
            ]
            context = await generic_call(messages)
            return context  # Return only context
        
        remaining_contexts = await asyncio.gather(
            *(get_context(chunk) for chunk in chunks[1:])
        )
        contextualized_chunks.extend(remaining_contexts)
        
        return contextualized_chunks

    async def _index_to_search_engines(
        self,
        es_documents: List[Dict],
        qdrant_points: List[PointStruct]
    ) -> None:
        """Index documents to both search engines"""
        if not es_documents or not qdrant_points:
            return
        
        # Index in batches of 100
        for i in range(0, len(es_documents), 100):
            batch_es = es_documents[i:i+100]
            batch_qdrant = qdrant_points[i:i+100]
            
            # Index to both engines
            self.es.index_documents(batch_es)  # Not async
            await self.qdrant_engine.client.upsert(
                collection_name=self.qdrant_engine.collection_name,
                points=batch_qdrant
            )
        