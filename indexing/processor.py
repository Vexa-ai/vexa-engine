from typing import List, Dict, Any, Tuple, Optional, Callable
import pandas as pd
from datetime import datetime
import uuid
from qdrant_client.models import PointStruct
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, and_
from vexa import VexaAPI
from elasticsearch import AsyncElasticsearch
from qdrant_client import AsyncQdrantClient
from core import generic_call, system_msg, user_msg
from psql_models import Content, ContentType
from .content_relations import update_child_content
from pydantic_models import TopicsExtraction
from .decorators import handle_processing_errors, with_logging
from .prompts import DOCUMENT_CONTEXT_PROMPT, CHUNK_CONTEXT_PROMPT
from .search_document import SearchDocument
from langchain_text_splitters import RecursiveCharacterTextSplitter
import asyncio
import logging

logger = logging.getLogger(__name__)

class ProcessingError(Exception): pass

class ContentProcessor:
    def __init__(self, qdrant_engine, es_engine, chunk_size: int = 1000, chunk_overlap: int = 200):
        self.qdrant_engine = qdrant_engine
        self.es = es_engine
        self.vexa = VexaAPI()
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=len,
            separators=["\n\n", "\n", ".", "!", "?", ",", " ", ""]
        )

    @handle_processing_errors(ProcessingError)
    @with_logging('info')
    async def process_content(self, content_id: str, user_id: str, token: str, session: AsyncSession) -> None:
        content = await session.get(Content, content_id)
        if not content:
            raise ProcessingError("Content not found")
            
        if content.type == ContentType.NOTE.value:
            await self._process_note_content(content, user_id, session)
        elif content.type == ContentType.MEETING.value:
            await self._process_meeting_content(content, user_id, token, session)
        
        content.is_indexed = True
        await session.commit()

    def _split_note_into_chunks(self, note_text: str) -> List[str]:
        """Split note text into smaller chunks using LangChain's RecursiveCharacterTextSplitter."""
        logger.info(f"Creating chunks from text of length {len(note_text)}")
        if not note_text:
            logger.warning("Empty note text provided")
            return []
            
        chunks = self.text_splitter.split_text(note_text)
        logger.info(f"Created {len(chunks)} chunks")
        for i, chunk in enumerate(chunks):
            logger.debug(f"Chunk {i}: {len(chunk)} chars")
            if i > 0:  # Log overlap with previous chunk
                overlap = set(chunks[i-1][-200:]).intersection(set(chunk[:200]))
                logger.debug(f"Overlap with previous chunk: {len(overlap)} chars")
        return chunks

    @handle_processing_errors()
    async def _process_note_content(self, content: Content, user_id: str, session: AsyncSession) -> None:
        stmt = select(Content.text).where(and_(Content.id == content.id, Content.type == ContentType.NOTE.value))
        note_text = await session.scalar(stmt)
        if not note_text:
            raise ProcessingError("Note text not found")
            
        # Split note into chunks
        chunks = self._split_note_into_chunks(note_text)
        if not chunks:
            logger.warning("No chunks created from note text")
            return
            
        # Contextualize chunks
        doc_content = '\n'.join(chunks)
        contextualized_chunks = await self._contextualize_chunks(chunks, doc_content)
        
        # Prepare and index documents
        es_documents, qdrant_points = await self._prepare_search_documents(
            chunks=chunks,
            contextualized_chunks=contextualized_chunks,
            content_id=str(content.id),
            timestamp=content.timestamp,
            topic_provider=lambda _: 'Note',  # Fixed topic for notes
            speaker_provider=lambda _: str(user_id),  # Fixed speaker for notes
            speakers=[str(user_id)],
            content_type=ContentType.NOTE
        )
        await self._index_to_search_engines(es_documents, qdrant_points)

    @handle_processing_errors()
    async def _process_meeting_content(self, content: Content, user_id: str, token: str, session: AsyncSession) -> None:
        self.vexa.token = token
        
        # Get user info before requesting transcription
        await self.vexa.get_user_info()
        
        # Get transcription with better error handling
        logger.info(f"Requesting transcription for content_id={content.id}, user_id={user_id}")
        transcription_result = await self.vexa.get_transcription(meeting_session_id=str(content.id))
        
        if not transcription_result:
            logger.warning(
                f"No transcription data available for meeting {content.id}. "
                f"User ID: {user_id}, Token: {token[:8]}..."
            )
            # Mark as processed but empty to avoid reprocessing
            content.is_indexed = True
            await session.commit()
            return
        
        try:
            df, formatted_output, start_datetime, speakers, transcript = transcription_result
        except (TypeError, ValueError) as e:
            logger.error(
                f"Invalid transcription data format for meeting {content.id}. "
                f"User ID: {user_id}, Error: {e}"
            )
            raise ProcessingError(f"Invalid transcription data format: {e}")

        if df.empty:
            raise ProcessingError(
                f"No transcription data available for meeting {content.id}. "
                f"User ID: {user_id}"
            )

        es_documents, qdrant_points = await self._merge_meeting_into_chunks(df, str(content.id), start_datetime, speakers)
        await self._index_to_search_engines(es_documents, qdrant_points)

    async def _merge_meeting_into_chunks(self, df: pd.DataFrame, content_id: str, start_datetime: datetime, speakers: List[str]) -> Tuple[List[Dict], List[PointStruct]]:
        # Convert to markdown
        input_text = df[['formatted_time','speaker', 'content']].to_markdown()
        
        # Use RecursiveCharacterTextSplitter with OpenAI's message length limit
        MAX_MESSAGE_LENGTH = 1000000  # Leaving some buffer
        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=MAX_MESSAGE_LENGTH,
            chunk_overlap=0  # No overlap needed since we're sending all chunks in one call
        )
        chunks = text_splitter.split_text(input_text)
        
        # Create messages array with system prompt and multiple user messages
        messages = [system_msg("Extract topics from the following meeting transcript sections. Maintain consistency across sections.")]
        for chunk in chunks:
            messages.append(user_msg(chunk))
        
        # Make single API call with all chunks as separate messages
        topics_result = await TopicsExtraction.call(messages)
        
        # Continue with existing processing
        topics_df = pd.DataFrame([{"formatted_time": m.formatted_time, "topic": m.topic} 
                                for m in topics_result.mapping])
        
        df = df.merge(topics_df, on='formatted_time', how='left')[['formatted_time','topic','speaker','content']].ffill()
        df['speaker_shift'] = (df['speaker']+df['topic'] != df['speaker']+df['topic'].shift(1)).cumsum()
        
        df_grouped = df.groupby('speaker_shift').agg({
            'formatted_time': 'first',
            'speaker': 'first',
            'topic': 'first',
            'content': ' '.join
        }).reset_index()
        
        chunks = (df_grouped['speaker'] + ': ' + df_grouped['content']).tolist()
        doc_content = '\n'.join(chunks)
        contextualized_chunks = await self._contextualize_chunks(chunks, doc_content)
        
        return await self._prepare_search_documents(
            chunks=chunks,
            contextualized_chunks=contextualized_chunks,
            content_id=content_id,
            timestamp=start_datetime,
            topic_provider=lambda i: df_grouped.iloc[i].topic,
            speaker_provider=lambda i: df_grouped.iloc[i].speaker,
            speakers=speakers,
            content_type=ContentType.MEETING
        )

    async def _prepare_search_documents(
        self,
        chunks: List[str],
        contextualized_chunks: List[str],
        content_id: str,
        timestamp: datetime,
        topic_provider: Callable[[int], str],
        speaker_provider: Callable[[int], str],
        speakers: List[str],
        content_type: ContentType
    ) -> Tuple[List[Dict], List[PointStruct]]:
        """Prepare search documents with embeddings for both Elasticsearch and Qdrant."""
        # Generate embeddings
        embeddings = self.qdrant_engine.voyage.embed(texts=contextualized_chunks, model='voyage-3').embeddings
        
        # Create search documents
        search_docs = [
            SearchDocument(
                content_id=content_id,
                timestamp=timestamp,
                chunk=chunk,
                context=context,
                chunk_index=i,
                topic=topic_provider(i),
                speaker=speaker_provider(i),
                speakers=speakers,
                content_type=content_type
            )
            for i, (chunk, context) in enumerate(zip(chunks, contextualized_chunks))
        ]
        
        # Convert to ES docs and Qdrant points
        es_documents = [doc.to_es_doc() for doc in search_docs]
        qdrant_points = [doc.to_qdrant_point(emb) for doc, emb in zip(search_docs, embeddings)]
        
        return es_documents, qdrant_points

    async def _contextualize_chunks(self, chunks: List[str], doc_content: str) -> List[str]:
        messages = [
            system_msg(DOCUMENT_CONTEXT_PROMPT.format(doc_content=doc_content)),
            user_msg(CHUNK_CONTEXT_PROMPT.format(chunk_content=chunks[0]))
        ]
        first_context = await generic_call(messages)
        contextualized_chunks = [first_context]
        
        async def get_context(chunk):
            messages = [
                system_msg(DOCUMENT_CONTEXT_PROMPT.format(doc_content=doc_content)),
                user_msg(CHUNK_CONTEXT_PROMPT.format(chunk_content=chunk))
            ]
            return await generic_call(messages)
        
        remaining_contexts = await asyncio.gather(*(get_context(chunk) for chunk in chunks[1:]))
        contextualized_chunks.extend(remaining_contexts)
        return contextualized_chunks

    async def _index_to_search_engines(self, es_documents: List[Dict], qdrant_points: List[PointStruct]) -> None:
        if not es_documents or not qdrant_points:
            return
        
        for i in range(0, len(es_documents), 100):
            batch_es = es_documents[i:i+100]
            batch_qdrant = qdrant_points[i:i+100]
            
            # Use es_client instead of client
            operations = []
            for doc in batch_es:
                operations.extend([
                    {"index": {"_index": self.es.index_name}},
                    doc
                ])
            await self.es.es_client.bulk(operations=operations)
            
            await self.qdrant_engine.client.upsert(
                collection_name=self.qdrant_engine.collection_name,
                points=batch_qdrant
            )
        