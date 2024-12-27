from typing import List, Dict, Any, Tuple, Optional
import uuid
from datetime import datetime
import logging
from qdrant_client.models import PointStruct
from langchain_text_splitters import RecursiveCharacterTextSplitter

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Mock functions for testing
async def generic_call(messages):
    return "Test context"

def system_msg(msg):
    return {"role": "system", "content": msg}

def user_msg(msg):
    return {"role": "user", "content": msg}

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

class NoteProcessor:
    def __init__(self, chunk_size: int = 1000, chunk_overlap: int = 200):
        logger.info(f"Initializing NoteProcessor with chunk_size={chunk_size}, chunk_overlap={chunk_overlap}")
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=len,
            separators=["\n\n", "\n", ".", "!", "?", ",", " ", ""]
        )
        logger.debug("Text splitter initialized with separators: [\\n\\n, \\n, ., !, ?, ,, <space>, ]")
    
    def create_chunks(self, note_text: str) -> List[str]:
        """Create chunks from note text using LangChain's RecursiveCharacterTextSplitter."""
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
    
    async def process_note(
        self,
        note_text: str,
        note_id: str,
        timestamp: datetime,
        voyage_client: Any,
        author: str
    ) -> Tuple[List[Dict], List[PointStruct]]:
        """Process a note into searchable chunks with metadata."""
        logger.info(f"Processing note: id={note_id}, length={len(note_text)}, author={author}")
        
        # 1. Create chunks using LangChain
        logger.debug("Step 1: Creating chunks")
        chunks = self.create_chunks(note_text)
        
        # Handle empty notes
        if not chunks:
            logger.warning("No chunks created from note text")
            return [], []
        
        # 2. Contextualize chunks
        logger.debug("Step 2: Contextualizing chunks")
        doc_content = '\n'.join(chunks)
        logger.debug(f"Combined document length: {len(doc_content)} chars")
        contextualized_chunks = []
        
        # Process first chunk to warm up cache
        logger.debug("Processing first chunk for context")
        messages = [
            system_msg(DOCUMENT_CONTEXT_PROMPT.format(doc_content=doc_content)),
            user_msg(CHUNK_CONTEXT_PROMPT.format(chunk_content=chunks[0]))
        ]
        first_context = await generic_call(messages)
        contextualized_chunks = [first_context]
        logger.debug(f"First chunk context length: {len(first_context)}")
        
        # Process remaining chunks concurrently
        if len(chunks) > 1:
            logger.debug(f"Processing remaining {len(chunks)-1} chunks concurrently")
            
            async def get_context(chunk):
                messages = [
                    system_msg(DOCUMENT_CONTEXT_PROMPT.format(doc_content=doc_content)),
                    user_msg(CHUNK_CONTEXT_PROMPT.format(chunk_content=chunk))
                ]
                context = await generic_call(messages)
                logger.debug(f"Generated context of length {len(context)} for chunk")
                return context
            
            import asyncio
            remaining_contexts = await asyncio.gather(
                *(get_context(chunk) for chunk in chunks[1:])
            )
            contextualized_chunks.extend(remaining_contexts)
            logger.debug(f"Processed {len(remaining_contexts)} additional chunks")
        
        # 3. Get embeddings
        logger.debug("Step 3: Generating embeddings")
        embeddings_response = voyage_client.embed(texts=contextualized_chunks, model='voyage-3')
        embeddings = embeddings_response.embeddings
        logger.info(f"Generated {len(embeddings)} embeddings")
        
        # Ensure we have the same number of embeddings as chunks
        if len(embeddings) != len(chunks):
            error_msg = f"Number of embeddings ({len(embeddings)}) does not match number of chunks ({len(chunks)})"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # 4. Prepare documents for both engines
        logger.debug("Step 4: Preparing documents for search engines")
        es_documents = []
        qdrant_points = []
        
        for i, (chunk, contextualized_chunk) in enumerate(zip(chunks, contextualized_chunks)):
            # Prepare Elasticsearch document
            es_doc = {
                'meeting_id': note_id,  # Use meeting_id for compatibility
                'timestamp': timestamp.isoformat(),
                'formatted_time': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'content': chunk,
                'contextualized_content': contextualized_chunk,
                'chunk_index': i,
                'topic': 'Note',
                'speaker': author,
                'speakers': [author],
                'author': author
            }
            es_documents.append(es_doc)
            logger.debug(f"Created ES document {i}: content_length={len(chunk)}, context_length={len(contextualized_chunk)}")
            
            # Prepare Qdrant point
            point_id = str(uuid.uuid4())
            qdrant_point = PointStruct(
                id=point_id,
                vector=embeddings[i],
                payload={
                    'meeting_id': note_id,
                    'timestamp': timestamp.isoformat(),
                    'formatted_time': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                    'content': chunk,
                    'contextualized_content': contextualized_chunk,
                    'chunk_index': i,
                    'topic': 'Note',
                    'speaker': author,
                    'speakers': [author],
                    'author': author
                }
            )
            qdrant_points.append(qdrant_point)
            logger.debug(f"Created Qdrant point {i}: id={point_id}, vector_dim={len(embeddings[i])}")
        
        logger.info(f"Processing complete: {len(es_documents)} ES docs, {len(qdrant_points)} Qdrant points")
        return es_documents, qdrant_points 