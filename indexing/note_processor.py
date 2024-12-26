from typing import List, Dict, Any, Tuple, Optional
import uuid
from datetime import datetime
from qdrant_client.models import PointStruct
from langchain_text_splitters import RecursiveCharacterTextSplitter

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
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            length_function=len,
            separators=["\n\n", "\n", ".", "!", "?", ",", " ", ""]
        )
    
    def create_chunks(self, note_text: str) -> List[str]:
        """Create chunks from note text using LangChain's RecursiveCharacterTextSplitter."""
        return self.text_splitter.split_text(note_text)
    
    async def process_note(
        self,
        note_text: str,
        note_id: str,
        timestamp: datetime,
        voyage_client: Any,
        author: str
    ) -> Tuple[List[Dict], List[PointStruct]]:
        """Process a note into searchable chunks with metadata."""
        # 1. Create chunks using LangChain
        chunks = self.create_chunks(note_text)
        
        # Handle empty notes
        if not chunks:
            return [], []
        
        # 2. Contextualize chunks
        doc_content = '\n'.join(chunks)
        contextualized_chunks = []
        
        # Process first chunk to warm up cache
        messages = [
            system_msg(DOCUMENT_CONTEXT_PROMPT.format(doc_content=doc_content)),
            user_msg(CHUNK_CONTEXT_PROMPT.format(chunk_content=chunks[0]))
        ]
        first_context = await generic_call(messages)
        contextualized_chunks = [first_context]
        
        # Process remaining chunks concurrently
        async def get_context(chunk):
            messages = [
                system_msg(DOCUMENT_CONTEXT_PROMPT.format(doc_content=doc_content)),
                user_msg(CHUNK_CONTEXT_PROMPT.format(chunk_content=chunk))
            ]
            context = await generic_call(messages)
            return context
        
        import asyncio
        remaining_contexts = await asyncio.gather(
            *(get_context(chunk) for chunk in chunks[1:])
        )
        contextualized_chunks.extend(remaining_contexts)
        
        # 3. Get embeddings
        embeddings_response = voyage_client.embed(texts=contextualized_chunks, model='voyage-3')
        embeddings = embeddings_response.embeddings
        
        # Ensure we have the same number of embeddings as chunks
        if len(embeddings) != len(chunks):
            raise ValueError(f"Number of embeddings ({len(embeddings)}) does not match number of chunks ({len(chunks)})")
        
        # 4. Prepare documents for both engines
        es_documents = []
        qdrant_points = []
        
        for i, (chunk, contextualized_chunk) in enumerate(zip(chunks, contextualized_chunks)):
            # Prepare Elasticsearch document
            es_doc = {
                'meeting_id': note_id,  # Use meeting_id for compatibility
                'timestamp': timestamp.isoformat(),
                'formatted_time': timestamp.strftime('%Y-%m-%d %H:%M:%S'),  # Add formatted time
                'content': chunk,
                'contextualized_content': contextualized_chunk,
                'chunk_index': i,
                'topic': 'Note',  # Default topic for notes
                'speaker': author,  # Use author as speaker
                'speakers': [author],  # Single speaker for notes
                'author': author
            }
            es_documents.append(es_doc)
            
            # Prepare Qdrant point
            qdrant_point = PointStruct(
                id=str(uuid.uuid4()),
                vector=embeddings[i],
                payload={
                    'meeting_id': note_id,  # Use meeting_id for compatibility
                    'timestamp': timestamp.isoformat(),
                    'formatted_time': timestamp.strftime('%Y-%m-%d %H:%M:%S'),  # Add formatted time
                    'content': chunk,
                    'contextualized_content': contextualized_chunk,
                    'chunk_index': i,
                    'topic': 'Note',  # Default topic for notes
                    'speaker': author,  # Use author as speaker
                    'speakers': [author],  # Single speaker for notes
                    'author': author
                }
            )
            qdrant_points.append(qdrant_point)
        
        return es_documents, qdrant_points 