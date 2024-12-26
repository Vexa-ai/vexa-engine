from typing import Dict, List, Tuple
import uuid
from datetime import datetime
from langchain.text_splitter import RecursiveCharacterTextSplitter
from qdrant_client.models import PointStruct

class NoteProcessor:
    """Processes notes for search indexing"""
    
    def __init__(self, qdrant_engine):
        self.qdrant_engine = qdrant_engine
        self.text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=500,  # Target chunk size
            chunk_overlap=50,  # Overlap between chunks
            length_function=len,
            separators=["\n\n", "\n", ".", "!", "?", ",", " ", ""]  # Prioritize natural breaks
        )
    
    async def process_note(
        self,
        note_id: str,
        text: str,
        timestamp: datetime,
        parent_id: str = None
    ) -> Tuple[List[Dict], List[PointStruct]]:
        """Process a note into searchable chunks"""
        
        # 1. Split text into chunks
        chunks = self.text_splitter.split_text(text)
        
        # 2. Add context to each chunk
        contextualized_chunks = []
        for i, chunk in enumerate(chunks):
            # Add position context
            position = "beginning of" if i == 0 else "middle of" if i < len(chunks)-1 else "end of"
            context = f"This chunk appears in the {position} a note"
            
            # Add parent context if available
            if parent_id:
                context += f" associated with meeting {parent_id}"
                
            # Combine context and chunk
            contextualized_chunks.append(f"{context}. {chunk}")
        
        # 3. Get embeddings using Voyage client
        embeddings_response = self.qdrant_engine.voyage.embed(
            texts=contextualized_chunks,
            model='voyage-3'
        )
        embeddings = embeddings_response.embeddings
        
        # 4. Prepare documents for both engines
        es_documents = []
        qdrant_points = []
        
        for i, (chunk, contextualized_chunk) in enumerate(zip(chunks, contextualized_chunks)):
            # Prepare Elasticsearch document
            es_doc = {
                'note_id': note_id,
                'timestamp': timestamp.isoformat(),
                'content': chunk,
                'contextualized_content': contextualized_chunk,
                'chunk_index': i,
                'parent_id': parent_id
            }
            es_documents.append(es_doc)
            
            # Prepare Qdrant point
            qdrant_point = PointStruct(
                id=str(uuid.uuid4()),
                vector=embeddings[i],
                payload={
                    'note_id': note_id,
                    'timestamp': timestamp.isoformat(),
                    'content': chunk,
                    'contextualized_content': contextualized_chunk,
                    'chunk_index': i,
                    'parent_id': parent_id
                }
            )
            qdrant_points.append(qdrant_point)
        
        return es_documents, qdrant_points 