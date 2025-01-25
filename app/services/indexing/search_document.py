from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from uuid import UUID

class SearchDocument(BaseModel):
    id: UUID
    content_id: UUID
    text: str
    embedding: Optional[List[float]] = None
    metadata: dict = {}
    created_at: datetime = datetime.utcnow()
    updated_at: datetime = datetime.utcnow()
    
    def to_elastic_doc(self) -> dict:
        return {
            "id": str(self.id),
            "content_id": str(self.content_id),
            "text": self.text,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }
        
    def to_qdrant_doc(self) -> dict:
        return {
            "id": str(self.id),
            "content_id": str(self.content_id),
            "text": self.text,
            "metadata": self.metadata,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "embedding": self.embedding
        } 