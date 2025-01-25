from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import datetime
from uuid import UUID

class ContentResponse(BaseModel):
    content_id: UUID
    text: str
    timestamp: datetime
    score: float
    metadata: Dict[str, Any] = {} 