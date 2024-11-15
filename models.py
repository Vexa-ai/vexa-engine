from datetime import datetime
from typing import Optional
from pydantic import BaseModel
import uuid

class TokenData(BaseModel):
    token: str
    user_id: uuid.UUID
    user_name: str
    
class VexaAPIError(Exception):
    """Custom exception for Vexa API errors"""
    pass 