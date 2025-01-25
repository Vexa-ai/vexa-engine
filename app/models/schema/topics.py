from pydantic import BaseModel
from typing import List
from app.models.base import BaseCall

class TopicMapping(BaseModel):
    formatted_time: str
    topic: str

class TopicsExtraction(BaseCall):
    topics: List[TopicMapping] 