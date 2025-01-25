from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

import pandas as pd
from core import BaseCall, system_msg, user_msg
from prompts import Prompts
prompts = Prompts()



class TopicsMapping(BaseCall):
    formatted_time: str = Field(..., description="time of the input qoute")
    topic: str = Field(..., description="topic of the input qoute")
    
class TopicsExtraction(BaseCall):
    mapping: list[TopicsMapping] = Field(..., description="Mapping of topics to the input text. Aim for equal chunks of time for each topic switch. ")
