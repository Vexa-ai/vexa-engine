import instructor
from openai import AsyncOpenAI
from dataclasses import dataclass
from typing import Literal
from typing import List, Optional

from pydantic import BaseModel
from typing import List
from instructor.dsl import Maybe
from typing import AsyncGenerator

from abc import abstractmethod

import pandas as pd
import json
from pydantic import BaseModel, Field
import requests


from typing import List, Optional, Tuple, Any
from pydantic import BaseModel, Field
from graphviz import Digraph
from IPython.display import display
import hashlib
from pydantic import BaseModel, PrivateAttr

import redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

import re
import os


from pydantic import Field
from typing import Literal
from typing import Type, List
from typing import Type, List, Dict, Any
from typing import Type, List, Dict, Tuple, Any
from enum import Enum
from typing import Type, List, Dict, Any, Tuple 
from datetime import date

from graphviz import Digraph
from IPython.display import display
from typing import List, Tuple, Any, Dict, ClassVar

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

ANTROPIC_API_KEY = os.getenv('ANTROPIC_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
GROQ_API_KEY = os.getenv('GROQ_API_KEY')
TOGETHERAI_API_KEY = os.getenv('TOGETHERAI_API_KEY')
import os
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
os.environ["ANTHROPIC_API_KEY"] = ANTROPIC_API_KEY
os.environ["GROQ_API_KEY"] = GROQ_API_KEY
os.environ["TOGETHERAI_API_KEY"] = TOGETHERAI_API_KEY


if not all([OPENAI_API_KEY]):
    raise ValueError("One or more required API keys are missing from the .env file")



from litellm import acompletion



#| export 
# Create instances or specific imports
client_generic = AsyncOpenAI(api_key=OPENAI_API_KEY)
client = instructor.from_openai(AsyncOpenAI(api_key=OPENAI_API_KEY))





#| export 
@dataclass
class Msg:
    role: Literal['user','system','assistant']
    content:str
    stage:  Optional[str] = None
    service_content: Optional[str] = None

def assistant_msg(msg,service_content=None):
    return Msg(role='assistant',content=msg, service_content=service_content)
def user_msg(msg):
    return Msg(role='user',content=msg)
def system_msg(msg):
    return Msg(role='system',content=msg)

async def generic_call_(messages: List[Msg], model='default', temperature=0, max_tokens=4000, timeout=60, streaming=False) -> AsyncGenerator[str, None]:
    if model == 'default': model = "gpt-4o-mini"
    if model == 'turbo': model = "gpt-4o"
    if model == 'claude': model = 'claude-3-5-sonnet-20240620'
    
    messages = [msg.__dict__ for msg in messages]
    for msg in messages:
        if 'service_content' in msg and msg['service_content'] is not None:
            msg['content'] = msg['service_content']
            del msg['service_content']
    messages = [{k: v for k, v in d.items() if k != 'stage'} for d in messages]
    
    try:
        results = await acompletion(
            temperature=temperature,
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            timeout=timeout,
            stream=streaming
        )
    except Exception as e:
        print(f"Error with provided model {model}. Falling back to default model.")
        model = "gpt-4o-mini"
        results = await acompletion(
            temperature=temperature,
            model=model,
            messages=messages,
            max_tokens=max_tokens,
            timeout=timeout,
            stream=streaming
        )
    
    if streaming:
        async for chunk in results:
            if chunk.choices[0].delta.content is not None:
                yield chunk.choices[0].delta.content
    else:
        yield results.choices[0].message.content
        
async def generic_call_stream(messages: List[Msg], model='default', temperature=0, max_tokens=4000, timeout=60):
    output = ""
    async for token in generic_call_(messages, model=model, temperature=temperature, max_tokens=max_tokens, timeout=timeout, streaming=True):
        print(token, end='', flush=True)
        output += token
    print()  # New line after completion
    return output

# For non-streaming usage:
async def generic_call(messages: List[Msg], model='default', temperature=0, max_tokens=4000, timeout=60):
    result = [token async for token in generic_call_(messages, model=model, temperature=temperature, max_tokens=max_tokens, timeout=timeout, streaming=False)]
    print(''.join(result))

class BaseCall(BaseModel):
    @classmethod
    def _generate_cache_key(cls, messages: List[Msg], model: str, temperature: float) -> str:
        messages_str = json.dumps([msg.__dict__ for msg in messages], sort_keys=True)
        key = f"{model}_{temperature}_{messages_str}"
        return hashlib.md5(key.encode()).hexdigest()

    @classmethod
    async def call(cls, messages: List[Msg], model='default', temperature=0., return_raw=True, force=False, use_cache=False) -> Tuple[Any, Any]:
        if model == 'default':
            model = "gpt-4o-mini"
        if model == 'turbo':
            model = "gpt-4o"

        if use_cache:
            cache_key = cls._generate_cache_key(messages, model, temperature)
            if not force:
                cached_result = redis_client.get(cache_key)
                if cached_result:
                    print("Returning cached result from Redis")
                    return cls.parse_raw(cached_result), None
            
        messages_dict = [msg.__dict__ for msg in messages]
        
        try:
            completion, raw_response = await client.chat.completions.create_with_completion(
                temperature=temperature,
                model=model,
                response_model=cls,
                max_retries=3,
                messages=messages_dict,
            )
            if use_cache:
                redis_client.set(cache_key, completion.json())
            return (completion, raw_response) if return_raw else completion
        except Exception as e:
            print('errored')
            return e.response

    @classmethod
    async def maybe_call(cls, messages: List[Msg], model='default', temperature=0., return_raw=True, force=False, use_cache=False) -> Tuple[Optional[Any], Any]:
        if model == 'default':
            model = "gpt-4o-mini"
        if model == 'turbo':
            model = "gpt-4o"

        if use_cache:
            cache_key = cls._generate_cache_key(messages, model, temperature)
            if not force:
                cached_result = redis_client.get(cache_key)
                if cached_result:
                    print("Returning cached result from Redis")
                    return cls.parse_raw(cached_result), None
            
        messages_dict = [msg.__dict__ for msg in messages]
        
        try:
            MaybeResult = Maybe(cls)
            completion, raw_response = await client.chat.completions.create_with_completion(
                temperature=temperature,
                model=model,
                response_model=MaybeResult,
                max_retries=3,
                messages=messages_dict,
            )
            
            if completion.result:
                if use_cache:
                    redis_client.set(cache_key, completion.result.json())
                return (completion.result, raw_response) if return_raw else completion.result
            else:
                return (None, raw_response) if return_raw else None
        except Exception as e:
            raw_response = getattr(e, 'response', None)
            print(f'errored raw_response {raw_response}')
            raise e
        
    def get(self):
        return self.model_dump_json(indent=2)
    
    def print(self):
        print(self.get())

class MostRelevantToChosen(BaseCall):
    title: Optional[str] = None
    
    
    
import tiktoken

def count_tokens(text: str, model: str = "gpt-4o-mini") -> int:
    """
    Count the number of tokens in a given string.
    
    Args:
    text (str): The input string to tokenize.
    model (str): The name of the model to use for tokenization (default: "gpt-4o-mini").
    
    Returns:
    int: The number of tokens in the input string.
    """
    try:
        encoding = tiktoken.encoding_for_model(model)
    except KeyError:
        print(f"Model {model} not found. Falling back to cl100k_base encoding.")
        encoding = tiktoken.get_encoding("cl100k_base")
    
    return len(encoding.encode(text))
    
    
def clean_text(text: str) -> str:
    return re.sub(' +', ' ', text)