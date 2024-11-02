import instructor
from openai import AsyncOpenAI
from dataclasses import dataclass
from typing import Literal, List, Optional, AsyncGenerator, Tuple, Any, Dict, ClassVar
from pydantic import BaseModel, Field, PrivateAttr
from abc import abstractmethod
import pandas as pd
import json
import requests
import hashlib
import redis
import re
import os
from enum import Enum
from datetime import date
from dotenv import load_dotenv
import tiktoken
from sqlalchemy.orm import sessionmaker, Session
from psql_models import Output, engine
from sqlalchemy import select
from psql_helpers import async_session

# Load environment variables from .env file
load_dotenv()

ANTROPIC_API_KEY = os.getenv('ANTROPIC_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
GROQ_API_KEY = os.getenv('GROQ_API_KEY')
TOGETHERAI_API_KEY = os.getenv('TOGETHERAI_API_KEY')
os.environ["OPENAI_API_KEY"] = OPENAI_API_KEY
os.environ["ANTHROPIC_API_KEY"] = ANTROPIC_API_KEY
os.environ["GROQ_API_KEY"] = GROQ_API_KEY
os.environ["TOGETHERAI_API_KEY"] = TOGETHERAI_API_KEY

if not all([OPENAI_API_KEY]):
    raise ValueError("One or more required API keys are missing from the .env file")

from litellm import acompletion

# Create instances or specific imports
client_generic = AsyncOpenAI(api_key=OPENAI_API_KEY)
client = instructor.from_openai(AsyncOpenAI(api_key=OPENAI_API_KEY))

# Create a session
Session = sessionmaker(bind=engine)
session = Session()

async def get_cached_output(cache_key: str) -> Optional[str]:
    async with async_session() as session:
        cached_output = await session.execute(
            select(Output).filter_by(cache_key=cache_key)
        )
        result = cached_output.scalar_one_or_none()
        if result:
            print("Returning cached result from database")
            return result.output_text
    return None

def generate_cache_key(messages: List[Dict[str, Any]], model: str, temperature: float) -> str:
    messages_str = json.dumps(messages, sort_keys=True)
    key = f"{model}_{temperature}_{messages_str}"
    return hashlib.md5(key.encode()).hexdigest()

async def store_output_in_cache(messages: List[Dict[str, Any]], output_text: str, model: str, temperature: float, max_tokens: int, cache_key: str, force_store: bool = False):
    async with async_session() as session:
        # Check if cache_key already exists
        existing_output = await session.execute(
            select(Output).filter_by(cache_key=cache_key)
        )
        existing_output = existing_output.scalar_one_or_none()

        if existing_output:
            if force_store:
                # Update existing record
                existing_output.output_text = output_text
                existing_output.input_text = str(messages)
                await session.commit()
        else:
            # Create new cache entry
            new_output = Output(
                input_text=str(messages),
                output_text=output_text,
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
                cache_key=cache_key
            )
            session.add(new_output)
            await session.commit()

def get_cached_output_sync(cache_key: str) -> Optional[str]:
    with Session(engine) as session:  # Use synchronous Session
        cached_output = session.execute(
            select(Output).filter_by(cache_key=cache_key)
        )
        result = cached_output.scalar_one_or_none()
        if result:
            print("Returning cached result from database")
            return result.output
    return None

@dataclass
class Msg:
    role: Literal['user','system','assistant']
    content: str
    stage: Optional[str] = None
    service_content: Optional[dict] = None

def assistant_msg(msg, service_content=None):
    return Msg(role='assistant', content=msg, service_content=service_content)
def user_msg(msg):
    return Msg(role='user', content=msg)
def system_msg(msg):
    return Msg(role='system', content=msg)

async def generic_call_(messages: List[Msg], model='default', temperature=0, max_tokens=4000, timeout=60, streaming=False, use_cache=False, force_store=False) -> AsyncGenerator[str, None]:
    if model == 'default': model = "gpt-4o-mini"
    if model == 'turbo': model = "gpt-4o"
    if model == 'claude': model = 'claude-3-5-sonnet-20240620'
    
    messages_dict = [msg.__dict__ for msg in messages]
    for msg in messages_dict:
        if 'service_content' in msg and msg['service_content'] is not None:
            # If service_content is a dict with 'output', use that
            if isinstance(msg['service_content'], dict) and 'output' in msg['service_content']:
                msg['content'] = msg['service_content']['output']
            del msg['service_content']
    messages_dict = [{k: v for k, v in d.items() if k != 'stage'} for d in messages_dict]

    cache_key = generate_cache_key(messages_dict, model, temperature)

    if use_cache:
        cached_output = get_cached_output(cache_key)
        if cached_output:
            yield cached_output
            return

    try:
        results = await acompletion(
            temperature=temperature,
            model=model,
            messages=messages_dict,
            max_tokens=max_tokens,
            timeout=timeout,
            stream=streaming
        )
        
        if (use_cache or force_store) and not streaming:
            await store_output_in_cache(messages_dict, results.choices[0].message.content, model, temperature, max_tokens, cache_key, force_store)

    except Exception as e:
        print(f"Error with provided model {model}. Falling back to default model.")
        model = "gpt-4o-mini"
        results = await acompletion(
            temperature=temperature,
            model=model,
            messages=messages_dict,
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
        
async def generic_call(messages: List[Msg], model='default', temperature=0, max_tokens=4000, timeout=60):
    result = [token async for token in generic_call_(messages, model=model, temperature=temperature, max_tokens=max_tokens, timeout=timeout, streaming=False)]
    print(''.join(result))


async def generic_call_stream(messages: List[Msg], model='default', temperature=0, max_tokens=4000, timeout=60, use_cache=False):
    output = ""
    async for token in generic_call_(messages, model=model, temperature=temperature, max_tokens=max_tokens, timeout=timeout, streaming=True, use_cache=use_cache):
        print(token, end='', flush=True)
        output += token
    print()  # New line after completion
    return output

class BaseCall(BaseModel):
    @classmethod
    async def call(cls, messages: List[Msg], model='default', temperature=0., use_cache=False, force_store=False) -> Any:
        """Call the model without streaming support"""
        if model == 'default':
            model = "gpt-4o-mini"
        if model == 'turbo':
            model = "gpt-4o"

        messages_dict = [msg.__dict__ for msg in messages]
        
        if use_cache or force_store:
            cache_key = generate_cache_key(messages_dict, model, temperature)

        if use_cache:
            cached_output = await get_cached_output(cache_key)
            if cached_output:
                return cls.model_validate_json(cached_output)

        try:
            client = instructor.patch(AsyncOpenAI())
            
            # Regular non-streaming response
            response = await client.chat.completions.create(
                model=model,
                messages=messages_dict,
                temperature=temperature,
                response_model=cls,
                stream=False
            )
            
            if use_cache or force_store:
                await store_output_in_cache(
                    messages_dict, 
                    response.model_dump_json(), 
                    model, 
                    temperature, 
                    4000, 
                    cache_key, 
                    force_store
                )

            return response

        except Exception as e:
            print(f'Error in BaseCall.call_sync: {str(e)}')
            return None
    
    def get(self):
        """Get model as JSON string"""
        return self.model_dump_json(indent=2)
    
    def print(self):
        """Print model as formatted JSON"""
        print(self.get())

    @classmethod
    async def call_stream(cls, messages: List[Msg], model='default', temperature=0.) -> AsyncGenerator[Any, None]:
        """Stream the model response with partial results"""
        if model == 'default':
            model = "gpt-4o-mini"
        if model == 'turbo':
            model = "gpt-4o"

        messages_dict = [msg.__dict__ for msg in messages]
        

        client = instructor.patch(AsyncOpenAI())
        
        # Create streaming response using Partial type
        stream = await client.chat.completions.create(
            model=model,
            messages=messages_dict,
            temperature=temperature,
            response_model=instructor.Partial[cls],
            stream=True
        )
        
        async for partial_response in stream:
            yield partial_response


class MostRelevantToChosen(BaseCall):
    title: Optional[str] = None

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
