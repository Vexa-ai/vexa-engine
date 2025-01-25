from typing import List, Optional, AsyncGenerator, Dict, Any
from dataclasses import dataclass
from litellm import acompletion
import json
import hashlib
import os

@dataclass
class Msg:
    role: str
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
            if isinstance(msg['service_content'], dict) and 'output' in msg['service_content']:
                msg['content'] = msg['service_content']['output']
            del msg['service_content']
    messages_dict = [{k: v for k, v in d.items() if k != 'stage'} for d in messages_dict]

    try:
        results = await acompletion(
            temperature=temperature,
            model=model,
            messages=messages_dict,
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
    return ''.join(result)

async def generic_call_stream(messages: List[Msg], model='default', temperature=0, max_tokens=4000, timeout=60, use_cache=False):
    output = ""
    async for token in generic_call_(messages, model=model, temperature=temperature, max_tokens=max_tokens, timeout=timeout, streaming=True, use_cache=use_cache):
        print(token, end='', flush=True)
        output += token
    print()  # New line after completion
    return output 