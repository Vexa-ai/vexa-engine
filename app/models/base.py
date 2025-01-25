from pydantic import BaseModel
from typing import List, Any
import instructor
from openai import AsyncOpenAI
from app.core.llm_functions import Msg

class BaseCall(BaseModel):
    @classmethod
    async def call(cls, messages: List[Msg], model='default', temperature=0., use_cache=False, force_store=False) -> Any:
        """Call the model without streaming support"""
        if model == 'default':
            model = "gpt-4o-mini"
        if model == 'turbo':
            model = "gpt-4o"

        # Fix message format for new OpenAI API
        messages_dict = []
        for msg in messages:
            # Convert content to string if it's not already
            content = str(msg.content) if isinstance(msg.content, (list, dict)) else msg.content
            messages_dict.append({
                'role': msg.role,
                'content': content
            })

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
    async def call_stream(cls, messages: List[Msg], model='default', temperature=0.) -> Any:
        """Stream the model response with partial results"""
        if model == 'default':
            model = "gpt-4o-mini"
        if model == 'turbo':
            model = "gpt-4o"

        # Fix message format for new OpenAI API
        messages_dict = []
        for msg in messages:
            # Convert content to string if it's not already
            content = str(msg.content) if isinstance(msg.content, (list, dict)) else msg.content
            messages_dict.append({
                'role': msg.role,
                'content': content
            })

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