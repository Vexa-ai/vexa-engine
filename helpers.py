from IPython.display import clear_output, display, Markdown
from functools import wraps
from typing import AsyncIterator

def jupyter_stream_output(markdown: bool = True):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            chunks = []
            async for response in func(*args, **kwargs):
                if isinstance(response, str):
                    chunks.append(response)
                    clear_output(wait=True)
                    if markdown:
                        display(Markdown(''.join(chunks)))
                    else:
                        print(''.join(chunks), flush=True)
                else:
                    return response
            return ''.join(chunks)
        return wrapper
    return decorator