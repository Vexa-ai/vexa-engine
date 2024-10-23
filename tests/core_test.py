import pytest
from unittest.mock import patch, AsyncMock
from ..core import generate_cache_key, get_cached_output, store_output_in_cache, generic_call_, Msg, Session
from psql_models import Output

@pytest.fixture(scope='function')
def db_session():
    """Fixture to provide a clean database session for each test."""
    session = Session()
    yield session
    session.rollback()
    session.close()

def test_generate_cache_key():
    messages = [{'role': 'user', 'content': 'Hello'}]
    model = 'gpt-4o-mini'
    temperature = 0.5
    cache_key = generate_cache_key(messages, model, temperature)
    print(f"Generated cache key: {cache_key}")
    assert isinstance(cache_key, str)
    assert len(cache_key) == 32  # MD5 hash length

def test_store_and_get_cached_output(db_session):
    messages = [{'role': 'user', 'content': 'Hello'}]
    model = 'gpt-4o-mini'
    temperature = 0.5
    max_tokens = 100
    cache_key = generate_cache_key(messages, model, temperature)
    output_text = "Hello, how can I help you?"

    # Store output in cache
    store_output_in_cache(messages, output_text, model, temperature, max_tokens, cache_key)
    print(f"Stored output in cache with key: {cache_key}")

    # Retrieve cached output
    cached_output = get_cached_output(cache_key)
    print(f"Retrieved cached output: {cached_output}")
    assert cached_output == output_text

@pytest.mark.asyncio
async def test_generic_call_with_cache(db_session):
    messages = [Msg(role='user', content='Hello')]
    model = 'gpt-4o-mini'
    temperature = 0.5
    max_tokens = 100

    # Mock acompletion to return a specific result
    with patch('core.acompletion', new_callable=AsyncMock) as mock_acompletion:
        mock_acompletion.return_value.choices = [AsyncMock(message=AsyncMock(content="Hello, how can I help you?"))]

        # Call generic_call_ with use_cache=True
        async for result in generic_call_(messages, model=model, temperature=temperature, max_tokens=max_tokens, use_cache=True):
            print(f"Result from generic_call_: {result}")
            assert result == "Hello, how can I help you?"

        # Ensure acompletion was called
        mock_acompletion.assert_called_once()

        # Check if the result is cached
        cache_key = generate_cache_key([msg.__dict__ for msg in messages], model, temperature)
        cached_output = get_cached_output(cache_key)
        print(f"Cached output after generic_call_: {cached_output}")
        assert cached_output == "Hello, how can I help you?"
