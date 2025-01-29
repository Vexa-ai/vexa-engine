import pytest
import pytest_asyncio
from chat_manager import ChatManager
from uuid import UUID
from unittest.mock import patch, AsyncMock
from typing import Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from qdrant_search import QdrantSearchEngine
from bm25_search import ElasticsearchBM25
import os
from aiohttp.client_exceptions import ClientPayloadError

class AsyncSessionMock:
    async def __aenter__(self):
        return self
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

class AsyncIteratorMock:
    def __init__(self, items):
        self.items = items
    def __aiter__(self):
        return self
    async def __anext__(self):
        try:
            return self.items.pop(0)
        except IndexError:
            raise StopAsyncIteration

@pytest_asyncio.fixture
async def search_engines():
    qdrant_engine = QdrantSearchEngine(os.getenv('VOYAGE_API_KEY'))
    es_engine = ElasticsearchBM25()
    await es_engine.initialize()
    return qdrant_engine, es_engine

@pytest_asyncio.fixture
async def chat_manager(search_engines):
    qdrant_engine, es_engine = search_engines
    return await ChatManager.create(qdrant_engine=qdrant_engine, es_engine=es_engine)

@pytest.mark.asyncio
async def test_chat_streaming_response(chat_manager):
    # Test streaming response with chunks
    expected_chunks = [
        {"chunk": "Processing"},
        {"chunk": " your"},
        {"chunk": " request"},
        {
            "thread_id": "test-123",
            "output": "Processing your request",
            "linked_output": "Processing your request with context",
            "service_content": {"context": "test context"}
        }
    ]
    
    async def mock_unified_chat(*args, **kwargs):
        return AsyncIteratorMock(expected_chunks[:])
    
    with patch('chat.UnifiedChatManager.chat', new=mock_unified_chat):
        responses = []
        async for response in chat_manager.chat(
            user_id="test-user",
            query="test query"
        ):
            responses.append(response)
        
        assert len(responses) == 4
        assert all("chunk" in r or "thread_id" in r for r in responses)
        assert responses[-1]["output"] == "Processing your request"

@pytest.mark.asyncio
async def test_chat_transfer_encoding_error(chat_manager):
    # Test handling of ClientPayloadError (which includes transfer encoding errors)
    async def mock_unified_chat(*args, **kwargs):
        raise ClientPayloadError("Not enough data for satisfy transfer length header.")
    
    with patch('chat.UnifiedChatManager.chat', new=mock_unified_chat):
        responses = []
        async for response in chat_manager.chat(
            user_id="test-user",
            query="test query"
        ):
            responses.append(response)
        
        assert len(responses) == 1
        assert "error" in responses[0]
        assert "Not enough data" in responses[0]["error"]

@pytest.mark.asyncio
async def test_chat_with_content_scope(chat_manager):
    # Test chat with content_ids scope
    content_id = UUID('580bb613-00de-4c4a-b048-2c2576fcdef0')
    expected_response = {
        "thread_id": "test-123",
        "output": "Response",
        "linked_output": f"Response with context from {content_id}",
        "service_content": {"context": f"Content from {content_id}"}
    }
    
    async def mock_unified_chat(*args, **kwargs):
        assert kwargs.get('content_id') == content_id
        return AsyncIteratorMock([expected_response])
    
    with patch('chat.UnifiedChatManager.chat', new=mock_unified_chat):
        responses = []
        async for response in chat_manager.chat(
            user_id="test-user",
            query="test query",
            content_id=content_id
        ):
            responses.append(response)
        
        assert len(responses) == 1
        assert responses[0]["linked_output"].endswith(str(content_id))

@pytest.mark.asyncio
async def test_edit_and_continue_streaming(chat_manager):
    # Test edit and continue with streaming response
    thread_id = "test-123"
    expected_chunks = [
        {"chunk": "Editing"},
        {"chunk": " and"},
        {"chunk": " continuing"},
        {
            "thread_id": thread_id,
            "output": "Edited response",
            "service_content": {"context": "Updated context"}
        }
    ]
    
    async def mock_edit(*args, **kwargs):
        assert kwargs.get('thread_id') == thread_id
        assert kwargs.get('message_index') == 1
        return AsyncIteratorMock(expected_chunks[:])
    
    with patch('chat.UnifiedChatManager.edit_and_continue', new=mock_edit):
        responses = []
        async for response in chat_manager.edit_and_continue(
            user_id="test-user",
            thread_id=thread_id,
            message_index=1,
            new_content="edited content"
        ):
            responses.append(response)
        
        assert len(responses) == 4
        assert responses[-1]["thread_id"] == thread_id
        assert responses[-1]["output"] == "Edited response"

@pytest.mark.asyncio
async def test_chat_model_params(chat_manager):
    # Test chat with model parameters
    async def mock_unified_chat(*args, **kwargs):
        assert kwargs.get('model') == "gpt-4o-mini"
        assert kwargs.get('temperature') == 0.7
        return AsyncIteratorMock([{"chunk": "Response"}])
    
    with patch('chat.UnifiedChatManager.chat', new=mock_unified_chat):
        responses = []
        async for response in chat_manager.chat(
            user_id="test-user",
            query="test query",
            model="gpt-4o-mini",
            temperature=0.7
        ):
            responses.append(response)
        
        assert len(responses) == 1 