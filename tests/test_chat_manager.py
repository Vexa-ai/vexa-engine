import pytest
import pytest_asyncio
from chat_manager import ChatManager
from uuid import UUID, uuid4
from unittest.mock import patch, MagicMock, AsyncMock
import asyncio
from typing import Dict, Any, AsyncGenerator

@pytest_asyncio.fixture
async def chat_manager():
    # Mock search engines
    mock_qdrant = MagicMock()
    mock_es = MagicMock()
    return await ChatManager.create(qdrant_engine=mock_qdrant, es_engine=mock_es)

@pytest.mark.asyncio
async def test_chat_basic(chat_manager):
    # Mock expected chat response
    expected_chunks = [
        {"chunk": "Hello"},
        {"chunk": " world"},
        {
            "thread_id": "test-thread-id",
            "output": "Hello world",
            "linked_output": "Hello world",
            "service_content": {
                "output": "Hello world",
                "context": "Test context"
            }
        }
    ]
    
    # Mock UnifiedChatManager.chat
    async def mock_chat(*args, **kwargs):
        for chunk in expected_chunks:
            yield chunk
    
    with patch.object(chat_manager.unified_chat, 'chat', side_effect=mock_chat):
        responses = []
        async for response in chat_manager.chat(
            user_id="test-user",
            query="test query"
        ):
            responses.append(response)
        
        assert responses == expected_chunks
        
@pytest.mark.asyncio
async def test_chat_with_content_ids(chat_manager):
    content_ids = [UUID('580bb613-00de-4c4a-b048-2c2576fcdef0')]
    expected_chunks = [
        {"chunk": "Response"},
        {
            "thread_id": "test-thread-id",
            "output": "Response",
            "linked_output": "Response with [1](/content/580bb613-00de-4c4a-b048-2c2576fcdef0)",
            "service_content": {
                "output": "Response with [1](/content/580bb613-00de-4c4a-b048-2c2576fcdef0)",
                "context": "Test context"
            }
        }
    ]
    
    async def mock_chat(*args, **kwargs):
        assert kwargs.get('content_ids') == content_ids
        for chunk in expected_chunks:
            yield chunk
            
    with patch.object(chat_manager.unified_chat, 'chat', side_effect=mock_chat):
        responses = []
        async for response in chat_manager.chat(
            user_id="test-user",
            query="test query",
            content_ids=content_ids
        ):
            responses.append(response)
            
        assert responses == expected_chunks

@pytest.mark.asyncio
async def test_chat_with_thread_continuation(chat_manager):
    thread_id = "test-thread-id"
    expected_chunks = [
        {"chunk": "Continued"},
        {
            "thread_id": thread_id,
            "output": "Continued response",
            "linked_output": "Continued response",
            "service_content": {
                "output": "Continued response",
                "context": "Previous context"
            }
        }
    ]
    
    async def mock_chat(*args, **kwargs):
        assert kwargs.get('thread_id') == thread_id
        for chunk in expected_chunks:
            yield chunk
            
    with patch.object(chat_manager.unified_chat, 'chat', side_effect=mock_chat):
        responses = []
        async for response in chat_manager.chat(
            user_id="test-user",
            query="follow up",
            thread_id=thread_id
        ):
            responses.append(response)
            
        assert responses == expected_chunks

@pytest.mark.asyncio
async def test_edit_and_continue(chat_manager):
    thread_id = "test-thread-id"
    message_index = 1
    new_content = "edited message"
    
    expected_chunks = [
        {"chunk": "Edited"},
        {
            "thread_id": thread_id,
            "output": "Edited response",
            "service_content": {
                "output": "Edited response",
                "context": "Updated context"
            }
        }
    ]
    
    async def mock_edit(*args, **kwargs):
        assert kwargs.get('thread_id') == thread_id
        assert kwargs.get('message_index') == message_index
        assert kwargs.get('new_content') == new_content
        for chunk in expected_chunks:
            yield chunk
            
    with patch.object(chat_manager.unified_chat, 'edit_and_continue', side_effect=mock_edit):
        responses = []
        async for response in chat_manager.edit_and_continue(
            user_id="test-user",
            thread_id=thread_id,
            message_index=message_index,
            new_content=new_content
        ):
            responses.append(response)
            
        assert responses == expected_chunks 