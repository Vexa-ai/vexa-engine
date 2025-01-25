import pytest
from fastapi.testclient import TestClient
from unittest.mock import AsyncMock, MagicMock, patch
from app import app
from chat import UnifiedChatManager
from qdrant_search import QdrantSearchEngine
from bm25_search import ElasticsearchBM25
from uuid import UUID

TEST_UUID = '12345678-1234-5678-1234-567812345678'

@pytest.fixture
def mock_token_manager():
    with patch('app.token_manager') as mock:
        async def check_token(token):
            if token == "test_token":
                return UUID(TEST_UUID), "Test User"
            raise ValueError("Invalid token")
        mock.check_token = AsyncMock(side_effect=check_token)
        mock.get_user_id = AsyncMock(return_value=UUID(TEST_UUID))
        yield mock

@pytest.fixture
def mock_chat_manager():
    with patch('app.chat_manager') as mock:
        async def mock_chat(*args, **kwargs):
            yield {"chunk": "Test response chunk"}
            yield {
                "thread_id": "test_thread",
                "linked_output": "Test response with [1](/meeting/123)",
                "service_content": {"context": "Test context"}
            }
        mock.chat = mock_chat
        yield mock

@pytest.fixture
def mock_get_user_name():
    with patch('app.get_user_name') as mock:
        mock.return_value = "Test User"
        yield mock

@pytest.fixture
def client(mock_token_manager, mock_chat_manager, mock_get_user_name):
    return TestClient(app)

def test_chat_with_speakers_only(client):
    # Test data
    request_data = {
        "query": "test query",
        "entities": ["Speaker1", "Speaker2"]
    }
    
    # Make request to chat endpoint
    response = client.post(
        "/chat",
        json=request_data,
        headers={"Authorization": "Bearer test_token"}
    )
    
    # Verify response
    assert response.status_code == 200
    assert "text/event-stream" in response.headers["content-type"]
    
    # Parse SSE response
    events = response.content.decode().strip().split("\n\n")
    events = [e.replace("data: ", "") for e in events if e.startswith("data: ")]
    
    # Verify events contain expected data
    assert len(events) > 0
    assert "chunk" in events[0] or "thread_id" in events[0]

def test_chat_with_empty_speakers(client):
    request_data = {
        "query": "test query",
        "entities": []
    }
    
    response = client.post(
        "/chat",
        json=request_data,
        headers={"Authorization": "Bearer test_token"}
    )
    
    assert response.status_code == 200

def test_chat_without_speakers(client):
    request_data = {
        "query": "test query"
    }
    
    response = client.post(
        "/chat",
        json=request_data,
        headers={"Authorization": "Bearer test_token"}
    )
    
    assert response.status_code == 200

def test_chat_invalid_auth(client, mock_token_manager):
    request_data = {
        "query": "test query",
        "entities": ["Speaker1"]
    }
    
    response = client.post(
        "/chat",
        json=request_data,
        headers={"Authorization": "Bearer invalid_token"}
    )
    
    assert response.status_code == 401

@pytest.mark.asyncio
async def test_chat_integration():
    # Create real instances for integration test
    qdrant_engine = AsyncMock(spec=QdrantSearchEngine)
    es_engine = MagicMock(spec=ElasticsearchBM25)
    session = AsyncMock()
    
    # Configure mock search results with timestamp
    mock_results = [
        {
            'meeting_id': 'meeting1',
            'formatted_time': '10:00',
            'content': 'Test content 1',
            'speakers': ['Speaker1'],
            'combined_score': 0.8,
            'sources': ['semantic'],
            'timestamp': '2024-03-21T10:00:00'
        }
    ]
    
    qdrant_engine.search.return_value = mock_results
    es_engine.search.return_value = mock_results
    
    # Mock thread manager
    thread_manager = AsyncMock()
    thread_manager.upsert_thread.return_value = "test_thread_id"
    
    # Mock user name lookup
    with patch('chat.get_user_name', return_value="Test User"):
        # Create chat manager instance
        chat_manager = UnifiedChatManager(
            session=session,
            qdrant_engine=qdrant_engine,
            es_engine=es_engine
        )
        chat_manager.thread_manager = thread_manager
        
        # Test chat with speakers
        speakers = ['Speaker1']
        results = []
        async for result in chat_manager.chat(
            user_id=TEST_UUID,
            query='test query',
            entities=speakers
        ):
            results.append(result)
            if 'chunk' not in result:
                assert 'thread_id' in result
                assert 'linked_output' in result
                
                # Verify search was called correctly
                qdrant_engine.search.assert_called_with(
                    'test query',
                    meeting_ids=None,
                    speakers=speakers,
                    limit=300,
                    min_score=0.000
                )
        
        assert len(results) > 0 