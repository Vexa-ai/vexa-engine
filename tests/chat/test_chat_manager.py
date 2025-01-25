import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from chat import UnifiedChatManager, UnifiedContextProvider
from qdrant_search import QdrantSearchEngine
from bm25_search import ElasticsearchBM25
from uuid import UUID

@pytest.fixture
def mock_qdrant_engine():
    return AsyncMock(spec=QdrantSearchEngine)

@pytest.fixture
def mock_es_engine():
    return MagicMock(spec=ElasticsearchBM25)

@pytest.fixture
def mock_session():
    return AsyncMock()

@pytest.fixture
def mock_thread_manager():
    manager = AsyncMock()
    manager.upsert_thread.return_value = "test_thread_id"
    return manager

@pytest.fixture
def mock_get_user_name():
    with patch('chat.get_user_name') as mock:
        mock.return_value = "Test User"
        yield mock

@pytest.fixture
def chat_manager(mock_qdrant_engine, mock_es_engine, mock_session, mock_thread_manager, mock_get_user_name):
    manager = UnifiedChatManager(
        session=mock_session,
        qdrant_engine=mock_qdrant_engine,
        es_engine=mock_es_engine
    )
    manager.thread_manager = mock_thread_manager
    return manager

@pytest.mark.asyncio
async def test_chat_with_speakers_only(chat_manager, mock_qdrant_engine, mock_es_engine):
    # Mock search results
    mock_search_results = [
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
    
    # Configure mock search engines
    mock_qdrant_engine.search.return_value = mock_search_results
    mock_es_engine.search.return_value = mock_search_results
    
    # Test chat with only speakers parameter
    speakers = ['Speaker1']
    results = []
    async for result in chat_manager.chat(
        user_id='test_user',
        query='test query',
        entities=speakers
    ):
        results.append(result)
        if 'chunk' not in result:
            # Verify search was called with correct parameters
            mock_qdrant_engine.search.assert_called_with(
                'test query',
                meeting_ids=None,
                speakers=speakers,
                limit=300,
                min_score=0.000
            )
    
    assert len(results) > 0

@pytest.mark.asyncio
async def test_chat_context_provider_selection(chat_manager):
    # Mock search results
    mock_results = [
        {
            'meeting_id': 'meeting1',
            'formatted_time': '10:00',
            'content': 'Test content 1',
            'speakers': ['Speaker1'],
            'timestamp': '2024-03-21T10:00:00'
        }
    ]
    
    chat_manager.unified_context_provider.qdrant_engine.search.return_value = mock_results
    chat_manager.unified_context_provider.es_engine.search.return_value = mock_results
    
    # Test that UnifiedContextProvider is used when no meeting_ids
    speakers = ['Speaker1']
    async for _ in chat_manager.chat(
        user_id='test_user',
        query='test query',
        entities=speakers
    ):
        assert isinstance(
            chat_manager.unified_context_provider,
            UnifiedContextProvider
        )
        break

@pytest.mark.asyncio
async def test_chat_with_empty_speakers(chat_manager, mock_qdrant_engine, mock_es_engine):
    # Mock search results with timestamp
    mock_results = [
        {
            'meeting_id': 'meeting1',
            'formatted_time': '10:00',
            'content': 'Test content 1',
            'speakers': ['Speaker1'],
            'timestamp': '2024-03-21T10:00:00'
        }
    ]
    
    mock_qdrant_engine.search.return_value = mock_results
    mock_es_engine.search.return_value = mock_results
    
    # Test chat with empty speakers list
    results = []
    async for result in chat_manager.chat(
        user_id='test_user',
        query='test query',
        entities=[]
    ):
        results.append(result)
        if 'chunk' not in result:
            # Verify search was called with None for empty speakers list
            mock_qdrant_engine.search.assert_called_with(
                'test query',
                meeting_ids=None,
                speakers=None,
                limit=300,
                min_score=0.000
            )
    
    assert len(results) > 0

@pytest.mark.asyncio
async def test_chat_with_multiple_speakers(chat_manager, mock_qdrant_engine, mock_es_engine):
    # Mock search results with timestamp
    mock_results = [
        {
            'meeting_id': 'meeting1',
            'formatted_time': '10:00',
            'content': 'Test content 1',
            'speakers': ['Speaker1', 'Speaker2'],
            'timestamp': '2024-03-21T10:00:00'
        }
    ]
    
    mock_qdrant_engine.search.return_value = mock_results
    mock_es_engine.search.return_value = mock_results
    
    # Test chat with multiple speakers
    speakers = ['Speaker1', 'Speaker2']
    results = []
    async for result in chat_manager.chat(
        user_id='test_user',
        query='test query',
        entities=speakers
    ):
        results.append(result)
        if 'chunk' not in result:
            # Verify search was called with multiple speakers
            mock_qdrant_engine.search.assert_called_with(
                'test query',
                meeting_ids=None,
                speakers=speakers,
                limit=300,
                min_score=0.000
            )
    
    assert len(results) > 0

@pytest.mark.asyncio
async def test_chat_output_linking(chat_manager, mock_qdrant_engine, mock_es_engine):
    # Mock search results with meeting references and timestamp
    mock_results = [
        {
            'meeting_id': 'meeting1',
            'formatted_time': '10:00',
            'content': 'Meeting 1 content',
            'speakers': ['Speaker1'],
            'combined_score': 0.8,
            'sources': ['semantic'],
            'timestamp': '2024-03-21T10:00:00'
        }
    ]
    
    mock_qdrant_engine.search.return_value = mock_results
    mock_es_engine.search.return_value = mock_results
    
    # Mock thread manager to return specific thread ID
    chat_manager.thread_manager.upsert_thread.return_value = "test_thread_id"
    
    # Test that output is generated
    found_output = False
    async for result in chat_manager.chat(
        user_id='test_user',
        query='test query',
        entities=['Speaker1']
    ):
        if 'linked_output' in result:
            assert isinstance(result['linked_output'], str)
            assert len(result['linked_output']) > 0
            found_output = True
            break
    
    assert found_output, "No output found in chat results" 