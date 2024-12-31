import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from hybrid_search import hybrid_search
from qdrant_search import QdrantSearchEngine
from bm25_search import ElasticsearchBM25

@pytest.fixture
def mock_qdrant_engine():
    engine = AsyncMock(spec=QdrantSearchEngine)
    engine.search.return_value = [
        {
            'meeting_id': 'meeting1',
            'formatted_time': '10:00',
            'content': 'Test content 1',
            'speakers': ['Speaker1', 'Speaker2']
        },
        {
            'meeting_id': 'meeting2',
            'formatted_time': '11:00',
            'content': 'Test content 2',
            'speakers': ['Speaker3']
        }
    ]
    return engine

@pytest.fixture
def mock_es_engine():
    engine = MagicMock(spec=ElasticsearchBM25)
    engine.search.return_value = [
        {
            'meeting_id': 'meeting1',
            'formatted_time': '10:00',
            'content': 'Test content 1',
            'speakers': ['Speaker1', 'Speaker2']
        },
        {
            'meeting_id': 'meeting3',
            'formatted_time': '12:00',
            'content': 'Test content 3',
            'speakers': ['Speaker1']
        }
    ]
    return engine

@pytest.mark.asyncio
async def test_hybrid_search_with_speakers_only(mock_qdrant_engine, mock_es_engine):
    # Test search with only speakers parameter
    speakers = ['Speaker1']
    result = await hybrid_search(
        query="test query",
        qdrant_engine=mock_qdrant_engine,
        es_engine=mock_es_engine,
        speakers=speakers
    )
    
    # Verify both engines were called with speakers parameter
    mock_qdrant_engine.search.assert_called_once_with(
        "test query",
        meeting_ids=None,
        speakers=speakers,
        limit=300,
        min_score=0.000
    )
    
    mock_es_engine.search.assert_called_once_with(
        "test query",
        meeting_ids=None,
        speakers=speakers,
        k=300
    )

@pytest.mark.asyncio
async def test_hybrid_search_multiple_speakers(mock_qdrant_engine, mock_es_engine):
    speakers = ['Speaker1', 'Speaker2']
    result = await hybrid_search(
        query="test query",
        qdrant_engine=mock_qdrant_engine,
        es_engine=mock_es_engine,
        speakers=speakers
    )
    
    # Verify search was called with multiple speakers
    mock_qdrant_engine.search.assert_called_once_with(
        "test query",
        meeting_ids=None,
        speakers=speakers,
        limit=300,
        min_score=0.000
    )

@pytest.mark.asyncio
async def test_hybrid_search_empty_speakers(mock_qdrant_engine, mock_es_engine):
    # Test with empty speakers list
    result = await hybrid_search(
        query="test query",
        qdrant_engine=mock_qdrant_engine,
        es_engine=mock_es_engine,
        speakers=[]
    )
    
    # Should return all results when no speakers specified
    assert len(result['results']) > 0
    
    # Verify engines were called with empty speakers list
    mock_qdrant_engine.search.assert_called_once_with(
        "test query",
        meeting_ids=None,
        speakers=None,  # Empty list becomes None
        limit=300,
        min_score=0.000
    )

@pytest.mark.asyncio
async def test_hybrid_search_nonexistent_speaker(mock_qdrant_engine, mock_es_engine):
    # Configure engines to return empty results for non-existent speaker
    mock_qdrant_engine.search.return_value = []
    mock_es_engine.search.return_value = []
    
    result = await hybrid_search(
        query="test query",
        qdrant_engine=mock_qdrant_engine,
        es_engine=mock_es_engine,
        speakers=['NonExistentSpeaker']
    )
    
    # Should return empty results
    assert len(result['results']) == 0
    assert result['stats']['semantic_percentage'] == 0
    assert result['stats']['bm25_percentage'] == 0

@pytest.mark.asyncio
async def test_hybrid_search_result_ranking(mock_qdrant_engine, mock_es_engine):
    # Test that results are properly ranked when found in both engines
    result = await hybrid_search(
        query="test query",
        qdrant_engine=mock_qdrant_engine,
        es_engine=mock_es_engine,
        speakers=['Speaker1']
    )
    
    # Verify ranking information
    for item in result['results']:
        assert 'combined_score' in item
        assert 'sources' in item
        assert isinstance(item['sources'], list)
        # Each result should come from at least one source
        assert len(item['sources']) > 0

@pytest.mark.asyncio
async def test_hybrid_search_case_sensitivity(mock_qdrant_engine, mock_es_engine):
    # Configure mock returns with differently cased speaker names
    mock_qdrant_engine.search.return_value = [
        {
            'meeting_id': 'meeting1',
            'formatted_time': '10:00',
            'content': 'Test content 1',
            'speakers': ['Speaker1', 'SPEAKER2']
        }
    ]
    
    mock_es_engine.search.return_value = [
        {
            'meeting_id': 'meeting2',
            'formatted_time': '11:00',
            'content': 'Test content 2',
            'speakers': ['speaker1', 'Speaker2']
        }
    ]
    
    # Test with exact case matching
    result = await hybrid_search(
        query="test query",
        qdrant_engine=mock_qdrant_engine,
        es_engine=mock_es_engine,
        speakers=['Speaker1']
    )
    
    # Verify search was called with exact case
    mock_qdrant_engine.search.assert_called_once_with(
        "test query",
        meeting_ids=None,
        speakers=['Speaker1'],
        limit=300,
        min_score=0.000
    )

@pytest.mark.asyncio
async def test_hybrid_search_result_stats(mock_qdrant_engine, mock_es_engine):
    # Configure mock returns with different results from each engine
    mock_qdrant_engine.search.return_value = [
        {
            'meeting_id': 'meeting1',
            'formatted_time': '10:00',
            'content': 'Semantic only content',
            'speakers': ['Speaker1']
        }
    ]
    
    mock_es_engine.search.return_value = [
        {
            'meeting_id': 'meeting2',
            'formatted_time': '11:00',
            'content': 'BM25 only content',
            'speakers': ['Speaker1']
        }
    ]
    
    result = await hybrid_search(
        query="test query",
        qdrant_engine=mock_qdrant_engine,
        es_engine=mock_es_engine,
        speakers=['Speaker1']
    )
    
    # Verify stats exist
    assert 'stats' in result
    assert 'semantic_percentage' in result['stats']
    assert 'bm25_percentage' in result['stats']
    # Each result contributes 1.0 to its source
    assert result['stats']['semantic_percentage'] == 1.0
    assert result['stats']['bm25_percentage'] == 1.0 