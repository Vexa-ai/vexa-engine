import pytest
from datetime import datetime
from typing import List, Dict
from unittest.mock import AsyncMock, MagicMock, ANY
from hybrid_search import hybrid_search

@pytest.fixture
def sample_semantic_results() -> List[Dict]:
    return [
        {
            "meeting_id": "meeting-1",
            "formatted_time": "2024-03-19 10:00:00",
            "content": "Discussion about machine learning",
            "score": 0.9,
            "speaker": "John",
            "speakers": ["John", "Jane"]
        },
        {
            "meeting_id": "meeting-2",
            "formatted_time": "2024-03-19 11:00:00",
            "content": "Python programming basics",
            "score": 0.8,
            "speaker": "Jane",
            "speakers": ["Jane"]
        }
    ]

@pytest.fixture
def sample_bm25_results() -> List[Dict]:
    return [
        {
            "meeting_id": "meeting-2",
            "formatted_time": "2024-03-19 11:00:00",
            "content": "Python programming basics",
            "score": 1.5,
            "speaker": "Jane",
            "speakers": ["Jane"]
        },
        {
            "meeting_id": "meeting-3",
            "formatted_time": "2024-03-19 12:00:00",
            "content": "Data structures overview",
            "score": 1.2,
            "speaker": "Bob",
            "speakers": ["Bob"]
        }
    ]

@pytest.mark.asyncio
async def test_hybrid_search_basic(
    mock_qdrant,
    mock_elasticsearch,
    sample_semantic_results,
    sample_bm25_results
):
    """Test basic hybrid search functionality."""
    # Configure mocks
    mock_qdrant.search = AsyncMock(return_value=sample_semantic_results)
    mock_elasticsearch.search = MagicMock(return_value=sample_bm25_results)

    # Perform search
    result = await hybrid_search(
        query="python programming",
        qdrant_engine=mock_qdrant,
        es_engine=mock_elasticsearch,
        k=3
    )

    # Verify results structure
    assert "results" in result
    assert "stats" in result
    assert isinstance(result["results"], list)
    assert all("combined_score" in r for r in result["results"])
    assert all("sources" in r for r in result["results"])

    # Check stats
    assert "semantic_percentage" in result["stats"]
    assert "bm25_percentage" in result["stats"]
    assert 0 <= result["stats"]["semantic_percentage"] <= 100
    assert 0 <= result["stats"]["bm25_percentage"] <= 100

@pytest.mark.asyncio
async def test_hybrid_search_filtering(
    mock_qdrant,
    mock_elasticsearch,
    sample_semantic_results,
    sample_bm25_results
):
    """Test search filtering by meeting IDs and speakers."""
    meeting_ids = ["meeting-1", "meeting-2"]
    speakers = ["John", "Jane"]

    # Configure mocks
    mock_qdrant.search = AsyncMock(return_value=sample_semantic_results)
    mock_elasticsearch.search = MagicMock(return_value=sample_bm25_results)

    result = await hybrid_search(
        query="test query",
        qdrant_engine=mock_qdrant,
        es_engine=mock_elasticsearch,
        meeting_ids=meeting_ids,
        speakers=speakers,
        k=5
    )

    # Verify filter parameters were passed
    mock_qdrant.search.assert_awaited_with(
        "test query",
        meeting_ids=meeting_ids,
        speakers=speakers,
        limit=ANY,
        min_score=ANY
    )
    mock_elasticsearch.search.assert_called_with(
        "test query",
        meeting_ids=meeting_ids,
        speakers=speakers,
        k=ANY
    )

@pytest.mark.asyncio
async def test_hybrid_search_weights(
    mock_qdrant,
    mock_elasticsearch,
    sample_semantic_results,
    sample_bm25_results
):
    """Test search result weighting."""
    # Configure mocks
    mock_qdrant.search = AsyncMock(return_value=sample_semantic_results)
    mock_elasticsearch.search = MagicMock(return_value=sample_bm25_results)

    # Test with different weights
    semantic_weight = 0.8
    bm25_weight = 0.2

    result = await hybrid_search(
        query="test query",
        qdrant_engine=mock_qdrant,
        es_engine=mock_elasticsearch,
        k=5,
        semantic_weight=semantic_weight,
        bm25_weight=bm25_weight
    )

    # Verify results have scores
    assert all("combined_score" in r for r in result["results"])
    
    # Check that semantic results have higher scores when semantic_weight is higher
    semantic_results = [r for r in result["results"] if "semantic" in r["sources"]]
    bm25_results = [r for r in result["results"] if "bm25" in r["sources"]]
    
    if semantic_results and bm25_results:
        avg_semantic_score = sum(r["combined_score"] for r in semantic_results) / len(semantic_results)
        avg_bm25_score = sum(r["combined_score"] for r in bm25_results) / len(bm25_results)
        assert avg_semantic_score > avg_bm25_score

@pytest.mark.asyncio
async def test_hybrid_search_empty_results(
    mock_qdrant,
    mock_elasticsearch
):
    """Test handling of empty search results."""
    # Configure mocks to return empty results
    mock_qdrant.search = AsyncMock(return_value=[])
    mock_elasticsearch.search = MagicMock(return_value=[])

    result = await hybrid_search(
        query="nonexistent query",
        qdrant_engine=mock_qdrant,
        es_engine=mock_elasticsearch,
        k=5
    )

    # Should handle empty results gracefully
    assert len(result["results"]) == 0
    assert result["stats"]["semantic_percentage"] == 0
    assert result["stats"]["bm25_percentage"] == 0 