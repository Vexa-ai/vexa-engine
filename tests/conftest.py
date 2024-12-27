import pytest
import asyncio
import pytest_asyncio
from typing import AsyncGenerator, Generator
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime

class MockEmbeddingResponse:
    def __init__(self, embeddings):
        self.embeddings = embeddings

# Mock Voyage Client
@pytest.fixture
def mock_voyage_client() -> MagicMock:
    client = MagicMock()
    def mock_embed(texts, model):
        # Return embeddings based on input size
        return MockEmbeddingResponse(
            embeddings=[[0.1] * 768 for _ in range(len(texts))]
        )
    client.embed = mock_embed
    return client

# Mock Redis
@pytest.fixture
def mock_redis() -> MagicMock:
    redis = MagicMock()
    redis.zrevrangebyscore = MagicMock(return_value=[])
    redis.zadd = MagicMock(return_value=True)
    redis.zrem = MagicMock(return_value=True)
    return redis

# Mock Elasticsearch
@pytest.fixture
def mock_elasticsearch() -> MagicMock:
    es = MagicMock()
    es.index = MagicMock(return_value={"result": "created"})
    es.search = MagicMock(return_value=[])
    return es

# Mock Qdrant
@pytest.fixture
def mock_qdrant() -> MagicMock:
    qdrant = MagicMock()
    qdrant.upsert = AsyncMock(return_value=True)
    qdrant.search = AsyncMock(return_value=[])
    qdrant.voyage = MagicMock()
    def mock_embed(texts, model):
        # Return embeddings based on input size
        return MockEmbeddingResponse(
            embeddings=[[0.1] * 768 for _ in range(len(texts))]
        )
    qdrant.voyage.embed = mock_embed
    qdrant.client = MagicMock()
    qdrant.client.upsert = AsyncMock(return_value=True)
    qdrant.collection_name = "test_collection"
    return qdrant

# Mock Database Session
@pytest_asyncio.fixture
async def mock_db_session() -> MagicMock:
    session = MagicMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    session.get = AsyncMock()
    session.scalar = AsyncMock()
    
    async def mock_execute(*args, **kwargs):
        return []
    
    session.execute = AsyncMock(side_effect=mock_execute)
    return session

# Test Data Fixtures
@pytest.fixture
def sample_note_text() -> str:
    return """This is a sample note for testing.
    It contains multiple lines and paragraphs.
    
    This is a new paragraph with some technical terms:
    - Python programming
    - Machine learning
    - Data structures
    
    And some special characters: @#$%^&*()
    """

@pytest.fixture
def sample_meeting_text() -> str:
    return """Meeting Transcript
    Date: 2024-03-19
    
    John: Hi everyone, welcome to the meeting.
    Jane: Thanks for having us.
    John: Let's discuss the project timeline.
    Jane: I think we should focus on the MVP first.
    """

# Event Loop
@pytest.fixture(scope="session")
def event_loop() -> Generator:
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close() 