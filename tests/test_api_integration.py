import pytest
import pytest_asyncio
import logging
from datetime import datetime
from uuid import UUID
from unittest.mock import MagicMock, AsyncMock
from fastapi import FastAPI, Depends
from fastapi.testclient import TestClient
from httpx import AsyncClient
from sqlalchemy import select

from notes.api import router as notes_router, get_current_user
from psql_models import Content, UserContent, ContentType, AccessLevel, UserToken
from psql_helpers import get_session
from indexing.meetings_monitor import MeetingsMonitor
from indexing.processor import ContentProcessor
from bm25_search import ElasticsearchBM25
from qdrant_search import QdrantSearchEngine
from token_manager import TokenManager

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Test app setup
app = FastAPI()

# Mock token manager for dependency override
mock_token_manager = MagicMock()
async def mock_check_token(token):
    return UUID("12345678-1234-5678-1234-567812345678"), "test@example.com"
mock_token_manager.check_token = AsyncMock(side_effect=mock_check_token)

# Override dependencies
async def override_get_current_user():
    return UUID("12345678-1234-5678-1234-567812345678"), "test@example.com", "test-token"

async def override_get_session(test_session=Depends(get_session)):
    yield test_session

app.dependency_overrides[get_current_user] = override_get_current_user
app.dependency_overrides[get_session] = override_get_session
app.dependency_overrides[TokenManager] = lambda: mock_token_manager
app.include_router(notes_router)

@pytest_asyncio.fixture
async def test_client(test_session):
    """Create a test client with database session."""
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client

@pytest.fixture
def mock_elasticsearch():
    es = MagicMock(spec=ElasticsearchBM25)
    async def index_documents(documents):
        logger.info(f"Mock indexing {len(documents)} documents to Elasticsearch")
        return True
    es.index_documents = AsyncMock(side_effect=index_documents)
    return es

@pytest.fixture
def mock_qdrant():
    qdrant = MagicMock(spec=QdrantSearchEngine)
    async def add_points(points):
        logger.info(f"Mock indexing {len(points)} points to Qdrant")
        return True
    qdrant.add_points = AsyncMock(side_effect=add_points)
    return qdrant

@pytest.fixture
def mock_monitor():
    monitor = MagicMock(spec=MeetingsMonitor)
    def add_to_queue(content_id):
        logger.info(f"Mock adding content {content_id} to indexing queue")
        return True
    monitor._add_to_queue = MagicMock(side_effect=add_to_queue)
    return monitor

@pytest.mark.asyncio
async def test_create_note_and_index(
    test_client,
    test_session,
    mock_elasticsearch,
    mock_qdrant,
    mock_monitor
):
    """Test creating a note via API and validating its indexing."""
    logger.info("Starting create note and index test")
    
    # Test data
    note_text = "Test note content with multiple paragraphs.\n\n" + \
                "This is the second paragraph.\n\n" + \
                "And this is the third paragraph with some technical terms: " + \
                "Python, FastAPI, and Elasticsearch."
    
    # Create note request
    logger.debug("Preparing note creation request")
    response = await test_client.post(
        "/notes",
        json={"text": note_text},
        headers={"Authorization": "Bearer test-token"}
    )
    
    # Verify response
    assert response.status_code == 200, f"Note creation failed: {response.text}"
    data = response.json()
    logger.info(f"Note created with ID: {data['id']}")
    
    # Verify note in database
    # Check Content entry
    content_query = await test_session.execute(
        select(Content).where(Content.id == UUID(data['id']))
    )
    content = content_query.scalar_one()
    assert content.text == note_text
    assert content.type == ContentType.NOTE
    assert not content.is_indexed  # Should start as False
    
    # Check UserContent entry
    user_content_query = await test_session.execute(
        select(UserContent).where(UserContent.content_id == UUID(data['id']))
    )
    user_content = user_content_query.scalar_one()
    assert user_content.access_level == AccessLevel.OWNER
    assert user_content.is_owner
    
    # Check UserToken entry
    token_query = await test_session.execute(
        select(UserToken).where(UserToken.token == "test-token")
    )
    token = token_query.scalar_one()
    assert token.user_id == UUID("12345678-1234-5678-1234-567812345678")
    
    # Verify added to indexing queue
    mock_monitor._add_to_queue.assert_called_once_with(str(data['id']))
    
    # Process the note (simulating worker)
    logger.info("Processing note for indexing")
    processor = ContentProcessor(
        es_engine=mock_elasticsearch,
        qdrant_engine=mock_qdrant
    )
    await processor.process_content(content, str(user_content.user_id))
    
    # Verify Elasticsearch indexing
    mock_elasticsearch.index_documents.assert_called_once()
    es_docs = mock_elasticsearch.index_documents.call_args[0][0]
    assert len(es_docs) > 0
    for doc in es_docs:
        assert doc['meeting_id'] == str(data['id'])
        assert doc['author'] == str(user_content.user_id)
        assert len(doc['content']) <= 1000  # Verify chunk size
    
    # Verify Qdrant indexing
    mock_qdrant.add_points.assert_called_once()
    qdrant_points = mock_qdrant.add_points.call_args[0][0]
    assert len(qdrant_points) > 0
    for point in qdrant_points:
        assert point.payload['meeting_id'] == str(data['id'])
        assert point.payload['author'] == str(user_content.user_id)
        assert len(point.payload['content']) <= 1000  # Verify chunk size
        assert len(point.vector) == 768  # Verify vector dimensions
    
    # Verify chunks match between engines
    assert len(es_docs) == len(qdrant_points)
    for es_doc, qdrant_point in zip(es_docs, qdrant_points):
        assert es_doc['content'] == qdrant_point.payload['content']
        assert es_doc['chunk_index'] == qdrant_point.payload['chunk_index']
    
    logger.info("Note creation and indexing test completed successfully") 