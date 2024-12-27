import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock
import logging

# Configure pytest-asyncio
pytest.asyncio_fixture_loop_scope = "function"

from .note_processor import NoteProcessor

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

@pytest.fixture
def note_processor():
    logger.info("Creating NoteProcessor instance with chunk_size=1000, chunk_overlap=200")
    return NoteProcessor(chunk_size=1000, chunk_overlap=200)

@pytest.fixture
def mock_voyage_client():
    logger.info("Setting up mock Voyage client")
    client = MagicMock()
    def mock_embed(texts, model):
        logger.debug(f"Mock embedding {len(texts)} texts with model {model}")
        # Return one embedding vector per input text
        embeddings = [[1.0] * 768 for _ in texts]
        logger.debug(f"Generated {len(embeddings)} mock embeddings")
        return MagicMock(embeddings=embeddings)
    client.embed = mock_embed
    return client

def test_create_chunks_basic(note_processor):
    """Test basic text chunking with paragraphs."""
    logger.info("Starting basic chunking test")
    text = "First paragraph.\n\nSecond paragraph.\n\nThird paragraph."
    logger.debug(f"Input text length: {len(text)} chars")
    
    chunks = note_processor.create_chunks(text)
    logger.info(f"Created {len(chunks)} chunks")
    for i, chunk in enumerate(chunks):
        logger.debug(f"Chunk {i}: {len(chunk)} chars")
    
    assert len(chunks) == 1  # Small enough to be one chunk
    assert "First paragraph" in chunks[0]
    assert "Second paragraph" in chunks[0]
    assert "Third paragraph" in chunks[0]
    logger.info("Basic chunking test completed successfully")

def test_create_chunks_long_text(note_processor):
    """Test chunking of long text."""
    logger.info("Starting long text chunking test")
    # Create text longer than chunk_size
    long_text = "This is a sentence. " * 100
    logger.debug(f"Generated long text: {len(long_text)} chars")
    
    chunks = note_processor.create_chunks(long_text)
    logger.info(f"Created {len(chunks)} chunks from long text")
    for i, chunk in enumerate(chunks):
        logger.debug(f"Chunk {i}: {len(chunk)} chars")
        if i > 0:
            logger.debug(f"Overlap with previous chunk: {len(set(chunks[i-1][-200:]).intersection(set(chunk)))}")
    
    assert len(chunks) > 1  # Should be split into multiple chunks
    # Check overlap
    assert chunks[0][-200:] in chunks[1]
    logger.info("Long text chunking test completed successfully")

def test_create_chunks_special_chars(note_processor):
    """Test chunking with special characters."""
    text = "Code block:\n```python\ndef test():\n    pass\n```\nRegular text."
    chunks = note_processor.create_chunks(text)
    assert len(chunks) == 1
    assert "```python" in chunks[0]
    assert "Regular text" in chunks[0]

def test_create_chunks_empty(note_processor):
    """Test chunking empty text."""
    chunks = note_processor.create_chunks("")
    assert len(chunks) == 0

def test_create_chunks_single_line(note_processor):
    """Test chunking single line text."""
    text = "This is a single line of text."
    chunks = note_processor.create_chunks(text)
    assert len(chunks) == 1
    assert chunks[0] == text

@pytest.mark.asyncio
async def test_process_note(note_processor, mock_voyage_client):
    """Test full note processing pipeline."""
    logger.info("Starting full note processing test")
    note_text = "Test note content.\n\nSecond paragraph."
    note_id = "test-id"
    timestamp = datetime.now()
    author = "test-author"
    
    logger.debug(f"Processing note: id={note_id}, length={len(note_text)}, author={author}")
    
    es_docs, qdrant_points = await note_processor.process_note(
        note_text=note_text,
        note_id=note_id,
        timestamp=timestamp,
        voyage_client=mock_voyage_client,
        author=author
    )
    
    logger.info(f"Generated {len(es_docs)} ES documents and {len(qdrant_points)} Qdrant points")
    
    # Log ES documents details
    for i, doc in enumerate(es_docs):
        logger.debug(f"ES Doc {i}:")
        logger.debug(f"  - Content length: {len(doc['content'])}")
        logger.debug(f"  - Chunk index: {doc['chunk_index']}")
        logger.debug(f"  - Metadata: meeting_id={doc['meeting_id']}, author={doc['author']}")
    
    # Log Qdrant points details
    for i, point in enumerate(qdrant_points):
        logger.debug(f"Qdrant Point {i}:")
        logger.debug(f"  - Vector dimensions: {len(point.vector)}")
        logger.debug(f"  - Content length: {len(point.payload['content'])}")
        logger.debug(f"  - Metadata: meeting_id={point.payload['meeting_id']}, author={point.payload['author']}")
    
    # Check Elasticsearch documents
    assert len(es_docs) == 1
    assert es_docs[0]["meeting_id"] == note_id
    assert es_docs[0]["content"] == note_text
    assert es_docs[0]["author"] == author
    
    # Check Qdrant points
    assert len(qdrant_points) == 1
    assert qdrant_points[0].payload["meeting_id"] == note_id
    assert qdrant_points[0].payload["content"] == note_text
    assert qdrant_points[0].payload["author"] == author
    logger.info("Full note processing test completed successfully")

@pytest.mark.asyncio
async def test_process_note_long(note_processor, mock_voyage_client):
    """Test processing of long notes."""
    logger.info("Starting long note processing test")
    long_text = "This is a sentence. " * 100
    note_id = "test-id"
    timestamp = datetime.now()
    author = "test-author"
    
    logger.debug(f"Processing long note: id={note_id}, length={len(long_text)}, author={author}")
    
    es_docs, qdrant_points = await note_processor.process_note(
        note_text=long_text,
        note_id=note_id,
        timestamp=timestamp,
        voyage_client=mock_voyage_client,
        author=author
    )
    
    logger.info(f"Generated {len(es_docs)} ES documents and {len(qdrant_points)} Qdrant points")
    
    # Should create multiple chunks
    assert len(es_docs) > 1, "Long text should create multiple chunks"
    assert len(qdrant_points) > 1, "Long text should create multiple Qdrant points"
    
    # Log and check chunk details
    for i, doc in enumerate(es_docs):
        logger.debug(f"ES Doc {i}:")
        logger.debug(f"  - Content length: {len(doc['content'])}")
        logger.debug(f"  - Chunk index: {doc['chunk_index']}")
        logger.debug(f"  - Metadata: note_id={doc['meeting_id']}, author={doc['author']}")
        
        # Check chunk indexing and metadata
        assert doc["chunk_index"] == i, f"Chunk index mismatch for chunk {i}"
        assert doc["meeting_id"] == note_id, f"Meeting ID mismatch for chunk {i}"
        assert doc["author"] == author, f"Author mismatch for chunk {i}"
        
        # Verify chunk size constraints
        assert len(doc["content"]) <= 1000, f"Chunk {i} exceeds maximum size"
        if i > 0:
            # Verify overlap with previous chunk
            prev_chunk = es_docs[i-1]["content"]
            overlap = set(prev_chunk[-200:]).intersection(set(doc["content"][:200]))
            logger.debug(f"  - Overlap with previous chunk: {len(overlap)} chars")
            assert len(overlap) > 0, f"No overlap found between chunks {i-1} and {i}"
    
    # Log and check Qdrant points
    for i, point in enumerate(qdrant_points):
        logger.debug(f"Qdrant Point {i}:")
        logger.debug(f"  - Vector dimensions: {len(point.vector)}")
        logger.debug(f"  - Content length: {len(point.payload['content'])}")
        logger.debug(f"  - Metadata: meeting_id={point.payload['meeting_id']}, author={point.payload['author']}")
        
        # Check point metadata
        assert point.payload["meeting_id"] == note_id, f"Meeting ID mismatch for point {i}"
        assert point.payload["author"] == author, f"Author mismatch for point {i}"
        assert point.payload["chunk_index"] == i, f"Chunk index mismatch for point {i}"
        
        # Verify vector dimensions
        assert len(point.vector) == 768, f"Incorrect vector dimensions for point {i}"
    
    logger.info("Long note processing test completed successfully")

@pytest.mark.asyncio
async def test_process_note_empty(note_processor, mock_voyage_client):
    """Test processing empty notes."""
    note_id = "test-id"
    timestamp = datetime.now()
    author = "test-author"

    es_docs, qdrant_points = await note_processor.process_note(
        note_text="",
        note_id=note_id,
        timestamp=timestamp,
        voyage_client=mock_voyage_client,
        author=author
    )

    assert len(es_docs) == 0
    assert len(qdrant_points) == 0 