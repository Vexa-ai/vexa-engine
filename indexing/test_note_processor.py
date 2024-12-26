import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

from .note_processor import NoteProcessor

@pytest.fixture
def note_processor():
    return NoteProcessor(chunk_size=1000, chunk_overlap=200)

@pytest.fixture
def mock_voyage_client():
    client = MagicMock()
    def mock_embed(texts, model):
        # Return one embedding vector per input text
        return MagicMock(embeddings=[[1.0] * 768 for _ in texts])
    client.embed = mock_embed
    return client

def test_create_chunks_basic(note_processor):
    """Test basic text chunking with paragraphs."""
    text = "First paragraph.\n\nSecond paragraph.\n\nThird paragraph."
    chunks = note_processor.create_chunks(text)
    assert len(chunks) == 1  # Small enough to be one chunk
    assert "First paragraph" in chunks[0]
    assert "Second paragraph" in chunks[0]
    assert "Third paragraph" in chunks[0]

def test_create_chunks_long_text(note_processor):
    """Test chunking of long text."""
    # Create text longer than chunk_size
    long_text = "This is a sentence. " * 100
    chunks = note_processor.create_chunks(long_text)
    assert len(chunks) > 1  # Should be split into multiple chunks
    # Check overlap
    assert chunks[0][-200:] in chunks[1]

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
    note_text = "Test note content.\n\nSecond paragraph."
    note_id = "test-id"
    timestamp = datetime.now()
    author = "test-author"

    es_docs, qdrant_points = await note_processor.process_note(
        note_text=note_text,
        note_id=note_id,
        timestamp=timestamp,
        voyage_client=mock_voyage_client,
        author=author
    )

    # Check Elasticsearch documents
    assert len(es_docs) == 1
    assert es_docs[0]["note_id"] == note_id
    assert es_docs[0]["content"] == note_text
    assert es_docs[0]["author"] == author

    # Check Qdrant points
    assert len(qdrant_points) == 1
    assert qdrant_points[0].payload["note_id"] == note_id
    assert qdrant_points[0].payload["content"] == note_text
    assert qdrant_points[0].payload["author"] == author

@pytest.mark.asyncio
async def test_process_note_long(note_processor, mock_voyage_client):
    """Test processing of long notes."""
    long_text = "This is a sentence. " * 100
    note_id = "test-id"
    timestamp = datetime.now()
    author = "test-author"

    es_docs, qdrant_points = await note_processor.process_note(
        note_text=long_text,
        note_id=note_id,
        timestamp=timestamp,
        voyage_client=mock_voyage_client,
        author=author
    )

    # Should create multiple chunks
    assert len(es_docs) > 1
    assert len(qdrant_points) > 1
    
    # Check chunk indexing
    for i, doc in enumerate(es_docs):
        assert doc["chunk_index"] == i
        assert doc["note_id"] == note_id
        assert doc["author"] == author

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