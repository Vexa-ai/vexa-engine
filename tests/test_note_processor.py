import pytest
from datetime import datetime
from indexing.note_processor import NoteProcessor

@pytest.fixture
def note_processor() -> NoteProcessor:
    return NoteProcessor()

@pytest.mark.asyncio
async def test_process_note_basic(
    note_processor: NoteProcessor,
    mock_voyage_client,
    sample_note_text: str
):
    """Test basic note processing functionality."""
    note_id = "test-note-1"
    timestamp = datetime.now()
    author = "test-author"

    es_docs, qdrant_points = await note_processor.process_note(
        note_text=sample_note_text,
        note_id=note_id,
        timestamp=timestamp,
        voyage_client=mock_voyage_client,
        author=author
    )

    # Verify documents were created
    assert len(es_docs) > 0
    assert len(qdrant_points) > 0
    assert len(es_docs) == len(qdrant_points)

    # Check document structure
    for doc in es_docs:
        assert doc["meeting_id"] == note_id  # Uses meeting_id for compatibility
        assert doc["timestamp"] == timestamp.isoformat()
        assert doc["author"] == author
        assert "content" in doc
        assert "contextualized_content" in doc
        assert "chunk_index" in doc
        assert doc["topic"] == "Note"
        assert doc["speaker"] == author
        assert doc["speakers"] == [author]

    # Check Qdrant points
    for point in qdrant_points:
        assert point.payload["meeting_id"] == note_id
        assert point.payload["timestamp"] == timestamp.isoformat()
        assert point.payload["author"] == author
        assert "content" in point.payload
        assert "contextualized_content" in point.payload
        assert "chunk_index" in point.payload
        assert point.payload["topic"] == "Note"
        assert point.payload["speaker"] == author
        assert point.payload["speakers"] == [author]
        assert len(point.vector) == 768  # Verify embedding dimension

@pytest.mark.asyncio
async def test_process_note_empty(
    note_processor: NoteProcessor,
    mock_voyage_client
):
    """Test processing of empty notes."""
    note_id = "test-note-empty"
    timestamp = datetime.now()
    author = "test-author"

    es_docs, qdrant_points = await note_processor.process_note(
        note_text="",
        note_id=note_id,
        timestamp=timestamp,
        voyage_client=mock_voyage_client,
        author=author
    )

    # Should handle empty notes gracefully
    assert len(es_docs) == 0
    assert len(qdrant_points) == 0

@pytest.mark.asyncio
async def test_process_note_special_chars(
    note_processor: NoteProcessor,
    mock_voyage_client
):
    """Test processing notes with special characters."""
    note_text = """Special characters test:
    !@#$%^&*()_+
    Unicode: 你好, Привет, مرحبا
    Emojis: 😀 🚀 💻
    """
    note_id = "test-note-special"
    timestamp = datetime.now()
    author = "test-author"

    es_docs, qdrant_points = await note_processor.process_note(
        note_text=note_text,
        note_id=note_id,
        timestamp=timestamp,
        voyage_client=mock_voyage_client,
        author=author
    )

    # Should handle special characters
    assert len(es_docs) > 0
    assert len(qdrant_points) > 0
    
    # Verify content preservation
    for doc in es_docs:
        assert "你好" in doc["content"]
        assert "😀" in doc["content"]
        assert "!@#$%^&*()_+" in doc["content"]
        assert doc["meeting_id"] == note_id
        assert doc["timestamp"] == timestamp.isoformat()
        assert doc["author"] == author
        assert "chunk_index" in doc
        assert doc["topic"] == "Note"
        assert doc["speaker"] == author
        assert doc["speakers"] == [author] 