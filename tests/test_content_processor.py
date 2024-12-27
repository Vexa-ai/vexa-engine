import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch
from indexing.processor import ContentProcessor, ProcessingError
from psql_models import Content, ContentType
import pandas as pd

@pytest.fixture
def mock_vexa_api():
    with patch('indexing.processor.VexaAPI') as mock:
        api = mock.return_value
        # Create a DataFrame with required columns
        df = pd.DataFrame({
            'text': ['Sample text'],
            'speaker': ['John'],
            'timestamp': [datetime.now()],
            'formatted_time': ['2024-03-19 10:00:00'],
            'content': ['Sample text content']
        })
        api.get_transcription = AsyncMock(return_value=(
            df,
            "Formatted output",
            datetime.now(),
            ["John"],
            "Full transcript"
        ))
        yield api

@pytest.fixture
def content_processor(mock_qdrant, mock_elasticsearch, mock_vexa_api):
    processor = ContentProcessor(
        qdrant_engine=mock_qdrant,
        es_engine=mock_elasticsearch
    )
    processor.vexa = mock_vexa_api
    return processor

@pytest.fixture
def sample_note_content():
    return Content(
        id="note-1",
        type=ContentType.NOTE.value,
        text="Sample note content",
        timestamp=datetime.now(),
        is_indexed=False
    )

@pytest.fixture
def sample_meeting_content():
    return Content(
        id="meeting-1",
        type=ContentType.MEETING.value,
        text="Sample meeting transcript",
        timestamp=datetime.now(),
        is_indexed=False
    )

@pytest.mark.asyncio
async def test_process_note_content(
    content_processor: ContentProcessor,
    mock_db_session,
    sample_note_content,
    mock_voyage_client
):
    """Test processing of note content."""
    # Setup
    user_id = "test-user"
    
    # Configure session to return content and text
    mock_db_session.get = AsyncMock(return_value=sample_note_content)
    mock_db_session.scalar = AsyncMock(return_value=sample_note_content.text)
    
    # Process content
    await content_processor.process_content(
        content_id=str(sample_note_content.id),
        user_id=user_id,
        token="test-token",
        session=mock_db_session
    )

    # Verify content was marked as indexed
    assert sample_note_content.is_indexed
    assert mock_db_session.commit.await_count == 1

@pytest.mark.asyncio
async def test_process_meeting_content(
    content_processor: ContentProcessor,
    mock_db_session,
    sample_meeting_content,
    mock_voyage_client
):
    """Test processing of meeting content."""
    # Setup
    user_id = "test-user"
    token = "test-token"
    
    # Configure session to return content
    mock_db_session.get = AsyncMock(return_value=sample_meeting_content)
    
    # Process content
    await content_processor.process_content(
        content_id=str(sample_meeting_content.id),
        user_id=user_id,
        token=token,
        session=mock_db_session
    )

    # Verify content was marked as indexed
    assert sample_meeting_content.is_indexed
    assert mock_db_session.commit.await_count == 1

@pytest.mark.asyncio
async def test_process_nonexistent_content(
    content_processor: ContentProcessor,
    mock_db_session
):
    """Test handling of nonexistent content."""
    # Configure session to return None
    mock_db_session.get = AsyncMock(return_value=None)
    
    # Process should raise error
    with pytest.raises(ProcessingError, match="Content not found"):
        await content_processor.process_content(
            content_id="nonexistent",
            user_id="test-user",
            token="test-token",
            session=mock_db_session
        )

@pytest.mark.asyncio
async def test_process_content_error_handling(
    content_processor: ContentProcessor,
    mock_db_session,
    sample_note_content
):
    """Test error handling during content processing."""
    # Configure session to raise an error
    mock_db_session.get = AsyncMock(return_value=sample_note_content)
    mock_db_session.scalar = AsyncMock(side_effect=Exception("Database error"))
    
    # Process should raise error
    with pytest.raises(ProcessingError, match="Content processing failed"):
        await content_processor.process_content(
            content_id=str(sample_note_content.id),
            user_id="test-user",
            token="test-token",
            session=mock_db_session
        )
    
    # Verify rollback was called
    assert mock_db_session.rollback.await_count == 1

@pytest.mark.asyncio
async def test_process_content_rollback(
    content_processor: ContentProcessor,
    mock_db_session,
    sample_note_content
):
    """Test transaction rollback on error."""
    # Configure session to raise an error during commit
    mock_db_session.get = AsyncMock(return_value=sample_note_content)
    mock_db_session.scalar = AsyncMock(return_value=sample_note_content.text)
    mock_db_session.commit = AsyncMock(side_effect=Exception("Database error"))
    
    # Process content (will raise error)
    with pytest.raises(ProcessingError):
        await content_processor.process_content(
            content_id=str(sample_note_content.id),
            user_id="test-user",
            token="test-token",
            session=mock_db_session
        )
    
    # Verify rollback was called
    assert mock_db_session.rollback.await_count == 1 