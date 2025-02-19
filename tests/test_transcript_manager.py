import pytest
import pytest_asyncio
from datetime import datetime, timezone, timedelta
from uuid import UUID, uuid4
from transcript_manager import TranscriptManager
from psql_models import Content, Transcript, TranscriptAccess, AccessLevel, ContentType
from psql_helpers import get_session, async_session
from sqlalchemy import select, delete, and_, func
from vexa import VexaAPI
import json
import os
from unittest.mock import AsyncMock, patch

@pytest_asyncio.fixture(autouse=True)
async def cleanup_transcripts():
    """Clean up any existing test transcripts"""
    async with async_session() as session:
        await session.execute(delete(TranscriptAccess))
        await session.execute(delete(Transcript))
        await session.commit()

@pytest.fixture
def legacy_segments():
    return [
        {
            'content': ' Субтитры сделал DimaTorzok Hi guys!',
            'html_content': None,
            'html_content_short': None,
            'keywords': [],
            'start_timestamp': '2025-02-13 15:32:22.950000+00:00',
            'end_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'confidence': 0.9,
            'segment_id': 123,
            'words': [['Hi', 0.0, 0.5], ['guys', 0.5, 1.0]],
            'speaker': 'Speaker 1',
            'server_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'transcription_timestamp': '2025-02-13 15:32:42.170000+00:00'
        },
        {
            'content': 'Welcome to Ken, and hopefully he will join us.',
            'html_content': None,
            'html_content_short': None,
            'keywords': [],
            'start_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'end_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'confidence': 0.85,
            'segment_id': 124,
            'words': [['Welcome', 0.0, 0.5], ['to', 0.5, 0.7], ['Ken', 0.7, 1.0]],
            'speaker': 'Speaker 2',
            'server_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'transcription_timestamp': '2025-02-13 15:33:02.170000+00:00'
        }
    ]

@pytest.fixture
def upstream_segments():
    return [
        {
            'content': ' я думал ты что-то про ну типа как хранить там и так далее но окей да... что конечно наворочили',
            'start_timestamp': '2025-02-13 15:32:22.950000+00:00',
            'end_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'confidence': 0.85,
            'segment_id': 123,
            'meeting_id': 'the-zdjv-byg',
            'words': [['я', 0.0, 0.5], ['думал', 0.5, 1.0], ['ты', 1.0, 1.5]],
            'speaker': 'Speaker 1',
            'server_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'transcription_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'present_user_ids': ['user1'],
            'partially_present_user_ids': []
        },
        {
            'content': ' а то тебе скоро векса начнет советовать психологов хоть с кем-нибудь поговорить',
            'start_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'end_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'confidence': 0.9,
            'segment_id': 124,
            'meeting_id': 'the-zdjv-byg',
            'words': [['а', 0.0, 0.5], ['то', 0.5, 1.0], ['тебе', 1.0, 1.5]],
            'speaker': 'Speaker 2',
            'server_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'transcription_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'present_user_ids': ['user1', 'user2'],
            'partially_present_user_ids': []
        }
    ]

@pytest.fixture
def invalid_segment():
    return {
        'content': None,
        'start_timestamp': '2025-02-13 15:32:22.950000+00:00',
        'end_timestamp': '2025-02-13 15:32:42.170000+00:00',
        'confidence': -1,
        'segment_id': 123,
        'meeting_id': 'test-meeting',
        'words': None,
        'speaker': None,
        'server_timestamp': '2025-02-13 15:32:42.170000+00:00',
        'transcription_timestamp': '2025-02-13 15:32:42.170000+00:00'
    }

@pytest_asyncio.fixture
async def test_content(setup_test_users):
    users, _ = setup_test_users[0]
    async with get_session() as session:
        content = Content(
            id=uuid4(),
            type=ContentType.MEETING,
            timestamp=datetime.now(timezone.utc)
        )
        session.add(content)
        await session.commit()
        return content

@pytest.fixture
def mock_vexa_api():
    """Mock VexaAPI for testing"""
    async def mock_get_transcription(*args, **kwargs):
        return [{
            "content": "Mocked Vexa API response",
            "timestamp": "2025-02-13T15:32:16.430000+00:00",
            "speaker": "Mocked Speaker",
            "confidence": 0.9,
            "segment_id": 1,
            "words": [["mocked", 0.0, 0.5], ["response", 0.5, 1.0]]
        }]
    
    with patch('vexa.VexaAPI') as mock:
        mock.return_value.get_transcription_ = AsyncMock(side_effect=mock_get_transcription)
        yield mock.return_value

@pytest.fixture
def mock_segment():
    return {
        "content": "Test content",
        "start_timestamp": "2025-02-13 15:32:16.430000+00:00",
        "end_timestamp": "2025-02-13 15:32:22.950000+00:00",
        "speaker": "Test Speaker",
        "confidence": 0.95,
        "segment_id": 1,
        "meeting_id": "test-meeting",
        "words": [["test", 0.0, 0.5], ["content", 0.5, 1.0]],
        "server_timestamp": "2025-02-13 15:32:16+00:00",
        "transcription_timestamp": "2025-02-18 15:03:21.243843+00:00",
        "present_user_ids": ["00000000-0000-4000-a000-000000000001"],
        "partially_present_user_ids": []
    }

@pytest.fixture
def mock_vexa_segment():
    return {
        'content': 'This is a test segment from Vexa API',
        'start_timestamp': '2025-02-13 15:32:22.950000+00:00',
        'end_timestamp': '2025-02-13 15:32:42.170000+00:00',
        'confidence': 0.95,
        'segment_id': 123,
        'meeting_id': 'test-meeting',
        'words': [['This', 0.0, 0.5], ['is', 0.5, 1.0], ['a', 1.0, 1.5]],
        'speaker': 'Speaker 1',
        'server_timestamp': '2025-02-13 15:32:42.170000+00:00',
        'transcription_timestamp': '2025-02-13 15:32:42.170000+00:00',
        'present_user_ids': ['user1', 'user2'],
        'partially_present_user_ids': []
    }

@pytest.fixture
def mock_segments_from_file():
    return [
        {
            'content': 'First segment from file',
            'start_timestamp': '2025-02-13 15:32:22.950000+00:00',
            'end_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'confidence': 0.9,
            'segment_id': 123,
            'meeting_id': 'test-meeting',
            'words': [['First', 0.0, 0.5], ['segment', 0.5, 1.0], ['from', 1.0, 1.5]],
            'speaker': 'Speaker 1',
            'server_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'transcription_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'present_user_ids': ['user1'],
            'partially_present_user_ids': []
        },
        {
            'content': 'Second segment from file',
            'start_timestamp': '2025-02-13 15:32:42.170000+00:00',
            'end_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'confidence': 0.85,
            'segment_id': 124,
            'meeting_id': 'test-meeting',
            'words': [['Second', 0.0, 0.5], ['segment', 0.5, 1.0], ['from', 1.0, 1.5]],
            'speaker': 'Speaker 2',
            'server_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'transcription_timestamp': '2025-02-13 15:33:02.170000+00:00',
            'present_user_ids': ['user1', 'user2'],
            'partially_present_user_ids': []
        }
    ]

@pytest.mark.asyncio
async def test_ingest_legacy_segments(setup_test_users, test_content, legacy_segments):
    user, _ = setup_test_users[0]
    manager = await TranscriptManager.create()
    
    # Test ingestion of first 2 legacy segments
    transcripts = await manager.ingest_transcript_segments(
        content_id=test_content.id,
        segments=legacy_segments[:2],
        user_id=user.id
    )
    
    assert len(transcripts) == 2
    
    # Verify data from legacy format was properly ingested
    for transcript, original_segment in zip(transcripts, legacy_segments[:2]):
        assert transcript.content_id == test_content.id
        assert transcript.text_content == original_segment['content']
        assert transcript.confidence == original_segment['confidence']
        assert transcript.original_segment_id == original_segment['segment_id']
        assert transcript.word_timing_data['words'] == original_segment['words']
        assert transcript.segment_metadata['speaker'] == original_segment['speaker']
        assert transcript.segment_metadata['source'] == 'legacy'
        assert 'original_format' in transcript.segment_metadata
        assert 'versions' in transcript.segment_metadata

@pytest.mark.asyncio
async def test_ingest_upstream_segments(setup_test_users, test_content, upstream_segments):
    user, _ = setup_test_users[0]
    manager = await TranscriptManager.create()
    
    # Test ingestion of first 2 upstream segments
    transcripts = await manager.ingest_transcript_segments(
        content_id=test_content.id,
        segments=upstream_segments[:2],
        user_id=user.id
    )
    
    assert len(transcripts) == 2
    
    # Verify data from upstream format was properly ingested
    for transcript, original_segment in zip(transcripts, upstream_segments[:2]):
        assert transcript.content_id == test_content.id
        assert transcript.text_content == original_segment['content']
        assert transcript.confidence == original_segment['confidence']
        assert transcript.original_segment_id == original_segment['segment_id']
        assert transcript.word_timing_data['words'] == original_segment['words']
        assert transcript.segment_metadata['speaker'] == original_segment['speaker']
        assert transcript.segment_metadata['source'] == 'upstream'
        assert 'server_timestamp' in transcript.segment_metadata
        assert 'transcription_timestamp' in transcript.segment_metadata
        assert 'present_user_ids' in transcript.segment_metadata
        assert 'original_format' in transcript.segment_metadata
        assert 'versions' in transcript.segment_metadata

@pytest.mark.asyncio
async def test_ingest_mixed_format_segments(setup_test_users, test_content, legacy_segments, upstream_segments):
    user, _ = setup_test_users[0]
    manager = await TranscriptManager.create()
    
    # Mix one segment from each format
    mixed_segments = [legacy_segments[0], upstream_segments[0]]
    transcripts = await manager.ingest_transcript_segments(
        content_id=test_content.id,
        segments=mixed_segments,
        user_id=user.id
    )
    
    assert len(transcripts) == 2
    
    # Verify legacy format
    legacy_transcript = transcripts[0]
    assert legacy_transcript.segment_metadata['source'] == 'legacy'
    assert 'server_timestamp' not in legacy_transcript.segment_metadata
    
    # Verify upstream format
    upstream_transcript = transcripts[1]
    assert upstream_transcript.segment_metadata['source'] == 'upstream'
    assert 'server_timestamp' in upstream_transcript.segment_metadata

@pytest.mark.asyncio
async def test_get_transcript_segments(setup_test_users, test_content, legacy_segments, upstream_segments):
    user, _ = setup_test_users[0]
    manager = await TranscriptManager.create()
    
    # Ingest mixed segments
    await manager.ingest_transcript_segments(
        content_id=test_content.id,
        segments=[legacy_segments[0], upstream_segments[0]],
        user_id=user.id
    )
    
    # Retrieve segments
    segments = await manager.get_transcript_segments(
        content_id=test_content.id,
        user_id=user.id
    )
    
    assert len(segments) == 2
    
    # Verify both formats are retrieved correctly
    legacy_segment = next(s for s in segments if s['segment_metadata']['source'] == 'legacy')
    upstream_segment = next(s for s in segments if s['segment_metadata']['source'] == 'upstream')
    
    assert 'server_timestamp' not in legacy_segment['segment_metadata']
    assert 'server_timestamp' in upstream_segment['segment_metadata']

@pytest.mark.asyncio
async def test_timestamp_handling(setup_test_users, test_content, legacy_segments):
    user, _ = setup_test_users[0]
    manager = await TranscriptManager.create()
    
    # Test legacy segment timestamp handling
    transcripts = await manager.ingest_transcript_segments(
        content_id=test_content.id,
        segments=[legacy_segments[0]],
        user_id=user.id
    )
    
    transcript = transcripts[0]
    assert transcript.start_timestamp is not None
    assert transcript.end_timestamp is not None
    assert transcript.end_timestamp > transcript.start_timestamp

@pytest.mark.asyncio
async def test_ingest_vexa_segments(setup_test_users, test_content, mock_vexa_segment):
    user, _ = setup_test_users[0]
    manager = await TranscriptManager.create()
    
    # Test ingestion of a Vexa API segment
    transcripts = await manager.ingest_transcript_segments(
        content_id=test_content.id,
        segments=[mock_vexa_segment],
        user_id=user.id
    )
    
    assert len(transcripts) == 1
    transcript = transcripts[0]
    
    # Verify transcript data
    assert transcript.content_id == test_content.id
    assert transcript.text_content == mock_vexa_segment['content']
    assert transcript.confidence == mock_vexa_segment['confidence']
    assert transcript.original_segment_id == mock_vexa_segment['segment_id']
    
    # Verify word timing data
    assert transcript.word_timing_data['words'] == mock_vexa_segment['words']
    
    # Verify metadata
    assert transcript.segment_metadata['speaker'] == mock_vexa_segment['speaker']
    assert transcript.segment_metadata['server_timestamp'] == mock_vexa_segment['server_timestamp']

@pytest.mark.asyncio
async def test_ingest_segments_from_file(setup_test_users, test_content, mock_segments_from_file):
    user, _ = setup_test_users[0]
    manager = await TranscriptManager.create()
    
    # Test ingestion of segments from file
    transcripts = await manager.ingest_transcript_segments(
        content_id=test_content.id,
        segments=mock_segments_from_file[:2],  # Test with first 2 segments
        user_id=user.id
    )
    
    assert len(transcripts) == 2
    
    # Verify data from file was properly ingested
    for transcript, original_segment in zip(transcripts, mock_segments_from_file[:2]):
        assert transcript.content_id == test_content.id
        assert transcript.text_content == original_segment['content']
        assert transcript.confidence == original_segment['confidence']
        assert transcript.original_segment_id == original_segment['segment_id']
        assert transcript.word_timing_data['words'] == original_segment['words']
        assert transcript.segment_metadata['speaker'] == original_segment['speaker']

@pytest.mark.asyncio
async def test_get_transcripts_from_vexa(setup_test_users, mock_vexa_api):
    user, _ = setup_test_users[0]
    meeting_id = "test-meeting-id"
    
    # Get transcripts from Vexa API
    transcripts = await mock_vexa_api.get_transcription_(meeting_id=meeting_id)
    assert len(transcripts) > 0
    
    # Verify structure matches expected format
    segment = transcripts[0]
    assert 'content' in segment
    assert 'timestamp' in segment
    assert 'speaker' in segment
    assert 'confidence' in segment
    assert 'segment_id' in segment
    assert 'words' in segment

@pytest.mark.asyncio
async def test_ingest_transcript_segments(setup_test_users, test_content, mock_segment):
    user, _ = setup_test_users[0]
    manager = await TranscriptManager.create()
    
    # Test ingestion of a single segment
    transcripts = await manager.ingest_transcript_segments(
        content_id=test_content.id,
        segments=[mock_segment],
        user_id=user.id
    )
    
    assert len(transcripts) == 1
    transcript = transcripts[0]
    
    # Verify transcript data
    assert transcript.content_id == test_content.id
    assert transcript.text_content == mock_segment['content']
    assert transcript.confidence == mock_segment['confidence']
    assert transcript.original_segment_id == mock_segment['segment_id']
    
    # Verify word timing data
    assert transcript.word_timing_data['words'] == mock_segment['words']
    
    # Verify metadata
    assert transcript.segment_metadata['speaker'] == mock_segment['speaker']
    assert transcript.segment_metadata['server_timestamp'] == mock_segment['server_timestamp']
    
    # Verify access
    async with get_session() as session:
        access = await session.execute(
            select(TranscriptAccess).where(
                TranscriptAccess.transcript_id == transcript.id,
                TranscriptAccess.user_id == user.id
            )
        )
        access_record = access.scalar_one()
        assert access_record.access_level == AccessLevel.OWNER

@pytest.mark.asyncio
async def test_ingest_invalid_segment(setup_test_users, test_content, invalid_segment):
    user, _ = setup_test_users[0]
    manager = await TranscriptManager.create()
    
    # Test ingestion of invalid segment
    with pytest.raises(ValueError):
        await manager.ingest_transcript_segments(
            content_id=test_content.id,
            segments=[invalid_segment],
            user_id=user.id
        )
    
    # Verify no transcripts were created
    async with get_session() as session:
        count = await session.execute(select(func.count()).select_from(Transcript))
        assert count.scalar_one() == 0

@pytest.mark.asyncio
async def test_get_transcript_segments_not_found(setup_test_users):
    user, _ = setup_test_users[0]
    manager = await TranscriptManager.create()
    
    # Try to get segments for non-existent content
    segments = await manager.get_transcript_segments(
        content_id=uuid4(),
        user_id=user.id
    )
    
    assert len(segments) == 0

@pytest.mark.asyncio
async def test_update_transcript_access(setup_test_users, test_content, mock_segment):
    owner, _ = setup_test_users[0]
    target_user, _ = setup_test_users[1]
    manager = await TranscriptManager.create()
    
    # First ingest a segment
    transcripts = await manager.ingest_transcript_segments(
        content_id=test_content.id,
        segments=[mock_segment],
        user_id=owner.id
    )
    transcript = transcripts[0]
    
    # Test granting access to another user
    success = await manager.update_transcript_access(
        transcript_id=transcript.id,
        user_id=owner.id,
        target_user_id=target_user.id,
        access_level=AccessLevel.SHARED
    )
    assert success is True
    
    # Verify target user can access the transcript
    segments = await manager.get_transcript_segments(
        content_id=test_content.id,
        user_id=target_user.id
    )
    assert len(segments) == 1
    
    # Test non-owner cannot grant access
    success = await manager.update_transcript_access(
        transcript_id=transcript.id,
        user_id=target_user.id,  # Not the owner
        target_user_id=setup_test_users[2][0].id,
        access_level=AccessLevel.SHARED
    )
    assert success is False

@pytest.mark.asyncio
async def test_get_transcript_segments_no_access(setup_test_users, test_content, mock_segment):
    owner, _ = setup_test_users[0]
    non_owner, _ = setup_test_users[1]
    manager = await TranscriptManager.create()
    
    # Ingest segment as owner
    await manager.ingest_transcript_segments(
        content_id=test_content.id,
        segments=[mock_segment],
        user_id=owner.id
    )
    
    # Try to access as non-owner
    segments = await manager.get_transcript_segments(
        content_id=test_content.id,
        user_id=non_owner.id
    )
    
    # Should return empty list since user has no access
    assert len(segments) == 0

@pytest.mark.asyncio
async def test_ingest_multiple_segments(setup_test_users, test_content):
    user, _ = setup_test_users[0]
    manager = await TranscriptManager.create()
    
    # Create multiple segments with different timestamps
    segments = []
    for i in range(3):
        segment = {
            "content": f"Test content {i}",
            "start_timestamp": f"2025-02-13 15:{32+i}:16.430000+00:00",
            "end_timestamp": f"2025-02-13 15:{32+i}:22.950000+00:00",
            "speaker": "Test Speaker",
            "confidence": 0.95,
            "segment_id": i,
            "meeting_id": "test-meeting",
            "words": [[f"word{j}", j*0.5, (j+1)*0.5] for j in range(3)],
            "server_timestamp": "2025-02-13 15:32:16+00:00",
            "transcription_timestamp": "2025-02-18 15:03:21.243843+00:00",
            "present_user_ids": ["00000000-0000-4000-a000-000000000001"],
            "partially_present_user_ids": []
        }
        segments.append(segment)
    
    # Ingest multiple segments
    transcripts = await manager.ingest_transcript_segments(
        content_id=test_content.id,
        segments=segments,
        user_id=user.id
    )
    
    assert len(transcripts) == 3
    
    # Verify segments are returned in chronological order
    retrieved_segments = await manager.get_transcript_segments(
        content_id=test_content.id,
        user_id=user.id
    )
    
    assert len(retrieved_segments) == 3
    for i in range(len(retrieved_segments)-1):
        current_start = datetime.fromisoformat(retrieved_segments[i]['start_timestamp'])
        next_start = datetime.fromisoformat(retrieved_segments[i+1]['start_timestamp'])
        assert current_start < next_start

@pytest.mark.asyncio
async def test_update_transcript_access_edge_cases(setup_test_users, test_content, mock_segment):
    owner, _ = setup_test_users[0]
    target_user, _ = setup_test_users[1]
    manager = await TranscriptManager.create()
    
    # Test with non-existent transcript
    success = await manager.update_transcript_access(
        transcript_id=uuid4(),
        user_id=owner.id,
        target_user_id=target_user.id,
        access_level=AccessLevel.SHARED
    )
    assert success is False
    
    # Test with non-existent user
    success = await manager.update_transcript_access(
        transcript_id=uuid4(),
        user_id=uuid4(),
        target_user_id=target_user.id,
        access_level=AccessLevel.SHARED
    )
    assert success is False
    
    # Test with same user as target
    transcripts = await manager.ingest_transcript_segments(
        content_id=test_content.id,
        segments=[mock_segment],
        user_id=owner.id
    )
    success = await manager.update_transcript_access(
        transcript_id=transcripts[0].id,
        user_id=owner.id,
        target_user_id=owner.id,
        access_level=AccessLevel.SHARED
    )
    assert success is False  # Should not be able to modify own access

@pytest.mark.asyncio
async def test_large_transcript_segment(setup_test_users, test_content):
    user, _ = setup_test_users[0]
    manager = await TranscriptManager.create()
    
    # Create a segment with large text and many words
    large_segment = {
        "content": "x" * 1000000,  # 1MB of text
        "start_timestamp": "2025-02-13 15:32:16.430000+00:00",
        "end_timestamp": "2025-02-13 15:32:22.950000+00:00",
        "speaker": "Test Speaker",
        "confidence": 0.95,
        "segment_id": 1,
        "meeting_id": "test-meeting",
        "words": [["word", i*0.1, (i+1)*0.1] for i in range(10000)],  # 10k words
        "server_timestamp": "2025-02-13 15:32:16+00:00",
        "transcription_timestamp": "2025-02-18 15:03:21.243843+00:00",
        "present_user_ids": ["00000000-0000-4000-a000-000000000001"],
        "partially_present_user_ids": []
    }
    
    # Should handle large data without issues
    transcripts = await manager.ingest_transcript_segments(
        content_id=test_content.id,
        segments=[large_segment],
        user_id=user.id
    )
    
    assert len(transcripts) == 1
    assert len(transcripts[0].text_content) == 1000000
    assert len(transcripts[0].word_timing_data['words']) == 10000 